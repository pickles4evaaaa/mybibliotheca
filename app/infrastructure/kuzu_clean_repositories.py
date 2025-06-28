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
    from .kuzu_graph import get_kuzu_database
except ImportError:
    from kuzu_graph import get_kuzu_database

logger = logging.getLogger(__name__)


class CleanKuzuUserRepository:
    """Clean user repository using simplified Kuzu schema."""
    
    def __init__(self):
        self.db = get_kuzu_database()
    
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
            print(f"🔍 [USER_REPO] Searching for user with username: '{username}'")
            query = "MATCH (u:User {username: $username}) RETURN u"
            print(f"🔍 [USER_REPO] Executing query: {query} with params: {{'username': '{username}'}}")
            results = self.db.query(query, {"username": username})
            print(f"🔍 [USER_REPO] Query results: {results}")
            
            if results:
                # The query returns a single column, so check both 'result' and 'col_0' keys
                user_data = results[0].get('result') or results[0].get('col_0')
                print(f"🔍 [USER_REPO] Extracted user_data: {user_data}")
                if user_data:
                    logger.debug(f"Found user by username: {username}")
                    user_dict = dict(user_data)
                    print(f"🔍 [USER_REPO] Converted to dict: {user_dict}")
                    return user_dict
            print(f"🔍 [USER_REPO] No user found for username: '{username}'")
            return None
            
        except Exception as e:
            print(f"🔍 [USER_REPO] Error getting user by username '{username}': {e}")
            logger.error(f"❌ Failed to get user by username {username}: {e}")
            return None
    
    async def get_by_email(self, email: str) -> Optional[Any]:
        """Get a user by email."""
        try:
            query = "MATCH (u:User {email: $email}) RETURN u"
            results = self.db.query(query, {"email": email})
            
            if results:
                # The query returns a single column, so check both 'result' and 'col_0' keys
                user_data = results[0].get('result') or results[0].get('col_0')
                if user_data:
                    logger.debug(f"Found user by email: {email}")
                    return dict(user_data)
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get user by email {email}: {e}")
            return None
    
    async def get_all(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Get all users with pagination."""
        try:
            query = f"MATCH (u:User) RETURN u SKIP {offset} LIMIT {limit}"
            results = self.db.query(query)
            
            users = []
            for result in results:
                # Handle both result formats for single column queries
                if 'result' in result:
                    users.append(dict(result['result']))
                elif 'col_0' in result:
                    users.append(dict(result['col_0']))
            
            logger.debug(f"Retrieved {len(users)} users")
            return users
            
        except Exception as e:
            logger.error(f"❌ Failed to get users: {e}")
            return []


class CleanKuzuPersonRepository:
    """Clean person repository using simplified Kuzu schema."""
    
    def __init__(self):
        self.db = get_kuzu_database()
    
    def create(self, person: Any) -> Optional[Any]:
        """Create a new person."""
        try:
            # Handle both dictionary and object inputs
            if isinstance(person, dict):
                person_name = person.get('name', '')
                person_id = person.get('id', None) or str(uuid.uuid4())
                birth_year = person.get('birth_year', None)
                death_year = person.get('death_year', None) 
                bio = person.get('bio', '')
                created_at = person.get('created_at', datetime.utcnow())
            else:
                person_name = getattr(person, 'name', '')
                person_id = getattr(person, 'id', None) or str(uuid.uuid4())
                birth_year = getattr(person, 'birth_year', None)
                death_year = getattr(person, 'death_year', None)
                bio = getattr(person, 'bio', '')
                created_at = getattr(person, 'created_at', datetime.utcnow())
            
            if not person_name:
                logger.error("❌ Person name is required")
                return None
            
            # Only include properties that exist in the Person schema
            person_data = {
                'id': person_id,
                'name': person_name,
                'normalized_name': person_name.strip().lower(),
                'birth_year': birth_year,
                'death_year': death_year,
                'bio': bio,
                'created_at': created_at
            }
            # Note: Filtering out birth_place, website, updated_at as they don't exist in DB schema
            
            success = self.db.create_node('Person', person_data)
            if success:
                logger.info(f"✅ Created new person: {person_name} (ID: {person_id})")
                return person
            else:
                logger.error(f"❌ Failed to create person: {person_name}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to create person: {e}")
            return None
    
    async def get_by_id(self, person_id: str) -> Optional[Dict[str, Any]]:
        """Get a person by ID."""
        try:
            query = """
            MATCH (p:Person {id: $person_id})
            RETURN p
            LIMIT 1
            """
            
            results = self.db.query(query, {"person_id": person_id})
            
            if results and 'col_0' in results[0]:
                return dict(results[0]['col_0'])
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get person by ID {person_id}: {e}")
            return None
    
    async def get_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a person by name."""
        try:
            normalized_name = name.strip().lower()
            query = """
            MATCH (p:Person) 
            WHERE p.normalized_name = $normalized_name OR p.name = $name
            RETURN p
            LIMIT 1
            """
            
            results = self.db.query(query, {
                "normalized_name": normalized_name,
                "name": name
            })
            
            if results and 'col_0' in results[0]:
                return dict(results[0]['col_0'])
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get person by name {name}: {e}")
            return None
    
    async def get_all(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Get all persons with pagination."""
        try:
            query = f"""
            MATCH (p:Person)
            RETURN p
            ORDER BY p.name ASC
            SKIP {offset} LIMIT {limit}
            """
            
            results = self.db.query(query)
            
            persons = []
            for result in results:
                # Handle both result formats for single column queries
                if 'result' in result:
                    persons.append(dict(result['result']))
                elif 'col_0' in result:
                    persons.append(dict(result['col_0']))
            
            logger.debug(f"Retrieved {len(persons)} persons")
            return persons
            
        except Exception as e:
            logger.error(f"❌ Failed to get all persons: {e}")
            return []
    
    async def update(self, person_id: str, updates: Dict[str, Any]) -> bool:
        """Update a person."""
        try:
            # Build SET clause for updates
            set_clauses = []
            params = {"person_id": person_id}
            
            for key, value in updates.items():
                # Only allow valid schema properties
                if key in ['name', 'normalized_name', 'birth_year', 'death_year', 'bio']:
                    set_clauses.append(f"p.{key} = ${key}")
                    params[key] = value
            
            if not set_clauses:
                logger.warning("❌ No valid properties to update")
                return False
            
            query = f"""
            MATCH (p:Person {{id: $person_id}})
            SET {', '.join(set_clauses)}
            RETURN p
            """
            
            results = self.db.query(query, params)
            success = bool(results)
            
            if success:
                logger.info(f"✅ Updated person {person_id}")
            else:
                logger.error(f"❌ Failed to update person {person_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Failed to update person: {e}")
            return False
    
    async def delete(self, person_id: str) -> bool:
        """Delete a person."""
        try:
            query = """
            MATCH (p:Person {id: $person_id})
            DETACH DELETE p
            """
            
            results = self.db.query(query, {"person_id": person_id})
            success = True  # KuzuDB doesn't return explicit success for DELETE
            
            if success:
                logger.info(f"✅ Deleted person {person_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Failed to delete person: {e}")
            return False


class CleanKuzuBookRepository:
    """Clean book repository using simplified Kuzu schema."""
    
    def __init__(self):
        self.db = get_kuzu_database()
    
    async def create(self, book: Any) -> Optional[Any]:
        """Create a new book with relationships."""
        try:
            print(f"📚 [REPO] Starting book creation: {getattr(book, 'title', 'unknown')}")
            print(f"📚 [REPO] Book ID: {getattr(book, 'id', 'none')}")
            
            # Debug contributors
            contributors = getattr(book, 'contributors', [])
            print(f"📚 [REPO] Contributors: {len(contributors)}")
            for i, contrib in enumerate(contributors):
                person = getattr(contrib, 'person', None)
                person_name = getattr(person, 'name', 'unknown') if person else 'no person'
                contrib_type = getattr(contrib, 'contribution_type', 'unknown')
                print(f"📚 [REPO]   {i}: {person_name} ({contrib_type})")
            
            # Debug categories
            categories = getattr(book, 'categories', [])
            raw_categories = getattr(book, 'raw_categories', None)
            print(f"📚 [REPO] Categories: {len(categories)} items: {categories}")
            print(f"📚 [REPO] Raw categories: {raw_categories}")
            
            # Debug publisher
            publisher = getattr(book, 'publisher', None)
            if publisher:
                publisher_name = getattr(publisher, 'name', 'unknown')
                print(f"📚 [REPO] Publisher: {publisher_name} (type: {type(publisher)})")
            else:
                print(f"📚 [REPO] Publisher: none")
            
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
            
            # Create the book node first
            success = self.db.create_node('Book', book_data)
            if not success:
                logger.error(f"❌ Failed to create book node: {getattr(book, 'title', 'unknown')}")
                return None
            
            logger.info(f"✅ Created book node: {getattr(book, 'title', 'unknown')}")
            book_id = book_data['id']
            
            # Create author relationships
            contributors = getattr(book, 'contributors', [])
            if contributors:
                logger.info(f"🔗 Creating {len(contributors)} contributor relationships")
                for i, contribution in enumerate(contributors):
                    await self._create_contributor_relationship(book_id, contribution, i)
            
            # Create category relationships
            categories = getattr(book, 'categories', [])
            raw_categories = getattr(book, 'raw_categories', None)
            
            # Process raw_categories if available (from API data)
            if raw_categories:
                await self._create_category_relationships_from_raw(book_id, raw_categories)
            # Otherwise use existing categories
            elif categories:
                logger.info(f"🔗 Creating {len(categories)} category relationships")
                for category in categories:
                    await self._create_category_relationship(book_id, category)
            
            # Create publisher relationship if present
            publisher = getattr(book, 'publisher', None)
            if publisher:
                await self._create_publisher_relationship(book_id, publisher)
            
            logger.info(f"✅ Created book with all relationships: {getattr(book, 'title', 'unknown')}")
            return book
            
        except Exception as e:
            logger.error(f"❌ Failed to create book: {e}")
            return None
    
    async def _create_contributor_relationship(self, book_id: str, contribution: Any, order_index: int = 0):
        """Create a contributor relationship (AUTHORED, EDITED, etc.)."""
        try:
            person = getattr(contribution, 'person', None)
            if not person:
                logger.warning(f"⚠️ Contribution has no person: {contribution}")
                return
            
            # Create or find the person
            person_id = await self._ensure_person_exists(person)
            if not person_id:
                logger.warning(f"⚠️ Could not create/find person: {getattr(person, 'name', 'unknown')}")
                return
            
            # Determine relationship type and properties
            contribution_type = getattr(contribution, 'contribution_type', None)
            if hasattr(contribution_type, 'value'):
                contribution_str = contribution_type.value.upper()
            else:
                contribution_str = str(contribution_type).upper() if contribution_type else 'AUTHORED'
            
            # Map contribution types to relationship types
            rel_type_map = {
                'AUTHORED': 'AUTHORED',
                'EDITED': 'AUTHORED',  # Use AUTHORED relationship with role property
                'NARRATED': 'AUTHORED',  # Use AUTHORED relationship with role property
                'CONTRIBUTED': 'AUTHORED'  # Use AUTHORED relationship with role property
            }
            
            rel_type = rel_type_map.get(contribution_str, 'AUTHORED')
            
            # Create the relationship with properties
            role = contribution_str.lower()
            rel_props = {
                'role': role,
                'order_index': getattr(contribution, 'order', order_index)
            }
            
            success = self.db.create_relationship(
                'Person', person_id, rel_type, 'Book', book_id, rel_props
            )
            
            if success:
                logger.info(f"✅ Created {rel_type} relationship: {getattr(person, 'name', 'unknown')} -> {book_id}")
            else:
                logger.error(f"❌ Failed to create {rel_type} relationship: {getattr(person, 'name', 'unknown')} -> {book_id}")
                
        except Exception as e:
            logger.error(f"❌ Failed to create contributor relationship: {e}")
    
    async def _ensure_person_exists(self, person: Any) -> Optional[str]:
        """Ensure a person exists in the database, create if necessary."""
        try:
            person_name = getattr(person, 'name', '')
            if not person_name:
                return None
            
            # Try to find existing person by name
            normalized_name = person_name.strip().lower()
            query = """
            MATCH (p:Person) 
            WHERE p.normalized_name = $normalized_name OR p.name = $name
            RETURN p.id as id
            LIMIT 1
            """
            
            results = self.db.query(query, {
                "normalized_name": normalized_name,
                "name": person_name
            })
            
            if results and (results[0].get('result') or results[0].get('col_0')):
                person_id = results[0].get('result') or results[0]['col_0']
                logger.debug(f"Found existing person: {person_name} (ID: {person_id})")
                return person_id
            
            # Create new person
            person_id = getattr(person, 'id', None) or str(uuid.uuid4())
            # Only include properties that exist in the Person schema
            person_data = {
                'id': person_id,
                'name': person_name,
                'normalized_name': normalized_name,
                'birth_year': getattr(person, 'birth_year', None),
                'death_year': getattr(person, 'death_year', None),
                'bio': getattr(person, 'bio', ''),
                'created_at': getattr(person, 'created_at', datetime.utcnow())
            }
            # Note: Filtering out birth_place, website, updated_at as they don't exist in DB schema
            
            success = self.db.create_node('Person', person_data)
            if success:
                logger.info(f"✅ Created new person: {person_name} (ID: {person_id})")
                return person_id
            else:
                logger.error(f"❌ Failed to create person: {person_name}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to ensure person exists: {e}")
            return None
    
    async def _create_category_relationships_from_raw(self, book_id: str, raw_categories: Any):
        """Create category relationships from raw category data (strings or list)."""
        try:
            # Handle different raw_categories formats
            if isinstance(raw_categories, str):
                # Split comma-separated string
                category_names = [cat.strip() for cat in raw_categories.split(',') if cat.strip()]
            elif isinstance(raw_categories, list):
                category_names = [str(cat).strip() for cat in raw_categories if str(cat).strip()]
            else:
                logger.warning(f"⚠️ Unknown raw_categories format: {type(raw_categories)}")
                return
            
            logger.info(f"🔗 Processing {len(category_names)} categories from raw data: {category_names}")
            
            for category_name in category_names:
                await self._create_category_relationship_by_name(book_id, category_name)
                
        except Exception as e:
            logger.error(f"❌ Failed to create category relationships from raw data: {e}")
    
    async def _create_category_relationship_by_name(self, book_id: str, category_name: str):
        """Create a category relationship by category name (create category if needed)."""
        try:
            category_id = await self._ensure_category_exists(category_name)
            if not category_id:
                logger.warning(f"⚠️ Could not create/find category: {category_name}")
                return
            
            # Create the CATEGORIZED_AS relationship
            success = self.db.create_relationship(
                'Book', book_id, 'CATEGORIZED_AS', 'Category', category_id, {}
            )
            
            if success:
                logger.info(f"✅ Created CATEGORIZED_AS relationship: {book_id} -> {category_name}")
            else:
                logger.error(f"❌ Failed to create CATEGORIZED_AS relationship: {book_id} -> {category_name}")
                
        except Exception as e:
            logger.error(f"❌ Failed to create category relationship: {e}")
    
    async def _create_category_relationship(self, book_id: str, category: Any):
        """Create a category relationship with an existing category object."""
        try:
            category_id = getattr(category, 'id', None)
            category_name = getattr(category, 'name', '')
            
            if not category_id and category_name:
                # Try to find/create by name
                category_id = await self._ensure_category_exists(category_name)
            
            if not category_id:
                logger.warning(f"⚠️ Could not determine category ID for: {category}")
                return
            
            # Create the relationship
            success = self.db.create_relationship(
                'Book', book_id, 'CATEGORIZED_AS', 'Category', category_id, {}
            )
            
            if success:
                logger.info(f"✅ Created CATEGORIZED_AS relationship: {book_id} -> {category_name}")
            else:
                logger.error(f"❌ Failed to create CATEGORIZED_AS relationship: {book_id} -> {category_name}")
                
        except Exception as e:
            logger.error(f"❌ Failed to create category relationship: {e}")
    
    async def _ensure_category_exists(self, category_name: str) -> Optional[str]:
        """Ensure a category exists in the database, create if necessary."""
        try:
            if not category_name:
                return None
            
            # Normalize the category name
            normalized_name = category_name.strip().lower()
            
            # Try to find existing category
            query = """
            MATCH (c:Category) 
            WHERE c.normalized_name = $normalized_name OR c.name = $name
            RETURN c.id as id
            LIMIT 1
            """
            
            results = self.db.query(query, {
                "normalized_name": normalized_name,
                "name": category_name
            })
            
            if results and (results[0].get('result') or results[0].get('col_0')):
                category_id = results[0].get('result') or results[0]['col_0']
                logger.debug(f"Found existing category: {category_name} (ID: {category_id})")
                return category_id
            
            # Create new category
            category_id = str(uuid.uuid4())
            # Include all properties that exist in the Category schema
            category_data = {
                'id': category_id,
                'name': category_name,
                'normalized_name': normalized_name,
                'description': '',
                'level': 0,  # Default to root level
                'color': '',
                'icon': '',
                'book_count': 0,
                'user_book_count': 0,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }
            # Note: parent_id and aliases are not included as they need special handling
            
            success = self.db.create_node('Category', category_data)
            if success:
                logger.info(f"✅ Created new category: {category_name} (ID: {category_id})")
                return category_id
            else:
                logger.error(f"❌ Failed to create category: {category_name}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to ensure category exists: {e}")
            return None
    
    async def _create_publisher_relationship(self, book_id: str, publisher: Any):
        """Create a publisher relationship."""
        try:
            publisher_id = await self._ensure_publisher_exists(publisher)
            if not publisher_id:
                logger.warning(f"⚠️ Could not create/find publisher: {publisher}")
                return
            
            # Create the PUBLISHED_BY relationship
            success = self.db.create_relationship(
                'Book', book_id, 'PUBLISHED_BY', 'Publisher', publisher_id, {}
            )
            
            if success:
                logger.info(f"✅ Created PUBLISHED_BY relationship: {book_id} -> {getattr(publisher, 'name', publisher)}")
            else:
                logger.error(f"❌ Failed to create PUBLISHED_BY relationship: {book_id} -> {getattr(publisher, 'name', publisher)}")
                
        except Exception as e:
            logger.error(f"❌ Failed to create publisher relationship: {e}")
    
    async def _ensure_publisher_exists(self, publisher: Any) -> Optional[str]:
        """Ensure a publisher exists in the database, create if necessary."""
        try:
            # Handle both string and object publishers
            if isinstance(publisher, str):
                publisher_name = publisher
                publisher_country = None
                publisher_founded = None
            else:
                publisher_name = getattr(publisher, 'name', '')
                publisher_country = getattr(publisher, 'country', None)
                publisher_founded = getattr(publisher, 'founded_year', None)
            
            if not publisher_name:
                return None
            
            # Try to find existing publisher
            query = """
            MATCH (p:Publisher {name: $name})
            RETURN p.id as id
            LIMIT 1
            """
            
            results = self.db.query(query, {"name": publisher_name})
            
            if results and (results[0].get('result') or results[0].get('col_0')):
                publisher_id = results[0].get('result') or results[0]['col_0']
                logger.debug(f"Found existing publisher: {publisher_name} (ID: {publisher_id})")
                return publisher_id
            
            # Create new publisher
            publisher_id = str(uuid.uuid4())
            # Only include properties that exist in the Publisher schema
            publisher_data = {
                'id': publisher_id,
                'name': publisher_name,
                'country': publisher_country or '',
                'founded_year': publisher_founded,
                'created_at': datetime.utcnow()
            }
            # Note: Filtering out updated_at as it doesn't exist in DB schema
            
            success = self.db.create_node('Publisher', publisher_data)
            if success:
                logger.info(f"✅ Created new publisher: {publisher_name} (ID: {publisher_id})")
                return publisher_id
            else:
                logger.error(f"❌ Failed to create publisher: {publisher_name}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to ensure publisher exists: {e}")
            return None
    
    # ========================================
    # Missing Methods Required by kuzu_integration
    # ========================================
    
    async def get_by_id(self, book_id: str) -> Optional[Dict[str, Any]]:
        """Get a book by ID."""
        try:
            query = """
            MATCH (b:Book {id: $book_id})
            RETURN b
            LIMIT 1
            """
            
            results = self.db.query(query, {"book_id": book_id})
            logger.info(f"🔍 Query results for book {book_id}: {results}")
            
            if results and len(results) > 0:
                result = results[0]
                logger.info(f"🔍 First result structure: {result}")
                
                # Try different ways to access the book data - try 'result' first (single column)
                book_data = None
                if 'result' in result:
                    book_data = result['result']
                    logger.info(f"✅ Found book via 'result' key: {type(book_data)}")
                elif 'col_0' in result:
                    book_data = result['col_0']
                    logger.info(f"✅ Found book via 'col_0' key: {type(book_data)}")
                else:
                    # Fallback: return whatever we got
                    logger.warning(f"⚠️ Unexpected result structure for book {book_id}: {result}")
                    return result
                
                # Convert book data to dict
                if book_data is not None:
                    if hasattr(book_data, '__dict__'):
                        book_dict = dict(book_data)
                        logger.info(f"✅ Converted book object to dict with keys: {list(book_dict.keys())}")
                        return book_dict
                    elif isinstance(book_data, dict):
                        logger.info(f"✅ Book data is already dict with keys: {list(book_data.keys())}")
                        return book_data
                    else:
                        logger.warning(f"⚠️ Book data is unexpected type: {type(book_data)}")
                        return {'id': book_id, 'data': str(book_data)}
            
            logger.warning(f"⚠️ No results found for book {book_id}")
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get book by ID {book_id}: {e}")
            return None
    
    async def get_by_uid(self, book_uid: str) -> Optional[Dict[str, Any]]:
        """Get a book by UID (alias for get_by_id for backward compatibility)."""
        logger.info(f"🔍 get_by_uid called with: {book_uid}")
        result = await self.get_by_id(book_uid)
        logger.info(f"🔍 get_by_uid result: {result}")
        return result
    
    async def get_by_isbn(self, isbn: str) -> Optional[Dict[str, Any]]:
        """Get a book by ISBN (13 or 10)."""
        try:
            query = """
            MATCH (b:Book)
            WHERE b.isbn13 = $isbn OR b.isbn10 = $isbn
            RETURN b
            LIMIT 1
            """
            
            results = self.db.query(query, {"isbn": isbn})
            
            if results and len(results) > 0:
                result = results[0]
                
                # Try different ways to access the book data - try 'result' first (single column)
                book_data = None
                if 'result' in result:
                    book_data = result['result']
                elif 'col_0' in result:
                    book_data = result['col_0']
                else:
                    return result
                
                # Convert book data to dict
                if book_data is not None:
                    if hasattr(book_data, '__dict__'):
                        return dict(book_data)
                    elif isinstance(book_data, dict):
                        return book_data
                    else:
                        return {'isbn': isbn, 'data': str(book_data)}
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get book by ISBN {isbn}: {e}")
            return None
    
    async def search(self, query_text: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Search for books by title or author."""
        try:
            # Search by title
            query = """
            MATCH (b:Book)
            WHERE b.title CONTAINS $query_text OR b.normalized_title CONTAINS $query_text
            RETURN b
            LIMIT $limit
            """
            
            results = self.db.query(query, {
                "query_text": query_text.lower(),
                "limit": limit
            })
            
            books = []
            for result in results:
                # Handle both result formats for single column queries
                if 'result' in result:
                    books.append(dict(result['result']))
                elif 'col_0' in result:
                    books.append(dict(result['col_0']))
            
            logger.debug(f"Found {len(books)} books matching query: {query_text}")
            return books
            
        except Exception as e:
            logger.error(f"❌ Failed to search books: {e}")
            return []
    
    async def get_all(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Get all books with pagination."""
        try:
            query = f"""
            MATCH (b:Book)
            RETURN b
            SKIP {offset} LIMIT {limit}
            """
            
            results = self.db.query(query)
            
            books = []
            for result in results:
                # Handle both result formats for single column queries
                if 'result' in result:
                    books.append(dict(result['result']))
                elif 'col_0' in result:
                    books.append(dict(result['col_0']))
            
            logger.debug(f"Retrieved {len(books)} books")
            return books
            
        except Exception as e:
            logger.error(f"❌ Failed to get all books: {e}")
            return []
    
    async def get_book_authors(self, book_id: str) -> List[Dict[str, Any]]:
        """Get all authors/contributors for a book."""
        try:
            query = """
            MATCH (p:Person)-[rel:AUTHORED]->(b:Book {id: $book_id})
            RETURN p.name as name, p.id as id, rel.role as role, rel.order_index as order_index
            ORDER BY rel.order_index ASC
            """
            
            results = self.db.query(query, {"book_id": book_id})
            
            authors = []
            for result in results:
                if result.get('col_0'):  # name
                    authors.append({
                        'name': result.get('col_0', ''),
                        'id': result.get('col_1', ''),
                        'role': result.get('col_2', 'authored'),
                        'order_index': result.get('col_3', 0)
                    })
            
            logger.debug(f"Found {len(authors)} authors for book {book_id}")
            return authors
            
        except Exception as e:
            logger.error(f"❌ Failed to get book authors: {e}")
            return []
    
    async def get_book_categories(self, book_id: str) -> List[Dict[str, Any]]:
        """Get all categories for a book."""
        try:
            query = """
            MATCH (b:Book {id: $book_id})-[:CATEGORIZED_AS]->(c:Category)
            RETURN c.name as name, c.id as id, c.description as description
            ORDER BY c.name ASC
            """
            
            results = self.db.query(query, {"book_id": book_id})
            
            categories = []
            for result in results:
                if result.get('col_0'):  # name
                    categories.append({
                        'name': result.get('col_0', ''),
                        'id': result.get('col_1', ''),
                        'description': result.get('col_2', '')
                    })
            
            logger.debug(f"Found {len(categories)} categories for book {book_id}")
            return categories
            
        except Exception as e:
            logger.error(f"❌ Failed to get book categories: {e}")
            return []
    
    async def get_book_publisher(self, book_id: str) -> Optional[Dict[str, Any]]:
        """Get the publisher for a book."""
        try:
            query = """
            MATCH (b:Book {id: $book_id})-[:PUBLISHED_BY]->(p:Publisher)
            RETURN p.name as name, p.id as id, p.country as country, p.founded_year as founded_year
            LIMIT 1
            """
            
            results = self.db.query(query, {"book_id": book_id})
            
            if results and results[0].get('col_0'):
                return {
                    'name': results[0].get('col_0', ''),
                    'id': results[0].get('col_1', ''),
                    'country': results[0].get('col_2', ''),
                    'founded_year': results[0].get('col_3', None)
                }
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get book publisher: {e}")
            return None
    
    async def get_all_persons(self) -> List[Dict[str, Any]]:
        """Get all persons in the database."""
        try:
            query = """
            MATCH (p:Person)
            RETURN p
            ORDER BY p.name ASC
            """
            
            results = self.db.query(query)
            
            persons = []
            for result in results:
                # Handle both result formats for single column queries
                if 'result' in result:
                    persons.append(dict(result['result']))
                elif 'col_0' in result:
                    persons.append(dict(result['col_0']))
            
            logger.debug(f"Retrieved {len(persons)} persons")
            return persons
            
        except Exception as e:
            logger.error(f"❌ Failed to get all persons: {e}")
            return []
    
    async def get_all_categories(self) -> List[Dict[str, Any]]:
        """Get all categories in the database."""
        try:
            query = """
            MATCH (c:Category)
            RETURN c
            ORDER BY c.name ASC
            """
            
            results = self.db.query(query)
            
            categories = []
            for result in results:
                # Handle both result formats for single column queries
                if 'result' in result:
                    categories.append(dict(result['result']))
                elif 'col_0' in result:
                    categories.append(dict(result['col_0']))
            
            logger.debug(f"Retrieved {len(categories)} categories")
            return categories
            
        except Exception as e:
            logger.error(f"❌ Failed to get all categories: {e}")
            return []


class CleanKuzuUserBookRepository:
    """Repository for user-book relationships."""
    
    def __init__(self):
        self.db = get_kuzu_database()
    
    async def add_book_to_library(self, user_id: str, book_id: str, 
                                 reading_status: str = "plan_to_read",
                                 ownership_status: str = "owned",
                                 media_type: str = "physical",
                                 notes: str = "",
                                 location_id: str = None) -> bool:
        """Add a book to user's library."""
        try:
            # Ensure all properties are strings except date_added which should be TIMESTAMP
            owns_props = {
                'reading_status': str(reading_status),
                'ownership_status': str(ownership_status),
                'media_type': str(media_type),
                'date_added': datetime.utcnow(),  # Always use current time as datetime
                'source': str('manual'),
                'personal_notes': str(notes or ''),
                'location_id': str(location_id or '')  # Changed from primary_location_id to location_id
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
    
    async def get_user_books(self, user_id: str, reading_status: str = None, 
                           limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Get all books in user's library, optionally filtered by reading status."""
        try:
            if reading_status:
                query = """
                MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
                WHERE owns.reading_status = $reading_status
                RETURN b, owns
                SKIP $offset LIMIT $limit
                """
                params = {"user_id": user_id, "reading_status": reading_status, "offset": offset, "limit": limit}
            else:
                query = """
                MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
                RETURN b, owns
                SKIP $offset LIMIT $limit
                """
                params = {"user_id": user_id, "offset": offset, "limit": limit}
            
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

    async def create_ownership(self, user_id: str, book_id: str, 
                             reading_status, ownership_status, media_type,
                             location_id: str = None, source: str = "manual",
                             notes: str = "", date_added = None, 
                             custom_metadata: Dict[str, Any] = None) -> bool:
        """Create an ownership relationship between user and book."""
        try:
            # Import here to avoid circular imports
            from app.domain.models import ReadingStatus, OwnershipStatus, MediaType
            
            # Convert enum values to strings
            reading_status_str = reading_status.value if hasattr(reading_status, 'value') else str(reading_status)
            ownership_status_str = ownership_status.value if hasattr(ownership_status, 'value') else str(ownership_status)
            media_type_str = media_type.value if hasattr(media_type, 'value') else str(media_type)
            
            # Ensure date_added is a proper datetime object
            if date_added is None:
                final_date_added = datetime.utcnow()
            elif isinstance(date_added, datetime):
                final_date_added = date_added
            elif isinstance(date_added, str):
                try:
                    # Try to parse ISO format string
                    final_date_added = datetime.fromisoformat(date_added.replace('Z', '+00:00'))
                except ValueError:
                    final_date_added = datetime.utcnow()
            elif isinstance(date_added, (int, float)):
                try:
                    # Try to parse as timestamp
                    final_date_added = datetime.fromtimestamp(date_added)
                except (ValueError, OSError):
                    final_date_added = datetime.utcnow()
            else:
                # Fallback to current time for any other type
                final_date_added = datetime.utcnow()
            
            # Ensure all properties are the correct types
            owns_props = {
                'reading_status': str(reading_status_str),
                'ownership_status': str(ownership_status_str),
                'media_type': str(media_type_str),
                'date_added': final_date_added,  # Now guaranteed to be datetime
                'source': str(source),
                'personal_notes': str(notes or ''),
                'location_id': str(location_id or '')  # Changed from primary_location_id to location_id
            }
            
            # Handle custom metadata if provided
            if custom_metadata:
                import json
                owns_props['custom_metadata'] = json.dumps(custom_metadata)
                print(f"🔍 [CREATE_OWNERSHIP] Adding custom metadata to OWNS relationship: {custom_metadata}")
            
            success = self.db.create_relationship(
                'User', user_id, 'OWNS', 'Book', book_id, owns_props
            )
            
            if success and custom_metadata:
                # Also store individual custom field relationships for tracking
                print(f"🔍 [CREATE_OWNERSHIP] Storing individual custom field relationships")
                from app.infrastructure.kuzu_graph import get_graph_storage
                storage = get_graph_storage()
                
                for field_name, field_value in custom_metadata.items():
                    try:
                        storage.store_custom_metadata(user_id, book_id, field_name, str(field_value))
                        print(f"✅ [CREATE_OWNERSHIP] Stored custom field: {field_name} = {field_value}")
                    except Exception as e:
                        print(f"⚠️ [CREATE_OWNERSHIP] Failed to store custom field {field_name}: {e}")
            
            if success:
                logger.info(f"✅ Created ownership relationship: user {user_id} owns book {book_id}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"❌ Failed to create ownership: {e}")
            return False
    
    async def remove_ownership(self, user_id: str, book_id: str) -> bool:
        """Remove an ownership relationship between user and book."""
        try:
            # Execute Cypher query to delete the OWNS relationship
            query = """
            MATCH (u:User {id: $user_id})-[r:OWNS]->(b:Book {id: $book_id})
            DELETE r
            RETURN COUNT(r) as deleted_count
            """
            
            result = self.db.query(query, {"user_id": user_id, "book_id": book_id})
            
            if result and len(result) > 0:
                deleted_count = result[0].get('col_0', 0)  # Access the first column
                if deleted_count > 0:
                    logger.info(f"✅ Removed ownership relationship: user {user_id} no longer owns book {book_id}")
                    return True
                else:
                    logger.warning(f"⚠️ No ownership relationship found between user {user_id} and book {book_id}")
                    return False
            else:
                logger.warning(f"⚠️ Query returned no results for removing ownership: user {user_id}, book {book_id}")
                return False
            
        except Exception as e:
            logger.error(f"❌ Failed to remove ownership: {e}")
            return False


class CleanKuzuLocationRepository:
    """Clean location repository."""
    
    def __init__(self):
        self.db = get_kuzu_database()
    
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
                # Handle both result formats for single column queries
                if 'result' in result:
                    locations.append(dict(result['result']))
                elif 'col_0' in result:
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
    
    async def get_by_id(self, location_id: str) -> Optional[Dict[str, Any]]:
        """Get a location by ID."""
        try:
            query = """
            MATCH (l:Location {id: $location_id})
            RETURN l
            LIMIT 1
            """
            
            results = self.db.query(query, {"location_id": location_id})
            
            if results and 'col_0' in results[0]:
                return dict(results[0]['col_0'])
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get location by ID {location_id}: {e}")
            return None


class CleanKuzuCategoryRepository:
    """Clean category repository using simplified Kuzu schema."""
    
    def __init__(self):
        self.db = get_kuzu_database()
    
    async def create(self, category: Any) -> Optional[Any]:
        """Create a new category."""
        try:
            category_name = getattr(category, 'name', '')
            if not category_name:
                logger.error("❌ Category name is required")
                return None
            
            # Generate ID if not provided
            category_id = getattr(category, 'id', None) or str(uuid.uuid4())
            
            # Include all properties that exist in the Category schema
            category_data = {
                'id': category_id,
                'name': category_name,
                'normalized_name': category_name.strip().lower(),
                'description': getattr(category, 'description', ''),
                'level': getattr(category, 'level', 0),  # Default to root level
                'color': getattr(category, 'color', ''),
                'icon': getattr(category, 'icon', ''),
                'book_count': getattr(category, 'book_count', 0),
                'user_book_count': getattr(category, 'user_book_count', 0),
                'created_at': getattr(category, 'created_at', datetime.utcnow()),
                'updated_at': getattr(category, 'updated_at', datetime.utcnow())
            }
            # Note: parent_id and aliases need special handling
            
            success = self.db.create_node('Category', category_data)
            if success:
                logger.info(f"✅ Created new category: {category_name} (ID: {category_id})")
                return category
            else:
                logger.error(f"❌ Failed to create category: {category_name}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to create category: {e}")
            return None
    
    async def get_by_id(self, category_id: str) -> Optional[Dict[str, Any]]:
        """Get a category by ID."""
        try:
            query = """
            MATCH (c:Category {id: $category_id})
            RETURN c
            LIMIT 1
            """
            
            results = self.db.query(query, {"category_id": category_id})
            
            if results and 'col_0' in results[0]:
                return dict(results[0]['col_0'])
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get category by ID {category_id}: {e}")
            return None
    
    async def get_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a category by name."""
        try:
            normalized_name = name.strip().lower()
            query = """
            MATCH (c:Category) 
            WHERE c.normalized_name = $normalized_name OR c.name = $name
            RETURN c
            LIMIT 1
            """
            
            results = self.db.query(query, {
                "normalized_name": normalized_name,
                "name": name
            })
            
            if results and 'col_0' in results[0]:
                return dict(results[0]['col_0'])
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get category by name {name}: {e}")
            return None
    
    async def get_all(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Get all categories with pagination."""
        try:
            query = f"""
            MATCH (c:Category)
            RETURN c
            ORDER BY c.name ASC
            SKIP {offset} LIMIT {limit}
            """
            
            results = self.db.query(query)
            
            categories = []
            for result in results:
                # Handle both result formats for single column queries
                if 'result' in result:
                    categories.append(dict(result['result']))
                elif 'col_0' in result:
                    categories.append(dict(result['col_0']))
            
            logger.debug(f"Retrieved {len(categories)} categories")
            return categories
            
        except Exception as e:
            logger.error(f"❌ Failed to get all categories: {e}")
            return []
