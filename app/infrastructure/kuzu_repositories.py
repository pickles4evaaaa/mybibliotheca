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


class KuzuUserRepository:
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
                'created_at': getattr(user, 'created_at', datetime.utcnow()).isoformat() if hasattr(getattr(user, 'created_at', datetime.utcnow()), 'isoformat') else datetime.utcnow().isoformat()
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
                # The query returns a single column, so check both 'result' and 'col_0' keys
                user_data = results[0].get('result') or results[0].get('col_0')
                if user_data:
                    logger.debug(f"Found user by username: {username}")
                    user_dict = dict(user_data)
                    return user_dict
            return None
            
        except Exception as e:
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
            
            return users
            
        except Exception as e:
            logger.error(f"❌ Failed to get users: {e}")
            return []


class KuzuPersonRepository:
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
                openlibrary_id = person.get('openlibrary_id', None)
                birth_place = person.get('birth_place', None)
                website = person.get('website', None)
                image_url = person.get('image_url', None)
            else:
                person_name = getattr(person, 'name', '')
                person_id = getattr(person, 'id', None) or str(uuid.uuid4())
                birth_year = getattr(person, 'birth_year', None)
                death_year = getattr(person, 'death_year', None)
                bio = getattr(person, 'bio', '')
                created_at = getattr(person, 'created_at', datetime.utcnow())
                openlibrary_id = getattr(person, 'openlibrary_id', None)
                birth_place = getattr(person, 'birth_place', None)
                website = getattr(person, 'website', None)
                image_url = getattr(person, 'image_url', None)
            
            if not person_name:
                logger.error("❌ Person name is required")
                return None
            
            # Auto-fetch OpenLibrary metadata if not already provided
            logger.info(f"🔍 [DEBUG] CREATE: Checking auto-fetch conditions for {person_name}: openlibrary_id={openlibrary_id}, bio='{bio}', birth_year={birth_year}, image_url={image_url}")
            if not openlibrary_id and not bio and not birth_year and not image_url:
                try:
                    from ..utils import search_author_by_name, fetch_author_data
                    logger.info(f"🔍 Auto-fetching OpenLibrary metadata for: {person_name}")
                    search_result = search_author_by_name(person_name)
                    
                    if search_result and search_result.get('openlibrary_id'):
                        # Get detailed author data using the OpenLibrary ID
                        author_id = search_result['openlibrary_id']
                        detailed_author_data = fetch_author_data(author_id)
                        
                        if detailed_author_data:
                            # Use the same comprehensive parser as the person metadata refresh
                            from ..routes.people_routes import parse_comprehensive_openlibrary_data
                            
                            # Parse comprehensive data
                            updates = parse_comprehensive_openlibrary_data(detailed_author_data)
                            
                            # Apply all the comprehensive updates if not already provided
                            if not openlibrary_id and updates.get('openlibrary_id'):
                                openlibrary_id = updates['openlibrary_id']
                            if not bio and updates.get('bio'):
                                bio = updates['bio']
                            if not birth_year and updates.get('birth_year'):
                                birth_year = updates['birth_year']
                            if not death_year and updates.get('death_year'):
                                death_year = updates['death_year']
                            if not image_url and updates.get('image_url'):
                                image_url = updates['image_url']
                            if not birth_place and updates.get('birth_place'):
                                birth_place = updates['birth_place']
                            if not website and updates.get('website'):
                                website = updates['website']
                            
                            logger.info(f"✅ Auto-fetched comprehensive metadata for {person_name}: OpenLibrary ID {openlibrary_id}")
                            logger.info(f"📚 Applied fields: {list(updates.keys())}")
                        else:
                            logger.warning(f"⚠️ Could not fetch detailed data for OpenLibrary ID: {author_id}")
                    else:
                        logger.info(f"📝 No OpenLibrary data found for: {person_name}")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Failed to auto-fetch metadata for {person_name}: {e}")
                    # Continue with person creation even if metadata fetch fails
            
            # Only include properties that exist in the Person schema
            person_data = {
                'id': person_id,
                'name': person_name,
                'normalized_name': person_name.strip().lower(),
                'birth_year': birth_year,
                'death_year': death_year,
                'birth_place': birth_place,
                'bio': bio,
                'website': website,
                'openlibrary_id': openlibrary_id,
                'image_url': image_url,
                'created_at': created_at,
                'updated_at': datetime.utcnow().isoformat()
            }
            
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
            print(f"🔧 [PERSON_REPO] Getting person by ID: {person_id}")
            query = """
            MATCH (p:Person {id: $person_id})
            RETURN p
            LIMIT 1
            """
            
            results = self.db.query(query, {"person_id": person_id})
            print(f"🔧 [PERSON_REPO] Query results: {results}")
            print(f"🔧 [PERSON_REPO] Results length: {len(results) if results else 0}")
            
            if results and len(results) > 0:
                print(f"🔧 [PERSON_REPO] First result keys: {list(results[0].keys()) if results[0] else 'None'}")
                
                # Try different possible key formats
                if 'result' in results[0]:
                    person_data = dict(results[0]['result'])
                    print(f"🔧 [PERSON_REPO] Found using 'result' key: {person_data}")
                    return person_data
                elif 'col_0' in results[0]:
                    person_data = dict(results[0]['col_0'])
                    print(f"🔧 [PERSON_REPO] Found using 'col_0' key: {person_data}")
                    return person_data
                elif 'p' in results[0]:
                    person_data = dict(results[0]['p'])
                    print(f"🔧 [PERSON_REPO] Found using 'p' key: {person_data}")
                    return person_data
                else:
                    print(f"🔧 [PERSON_REPO] No recognized key format found")
                    return None
            else:
                print(f"🔧 [PERSON_REPO] No results found")
                return None
            
        except Exception as e:
            print(f"🔧 [PERSON_REPO] Exception in get_by_id: {type(e).__name__}: {str(e)}")
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
                if key in ['name', 'normalized_name', 'birth_year', 'death_year', 'bio', 'openlibrary_id', 'birth_place', 'website', 'image_url']:
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


class KuzuBookRepository:
    """Clean book repository using simplified Kuzu schema."""
    
    def __init__(self):
        self.db = get_kuzu_database()
    
    async def create(self, book: Any) -> Optional[Any]:
        """Create a new book with relationships."""
        try:
            
            # Debug contributors
            contributors = getattr(book, 'contributors', [])
            for i, contrib in enumerate(contributors):
                person = getattr(contrib, 'person', None)
                person_name = getattr(person, 'name', 'unknown') if person else 'no person'
                contrib_type = getattr(contrib, 'contribution_type', 'unknown')
            
            # Debug categories
            categories = getattr(book, 'categories', [])
            raw_categories = getattr(book, 'raw_categories', None)
            
            # Debug publisher
            publisher = getattr(book, 'publisher', None)
            if publisher:
                publisher_name = getattr(publisher, 'name', 'unknown')
            else:
                publisher_name = 'unknown'
            
            if not getattr(book, 'id', None):
                if hasattr(book, 'id'):
                    book.id = str(uuid.uuid4())
            
            # Handle series field - it can be a Series object or a string
            series_value = None
            series_obj = getattr(book, 'series', None)
            if series_obj:
                if hasattr(series_obj, 'name'):
                    # Series object with name attribute
                    series_value = series_obj.name
                else:
                    # Assume it's already a string
                    series_value = str(series_obj)
            
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
                'series': series_value,
                'series_volume': getattr(book, 'series_volume', None),
                'series_order': getattr(book, 'series_order', None),
                'custom_metadata': getattr(book, 'custom_metadata', None),
                'created_at': getattr(book, 'created_at', datetime.utcnow()).isoformat() if hasattr(getattr(book, 'created_at', datetime.utcnow()), 'isoformat') else datetime.utcnow().isoformat()
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
            if contribution_type and hasattr(contribution_type, 'value'):
                contribution_str = contribution_type.value.upper()
            else:
                contribution_str = str(contribution_type).upper() if contribution_type else 'AUTHORED'
            
            # Map contribution types to relationship types
            rel_type_map = {
                'AUTHORED': 'AUTHORED',
                'EDITED': 'AUTHORED',  # Use AUTHORED relationship with role property
                'TRANSLATED': 'AUTHORED',  # Use AUTHORED relationship with role property
                'ILLUSTRATED': 'AUTHORED',  # Use AUTHORED relationship with role property
                'NARRATED': 'AUTHORED',  # Use AUTHORED relationship with role property
                'GAVE_FOREWORD': 'AUTHORED',  # Use AUTHORED relationship with role property
                'GAVE_INTRODUCTION': 'AUTHORED',  # Use AUTHORED relationship with role property
                'GAVE_AFTERWORD': 'AUTHORED',  # Use AUTHORED relationship with role property
                'COMPILED': 'AUTHORED',  # Use AUTHORED relationship with role property
                'CONTRIBUTED': 'AUTHORED',  # Use AUTHORED relationship with role property
                'CO_AUTHORED': 'AUTHORED',  # Use AUTHORED relationship with role property
                'GHOST_WROTE': 'AUTHORED'  # Use AUTHORED relationship with role property
            }
            
            rel_type = rel_type_map.get(contribution_str, 'AUTHORED')
            
            # Create the relationship with properties
            role = contribution_str.lower()
            rel_props = {
                'role': role,
                'order_index': getattr(contribution, 'order', order_index)
            }
            
            logger.info(f"🔍 [DEBUG] Creating relationship with role: {role}, contribution_str: {contribution_str}")
            
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
            RETURN p.id
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
            
            # Auto-fetch OpenLibrary metadata if available
            birth_year = getattr(person, 'birth_year', None)
            death_year = getattr(person, 'death_year', None)
            bio = getattr(person, 'bio', '')
            openlibrary_id = getattr(person, 'openlibrary_id', None)
            image_url = getattr(person, 'image_url', None)
            birth_place = getattr(person, 'birth_place', None)
            website = getattr(person, 'website', None)
            
            # Auto-fetch OpenLibrary metadata if not already provided
            logger.info(f"🔍 [DEBUG] Checking auto-fetch conditions for {person_name}: openlibrary_id={openlibrary_id}, bio='{bio}', birth_year={birth_year}, image_url={image_url}")
            if not openlibrary_id and not bio and not birth_year and not image_url:
                try:
                    from ..utils import search_author_by_name, fetch_author_data
                    logger.info(f"🔍 Auto-fetching OpenLibrary metadata for: {person_name}")
                    search_result = search_author_by_name(person_name)
                    
                    if search_result and search_result.get('openlibrary_id'):
                        # Get detailed author data using the OpenLibrary ID
                        author_id = search_result['openlibrary_id']
                        detailed_author_data = fetch_author_data(author_id)
                        
                        if detailed_author_data:
                            # Use the same comprehensive parser as the person metadata refresh
                            from ..routes.people_routes import parse_comprehensive_openlibrary_data
                            
                            # Parse comprehensive data
                            updates = parse_comprehensive_openlibrary_data(detailed_author_data)
                            
                            # Apply all the comprehensive updates if not already provided
                            if not openlibrary_id and updates.get('openlibrary_id'):
                                openlibrary_id = updates['openlibrary_id']
                            if not bio and updates.get('bio'):
                                bio = updates['bio']
                            if not birth_year and updates.get('birth_year'):
                                birth_year = updates['birth_year']
                            if not death_year and updates.get('death_year'):
                                death_year = updates['death_year']
                            if not image_url and updates.get('image_url'):
                                image_url = updates['image_url']
                            if not birth_place and updates.get('birth_place'):
                                birth_place = updates['birth_place']
                            if not website and updates.get('website'):
                                website = updates['website']
                            
                            logger.info(f"✅ Auto-fetched comprehensive metadata for {person_name}: OpenLibrary ID {openlibrary_id}")
                            logger.info(f"📚 Applied fields: {list(updates.keys())}")
                        else:
                            logger.warning(f"⚠️ Could not fetch detailed data for OpenLibrary ID: {author_id}")
                    else:
                        logger.info(f"📝 No OpenLibrary data found for: {person_name}")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Failed to auto-fetch metadata for {person_name}: {e}")
                    # Continue with person creation even if metadata fetch fails
            
            # Only include properties that exist in the Person schema
            person_data = {
                'id': person_id,
                'name': person_name,
                'normalized_name': normalized_name,
                'birth_year': birth_year,
                'death_year': death_year,
                'birth_place': birth_place,
                'bio': bio,
                'website': website,
                'openlibrary_id': openlibrary_id,
                'image_url': image_url,
                'created_at': getattr(person, 'created_at', datetime.utcnow()).isoformat() if hasattr(getattr(person, 'created_at', datetime.utcnow()), 'isoformat') else datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
            }
            # Filter out fields that don't exist in the current schema
            # birth_place and website are not in the current schema, but openlibrary_id and image_url are
            filtered_person_data = {
                'id': person_data['id'],
                'name': person_data['name'],
                'normalized_name': person_data['normalized_name'],
                'birth_year': person_data['birth_year'],
                'death_year': person_data['death_year'],
                'bio': person_data['bio'],
                'openlibrary_id': person_data['openlibrary_id'],
                'image_url': person_data['image_url'],
                'created_at': person_data['created_at']
            }
            
            success = self.db.create_node('Person', filtered_person_data)
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
            RETURN c.id
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
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat()
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
            RETURN p.id
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
                'created_at': datetime.utcnow().isoformat()
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
        """Get all contributors for a book with their roles."""
        try:
            query = """
            MATCH (p:Person)-[rel:AUTHORED]->(b:Book {id: $book_id})
            RETURN p.name as name, p.id as id, rel.role as role, rel.order_index as order_index
            ORDER BY rel.order_index ASC
            """
            
            results = self.db.query(query, {"book_id": book_id})
            
            contributors = []
            for result in results:
                if result.get('col_0'):  # name
                    contributors.append({
                        'name': result.get('col_0', ''),
                        'id': result.get('col_1', ''),
                        'role': result.get('col_2', 'authored'),  # Default to 'authored' if no role
                        'order_index': result.get('col_3', 0)
                    })
            
            logger.debug(f"Found {len(contributors)} contributors for book {book_id}")
            return contributors
            
        except Exception as e:
            logger.error(f"❌ Failed to get book authors: {e}")
            return []
    
    async def get_book_categories(self, book_id: str) -> List[Dict[str, Any]]:
        """Get all categories for a book."""
        try:
            query = """
            MATCH (b:Book {id: $book_id})-[:CATEGORIZED_AS]->(c:Category)
            RETURN c.name as name, c.id as id, c.description as description, 
                   c.color as color, c.icon as icon, c.aliases as aliases,
                   c.normalized_name as normalized_name, c.parent_id as parent_id,
                   c.level as level, c.book_count as book_count, c.user_book_count as user_book_count,
                   c.created_at as created_at, c.updated_at as updated_at
            ORDER BY c.name ASC
            """
            
            results = self.db.query(query, {"book_id": book_id})
            
            categories = []
            for result in results:
                if result.get('col_0'):  # name
                    categories.append({
                        'name': result.get('col_0', ''),
                        'id': result.get('col_1', ''),
                        'description': result.get('col_2', ''),
                        'color': result.get('col_3', ''),
                        'icon': result.get('col_4', ''),
                        'aliases': result.get('col_5', []),
                        'normalized_name': result.get('col_6', ''),
                        'parent_id': result.get('col_7', None),
                        'level': result.get('col_8', 0),
                        'book_count': result.get('col_9', 0),
                        'user_book_count': result.get('col_10', 0),
                        'created_at': result.get('col_11', None),
                        'updated_at': result.get('col_12', None)
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
        """Get all persons in the database with book counts."""
        try:
            query = """
            MATCH (p:Person)
            OPTIONAL MATCH (p)-[:AUTHORED]->(b:Book)
            RETURN p, COUNT(DISTINCT b) as book_count
            ORDER BY p.name ASC
            """
            
            results = self.db.query(query)
            
            persons = []
            for result in results:
                # Handle both result formats for two column queries
                person_data = None
                book_count = 0
                
                if 'col_0' in result and 'col_1' in result:
                    person_data = dict(result['col_0'])
                    book_count = result['col_1'] or 0
                elif 'result' in result:
                    # Fallback for single column format
                    person_data = dict(result['result'])
                    book_count = 0
                
                if person_data:
                    person_data['book_count'] = book_count
                    persons.append(person_data)
            
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

    async def delete(self, book_id: str) -> bool:
        """Delete a book and all its relationships globally."""
        try:
            logger.info(f"🗑️ Starting global delete for book ID: {book_id}")
            
            # Use DETACH DELETE to remove the book and all its relationships
            delete_query = """
            MATCH (b:Book {id: $book_id})
            DETACH DELETE b
            """
            
            # Execute the deletion
            self.db.query(delete_query, {"book_id": book_id})
            logger.info(f"✅ Successfully deleted book and all relationships: {book_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to delete book {book_id}: {e}")
            return False


class KuzuUserBookRepository:
    """Repository for user-book relationships."""
    
    def __init__(self):
        self.db = get_kuzu_database()
    
    async def add_book_to_library(self, user_id: str, book_id: str, 
                                 reading_status: str = "plan_to_read",
                                 ownership_status: str = "owned",
                                 media_type: str = "physical",
                                 notes: str = "",
                                 location_id: Optional[str] = None) -> bool:
        """Add a book to user's library."""
        try:
            # Ensure all properties are strings except date_added which should be TIMESTAMP
            owns_props = {
                'reading_status': str(reading_status),
                'ownership_status': str(ownership_status),
                'media_type': str(media_type),
                'date_added': datetime.utcnow().isoformat(),  # Always use current time as ISO string
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
    
    async def get_user_books(self, user_id: str, reading_status: Optional[str] = None, 
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
                             location_id: Optional[str] = None, source: str = "manual",
                             notes: str = "", date_added = None, 
                             custom_metadata: Optional[Dict[str, Any]] = None) -> bool:
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
            
            success = self.db.create_relationship(
                'User', user_id, 'OWNS', 'Book', book_id, owns_props
            )
            
            if success and custom_metadata:
                # Also store individual custom field relationships for tracking
                from app.infrastructure.kuzu_graph import get_graph_storage
                storage = get_graph_storage()
                
                for field_name, field_value in custom_metadata.items():
                    try:
                        storage.store_custom_metadata(user_id, book_id, field_name, str(field_value))
                    except Exception as e:
                        pass  # Log custom metadata storage error but continue
            
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
    
    async def get_user_statistics(self, user_id: str) -> Dict[str, Any]:
        """Get comprehensive user library statistics."""
        try:
            stats = {}
            
            # Get total books count
            query = """
            MATCH (u:User {id: $user_id})-[:OWNS]->(b:Book)
            RETURN COUNT(b) as total_books
            """
            result = self.db.query(query, {"user_id": user_id})
            stats['total_books'] = result[0].get('col_0', 0) if result else 0
            
            # Get counts by reading status
            status_queries = {
                'plan_to_read': 'plan_to_read',
                'currently_reading': 'currently_reading',
                'completed': 'completed',
                'on_hold': 'on_hold',
                'dropped': 'dropped'
            }
            
            for status_key, status_value in status_queries.items():
                query = """
                MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
                WHERE owns.reading_status = $status
                RETURN COUNT(b) as count
                """
                result = self.db.query(query, {"user_id": user_id, "status": status_value})
                stats[status_key] = result[0].get('col_0', 0) if result else 0
            
            # Get counts by ownership status
            ownership_queries = {
                'owned': 'owned',
                'wishlist': 'wishlist',
                'borrowed': 'borrowed',
                'loaned': 'loaned'
            }
            
            for ownership_key, ownership_value in ownership_queries.items():
                query = """
                MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
                WHERE owns.ownership_status = $status
                RETURN COUNT(b) as count
                """
                result = self.db.query(query, {"user_id": user_id, "status": ownership_value})
                stats[ownership_key] = result[0].get('col_0', 0) if result else 0
            
            # Get counts by media type
            media_queries = {
                'physical': 'physical',
                'ebook': 'ebook',
                'audiobook': 'audiobook'
            }
            
            for media_key, media_value in media_queries.items():
                query = """
                MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
                WHERE owns.media_type = $media_type
                RETURN COUNT(b) as count
                """
                result = self.db.query(query, {"user_id": user_id, "media_type": media_value})
                stats[media_key] = result[0].get('col_0', 0) if result else 0
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Failed to get user statistics: {e}")
            return {}
    
    async def get_reading_timeline(self, user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Get user's reading timeline - recent activity and books."""
        try:
            # Get recent books added to library, ordered by date_added
            query = """
            MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
            RETURN b, owns
            ORDER BY owns.date_added DESC
            LIMIT $limit
            """
            
            results = self.db.query(query, {"user_id": user_id, "limit": limit})
            
            timeline = []
            for result in results:
                if 'col_0' in result and 'col_1' in result:
                    book_data = dict(result['col_0'])
                    owns_data = dict(result['col_1'])
                    
                    # Create timeline entry
                    timeline_entry = {
                        'book': book_data,
                        'activity_type': 'added_to_library',
                        'activity_date': owns_data.get('date_added'),
                        'reading_status': owns_data.get('reading_status'),
                        'ownership_status': owns_data.get('ownership_status'),
                        'media_type': owns_data.get('media_type'),
                        'notes': owns_data.get('personal_notes', '')
                    }
                    timeline.append(timeline_entry)
            
            return timeline
            
        except Exception as e:
            logger.error(f"❌ Failed to get reading timeline: {e}")
            return []


class KuzuLocationRepository:
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
                'user_id': user_id,  # Store user_id as property
                'name': getattr(location, 'name', ''),
                'description': getattr(location, 'description', ''),
                'location_type': getattr(location, 'location_type', 'room'),
                'is_default': getattr(location, 'is_default', False),
                'created_at': getattr(location, 'created_at', datetime.utcnow()).isoformat() if hasattr(getattr(location, 'created_at', datetime.utcnow()), 'isoformat') else datetime.utcnow().isoformat()
            }
            
            success = self.db.create_node('Location', location_data)
            if success:
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
            MATCH (l:Location {user_id: $user_id})
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
            MATCH (l:Location {user_id: $user_id, is_default: true})
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


class KuzuCategoryRepository:
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
            parent_id = getattr(category, 'parent_id', None)
            
            # Debug parent_id value
            logger.info(f"📁 [CREATE_CATEGORY] Creating category '{category_name}' with parent_id: {parent_id} (type: {type(parent_id)})")
            
            # Include all properties that exist in the Category schema
            category_data = {
                'id': category_id,
                'name': category_name,
                'normalized_name': category_name.strip().lower(),
                'description': getattr(category, 'description', ''),
                'parent_id': parent_id,  # Include parent_id
                'level': getattr(category, 'level', 0),  # Default to root level
                'color': getattr(category, 'color', ''),
                'icon': getattr(category, 'icon', ''),
                'aliases': getattr(category, 'aliases', []),
                'book_count': getattr(category, 'book_count', 0),
                'user_book_count': getattr(category, 'user_book_count', 0),
                'created_at': getattr(category, 'created_at', datetime.utcnow()),
                'updated_at': getattr(category, 'updated_at', datetime.utcnow())
            }
            # Note: parent_id and aliases need special handling
            
            success = self.db.create_node('Category', category_data)
            if success:
                logger.info(f"✅ Created new category: {category_name} (ID: {category_id})")
                # Create a new category object with the generated ID
                from app.domain.models import Category
                created_category = Category(
                    id=category_id,
                    name=category_name,
                    normalized_name=category_name.strip().lower(),
                    description=getattr(category, 'description', ''),
                    parent_id=parent_id,
                    level=getattr(category, 'level', 0),
                    color=getattr(category, 'color', None),
                    icon=getattr(category, 'icon', None),
                    aliases=getattr(category, 'aliases', []),
                    book_count=getattr(category, 'book_count', 0),
                    user_book_count=getattr(category, 'user_book_count', 0),
                    created_at=getattr(category, 'created_at', datetime.utcnow()),
                    updated_at=getattr(category, 'updated_at', datetime.utcnow())
                )
                return created_category
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

    async def update(self, category: Any) -> Optional[Any]:
        """Update an existing category."""
        try:
            category_id = getattr(category, 'id', '')
            if not category_id:
                logger.error("❌ Category ID is required for update")
                return None
            
            # Prepare update data
            category_data = {
                'id': category_id,
                'name': getattr(category, 'name', ''),
                'normalized_name': getattr(category, 'name', '').strip().lower(),
                'description': getattr(category, 'description', ''),
                'parent_id': getattr(category, 'parent_id', None),
                'level': getattr(category, 'level', 0),
                'color': getattr(category, 'color', ''),
                'icon': getattr(category, 'icon', ''),
                'aliases': getattr(category, 'aliases', []),
                'updated_at': datetime.utcnow()  # KuzuDB requires datetime objects for TIMESTAMP fields
            }
            
            # Update using Kuzu query
            query = """
            MATCH (c:Category {id: $id})
            SET c.name = $name,
                c.normalized_name = $normalized_name,
                c.description = $description,
                c.parent_id = $parent_id,
                c.level = $level,
                c.color = $color,
                c.icon = $icon,
                c.aliases = $aliases,
                c.updated_at = $updated_at
            RETURN c
            """
            
            results = self.db.query(query, category_data)
            
            if results:
                logger.info(f"✅ Updated category: {getattr(category, 'name', 'unknown')} (ID: {category_id})")
                return category
            else:
                logger.error(f"❌ Failed to update category: {getattr(category, 'name', 'unknown')}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to update category: {e}")
            return None


class KuzuCustomFieldRepository:
    """Clean custom field repository using simplified Kuzu schema."""
    
    def __init__(self):
        self.db = get_kuzu_database()
    
    async def create(self, field_def: Any) -> Optional[Any]:
        """Create a new custom field definition."""
        try:
            if not getattr(field_def, 'id', None):
                if hasattr(field_def, 'id'):
                    field_def.id = str(uuid.uuid4())
            
            field_data = {
                'id': getattr(field_def, 'id', str(uuid.uuid4())),
                'name': getattr(field_def, 'name', ''),
                'display_name': getattr(field_def, 'display_name', ''),
                'field_type': getattr(field_def, 'field_type', ''),
                'description': getattr(field_def, 'description', ''),
                'created_by_user_id': getattr(field_def, 'created_by_user_id', ''),
                'is_shareable': getattr(field_def, 'is_shareable', False),
                'is_global': getattr(field_def, 'is_global', False),
                'default_value': getattr(field_def, 'default_value', ''),
                'usage_count': getattr(field_def, 'usage_count', 0),
                'created_at': getattr(field_def, 'created_at', datetime.utcnow()).isoformat() if hasattr(getattr(field_def, 'created_at', datetime.utcnow()), 'isoformat') else datetime.utcnow().isoformat()
            }
            
            # Convert enum to string if needed
            if hasattr(field_data['field_type'], 'value'):
                field_data['field_type'] = field_data['field_type'].value
            
            success = self.db.create_node('CustomField', field_data)
            if success:
                logger.info(f"✅ Created custom field: {getattr(field_def, 'name', 'unknown')}")
                return field_def
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to create custom field: {e}")
            return None
    
    async def get_by_id(self, field_id: str) -> Optional[Any]:
        """Get a custom field by ID."""
        try:
            field_data = self.db.get_node('CustomField', field_id)
            if field_data:
                return field_data
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get custom field {field_id}: {e}")
            return None
    
    async def get_by_user(self, user_id: str) -> List[Any]:
        """Get all custom fields for a user."""
        try:
            query = """
            MATCH (cf:CustomField)
            WHERE cf.created_by_user_id = $user_id
            RETURN cf
            """
            results = self.db.query(query, {"user_id": user_id})
            fields = []
            for result in results:
                if 'col_0' in result:
                    fields.append(dict(result['col_0']))
            return fields
            
        except Exception as e:
            logger.error(f"❌ Failed to get custom fields for user {user_id}: {e}")
            return []
    
    async def update(self, field_def: Any) -> Optional[Any]:
        """Update an existing custom field."""
        try:
            field_id = getattr(field_def, 'id', None)
            if not field_id:
                return None
            
            field_data = {
                'name': getattr(field_def, 'name', ''),
                'display_name': getattr(field_def, 'display_name', ''),
                'description': getattr(field_def, 'description', ''),
                'is_shareable': getattr(field_def, 'is_shareable', False),
                'updated_at': datetime.utcnow().isoformat()
            }
            
            success = self.db.update_node('CustomField', field_id, field_data)
            return field_def if success else None
            
        except Exception as e:
            logger.error(f"❌ Failed to update custom field: {e}")
            return None
    
    async def delete(self, field_id: str) -> bool:
        """Delete a custom field."""
        try:
            return self.db.delete_node('CustomField', field_id)
        except Exception as e:
            logger.error(f"❌ Failed to delete custom field {field_id}: {e}")
            return False
    
    async def get_shareable(self, exclude_user_id: Optional[str] = None) -> List[Any]:
        """Get all shareable custom field definitions."""
        try:
            query_conditions = ["cf.is_shareable = true"]
            params = {}
            
            if exclude_user_id:
                query_conditions.append("cf.created_by_user_id <> $exclude_user_id")
                params['exclude_user_id'] = exclude_user_id
            
            where_clause = " AND ".join(query_conditions)
            query = f"""
            MATCH (cf:CustomField)
            WHERE {where_clause}
            RETURN cf
            ORDER BY cf.usage_count DESC
            """
            
            results = self.db.query(query, params)
            fields = []
            for result in results:
                if 'col_0' in result:
                    fields.append(dict(result['col_0']))
            return fields
            
        except Exception as e:
            logger.error(f"❌ Failed to get shareable custom fields: {e}")
            return []
    
    async def search(self, query: str, user_id: Optional[str] = None) -> List[Any]:
        """Search custom field definitions."""
        try:
            query_conditions = []
            params = {"search_query": f"%{query}%"}
            
            query_conditions.append("(cf.name CONTAINS $search_query OR cf.description CONTAINS $search_query)")
            
            if user_id:
                query_conditions.append("(cf.created_by_user_id = $user_id OR cf.is_shareable = true)")
                params['user_id'] = user_id
            else:
                query_conditions.append("cf.is_shareable = true")
            
            where_clause = " AND ".join(query_conditions)
            cypher_query = f"""
            MATCH (cf:CustomField)
            WHERE {where_clause}
            RETURN cf
            ORDER BY cf.usage_count DESC
            LIMIT 50
            """
            
            results = self.db.query(cypher_query, params)
            fields = []
            for result in results:
                if 'col_0' in result:
                    fields.append(dict(result['col_0']))
            return fields
            
        except Exception as e:
            logger.error(f"❌ Failed to search custom fields: {e}")
            return []
    
    async def increment_usage(self, field_id: str) -> None:
        """Increment usage count for a field definition."""
        try:
            query = """
            MATCH (cf:CustomField)
            WHERE cf.id = $field_id
            SET cf.usage_count = COALESCE(cf.usage_count, 0) + 1
            """
            self.db.query(query, {"field_id": field_id})
        except Exception as e:
            logger.warning(f"Could not increment usage count for field {field_id}: {e}")
    
    async def get_popular(self, limit: int = 20) -> List[Any]:
        """Get most popular shareable custom field definitions."""
        try:
            query = """
            MATCH (cf:CustomField)
            WHERE cf.is_shareable = true
            RETURN cf
            ORDER BY COALESCE(cf.usage_count, 0) DESC
            LIMIT $limit
            """
            
            results = self.db.query(query, {"limit": limit})
            fields = []
            for result in results:
                if 'col_0' in result:
                    fields.append(dict(result['col_0']))
            return fields
            
        except Exception as e:
            logger.error(f"❌ Failed to get popular custom fields: {e}")
            return []


class KuzuImportMappingRepository:
    """Clean import mapping repository using simplified Kuzu schema."""
    
    def __init__(self):
        self.db = get_kuzu_database()
    
    async def create(self, template: Any) -> Optional[Any]:
        """Create a new import mapping template."""
        try:
            if not getattr(template, 'id', None):
                if hasattr(template, 'id'):
                    template.id = str(uuid.uuid4())
            
            template_data = {
                'id': getattr(template, 'id', str(uuid.uuid4())),
                'name': getattr(template, 'name', ''),
                'description': getattr(template, 'description', ''),
                'user_id': getattr(template, 'user_id', ''),
                'import_type': getattr(template, 'import_type', ''),
                'field_mappings': getattr(template, 'field_mappings', ''),
                'sample_headers': getattr(template, 'sample_headers', ''),
                'usage_count': getattr(template, 'usage_count', 0),
                'created_at': getattr(template, 'created_at', datetime.utcnow()).isoformat() if hasattr(getattr(template, 'created_at', datetime.utcnow()), 'isoformat') else datetime.utcnow().isoformat()
            }
            
            success = self.db.create_node('ImportMapping', template_data)
            if success:
                logger.info(f"✅ Created import mapping template: {getattr(template, 'name', 'unknown')}")
                return template
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to create import mapping template: {e}")
            return None
    
    async def get_by_id(self, template_id: str) -> Optional[Any]:
        """Get an import mapping template by ID."""
        try:
            template_data = self.db.get_node('ImportMapping', template_id)
            if template_data:
                return template_data
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get import mapping template {template_id}: {e}")
            return None
    
    async def get_by_user(self, user_id: str) -> List[Any]:
        """Get all import mapping templates for a user."""
        try:
            query = """
            MATCH (im:ImportMapping)
            WHERE im.user_id = $user_id
            RETURN im
            """
            results = self.db.query(query, {"user_id": user_id})
            templates = []
            for result in results:
                if 'col_0' in result:
                    templates.append(dict(result['col_0']))
            return templates
            
        except Exception as e:
            logger.error(f"❌ Failed to get import mapping templates for user {user_id}: {e}")
            return []
    
    async def update(self, template: Any) -> Optional[Any]:
        """Update an existing import mapping template."""
        try:
            template_id = getattr(template, 'id', None)
            if not template_id:
                return None
            
            template_data = {
                'name': getattr(template, 'name', ''),
                'description': getattr(template, 'description', ''),
                'field_mappings': getattr(template, 'field_mappings', ''),
                'updated_at': datetime.utcnow().isoformat()
            }
            
            success = self.db.update_node('ImportMapping', template_id, template_data)
            return template if success else None
            
        except Exception as e:
            logger.error(f"❌ Failed to update import mapping template: {e}")
            return None
    
    async def delete(self, template_id: str) -> bool:
        """Delete an import mapping template."""
        try:
            return self.db.delete_node('ImportMapping', template_id)
        except Exception as e:
            logger.error(f"❌ Failed to delete import mapping template {template_id}: {e}")
            return False
    
    async def detect_template(self, headers: List[str], user_id: str) -> Optional[Any]:
        """Detect matching template based on CSV headers."""
        try:
            headers_lower = [h.lower().strip() for h in headers]
            
            # Get user's templates
            user_templates = await self.get_by_user(user_id)
            
            # Get system templates
            system_templates = await self.get_system_templates()
            
            # Combine templates
            all_templates = user_templates + system_templates
            
            best_match = None
            best_score = 0
            
            for template in all_templates:
                sample_headers = template.get('sample_headers', [])
                if not sample_headers:
                    continue
                    
                template_headers_lower = [h.lower().strip() for h in sample_headers]
                
                # Count exact matches
                exact_matches = len(set(headers_lower) & set(template_headers_lower))
                
                # Calculate percentage match
                if len(template_headers_lower) > 0:
                    match_score = exact_matches / len(template_headers_lower)
                    
                    if match_score >= 0.7 and match_score > best_score:
                        best_match = template
                        best_score = match_score
            
            return best_match
            
        except Exception as e:
            logger.error(f"❌ Failed to detect template: {e}")
            return None
    
    async def get_system_templates(self) -> List[Any]:
        """Get all system default templates."""
        try:
            query = """
            MATCH (im:ImportMapping)
            WHERE im.user_id = '__system__'
            RETURN im
            """
            results = self.db.query(query)
            templates = []
            for result in results:
                if 'col_0' in result:
                    templates.append(dict(result['col_0']))
            return templates
            
        except Exception as e:
            logger.error(f"❌ Failed to get system templates: {e}")
            return []
    
    async def increment_usage(self, template_id: str) -> None:
        """Increment usage count and update last used timestamp."""
        try:
            query = """
            MATCH (im:ImportMapping)
            WHERE im.id = $template_id
            SET im.usage_count = COALESCE(im.usage_count, 0) + 1,
                im.updated_at = $updated_at
            """
            self.db.query(query, {
                "template_id": template_id,
                "updated_at": datetime.utcnow().isoformat()
            })
        except Exception as e:
            logger.warning(f"Could not increment usage count for template {template_id}: {e}")
