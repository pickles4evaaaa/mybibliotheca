"""
Clean Kuzu repositories that work with the simplified graph schema.
"""

import uuid
import logging
from typing import Optional, List, Dict, Any, TYPE_CHECKING
from datetime import datetime, date, timezone

if TYPE_CHECKING:
    # Use TYPE_CHECKING to avoid circular imports during runtime
    from ..domain.models import (
        User, Book, Person, Category, Series, Publisher, Location,
        ReadingStatus, OwnershipStatus, MediaType
    )

from ..utils.safe_kuzu_manager import SafeKuzuManager, get_safe_kuzu_manager, safe_get_connection

# Set up logging
logger = logging.getLogger(__name__)

def _safe_get_row_value(row: Any, index: int) -> Any:
    """Safely extract a value from a KuzuDB row at the given index."""
    if isinstance(row, list):
        return row[index] if index < len(row) else None
    elif isinstance(row, dict):
        keys = list(row.keys())
        return row[keys[index]] if index < len(keys) else None
    else:
        try:
            return row[index]  # type: ignore
        except (IndexError, KeyError, TypeError):
            return None

# Compatibility adapter for repository patterns
class KuzuRepositoryAdapter:
    """
    Adapter that provides the old repository interface (create_node, query, etc.)
    while using SafeKuzuManager underneath for thread safety.
    """
    
    def __init__(self, safe_manager: SafeKuzuManager):
        self.safe_manager = safe_manager
    
    def create_node(self, node_type: str, node_data: Dict[str, Any]) -> bool:
        """Create a node using SafeKuzuManager."""
        try:
            # Build dynamic CREATE query with timestamp handling
            props = []
            for key, value in node_data.items():
                if (key.endswith('_str') and ('created_at' in key or 'updated_at' in key)):
                    # Handle timestamp fields specially with empty-string guard
                    timestamp_field = key.replace('_str', '')
                    props.append(f"{timestamp_field}: CASE WHEN ${key} IS NULL OR ${key} = '' THEN NULL ELSE timestamp(${key}) END")
                else:
                    props.append(f"{key}: ${key}")
            
            query = f"""
            CREATE (n:{node_type} {{{', '.join(props)}}})
            RETURN n.id as id
            """
            
            result = self.safe_manager.execute_query(query, node_data)
            return result is not None
        except Exception as e:
            logger.error(f"Failed to create {node_type} node: {e}")
            return False
    
    def query(self, cypher_query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a query using SafeKuzuManager and return results in old format."""
        try:
            result = self.safe_manager.execute_query(cypher_query, params or {})
            if not result:
                return []
            
            # Convert to the same format as the old KuzuGraphDB.query() method
            rows = []
            while result.has_next():
                row = result.get_next()
                # Convert row to dict format expected by services
                if len(row) == 1:
                    # Single column result
                    rows.append({'result': _safe_get_row_value(row, 0)})
                else:
                    # Multiple columns - create dict with generic column names
                    row_dict = {}
                    for i in range(len(row)):
                        row_dict[f'col_{i}'] = row[i]
                    rows.append(row_dict)
            
            logger.info(f"🔍 [ADAPTER] Query returned {len(rows)} rows")
            return rows
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return []
    
    def create_relationship(self, from_type: str, from_id: str, rel_type: str, 
                          to_type: str, to_id: str, rel_props: Optional[Dict[str, Any]] = None) -> bool:
        """Create a relationship using SafeKuzuManager."""
        try:
            if rel_props:
                props = []
                for key, value in rel_props.items():
                    props.append(f"{key}: ${key}")
                rel_clause = f" {{{', '.join(props)}}}"
                params = {**rel_props, 'from_id': from_id, 'to_id': to_id}
            else:
                rel_clause = ""
                params = {'from_id': from_id, 'to_id': to_id}
            
            query = f"""
            MATCH (from:{from_type} {{id: $from_id}}), (to:{to_type} {{id: $to_id}})
            CREATE (from)-[r:{rel_type}{rel_clause}]->(to)
            RETURN r
            """
            
            result = self.safe_manager.execute_query(query, params)
            return result is not None
        except Exception as e:
            logger.error(f"Failed to create {rel_type} relationship: {e}")
            return False
    
    def get_node(self, node_type: str, node_id: str) -> Optional[Dict[str, Any]]:
        """Get a node by ID using SafeKuzuManager."""
        try:
            query = f"MATCH (n:{node_type} {{id: $node_id}}) RETURN n"
            result = self.safe_manager.execute_query(query, {"node_id": node_id})
            result_list = _convert_query_result_to_list(result) if result else []
            return result_list[0]['n'] if result_list else None
        except Exception as e:
            logger.error(f"Failed to get {node_type} node: {e}")
            return None
    
    def update_node(self, node_type: str, node_id: str, updates: Dict[str, Any]) -> bool:
        """Update a node using SafeKuzuManager."""
        try:
            set_clauses = []
            params = {"node_id": node_id}
            
            for key, value in updates.items():
                set_clauses.append(f"n.{key} = ${key}")
                params[key] = value
            
            query = f"""
            MATCH (n:{node_type} {{id: $node_id}})
            SET {', '.join(set_clauses)}
            RETURN n
            """
            
            result = self.safe_manager.execute_query(query, params)
            return result is not None
        except Exception as e:
            logger.error(f"Failed to update {node_type} node: {e}")
            return False
    
    def delete_node(self, node_type: str, node_id: str) -> bool:
        """Delete a node using SafeKuzuManager."""
        try:
            query = f"MATCH (n:{node_type} {{id: $node_id}}) DETACH DELETE n"
            result = self.safe_manager.execute_query(query, {"node_id": node_id})
            return True  # KuzuDB doesn't return explicit success for DELETE
        except Exception as e:
            logger.error(f"Failed to delete {node_type} node: {e}")
            return False

# Helper function for query result conversion
def _convert_query_result_to_list(result) -> list:
    """Convert KuzuDB query result to list of dictionaries."""
    if not result:
        return []
    
    data = []
    while result.has_next():
        row = result.get_next()
        record = {}
        for i in range(len(row)):
            column_name = result.get_column_names()[i]
            record[column_name] = row[i]
        data.append(record)
    
    return data


class KuzuUserRepository:
    """Clean user repository using simplified Kuzu schema."""
    
    def __init__(self):
        # Lazy initialization - don't connect during startup
        self._safe_manager = None
    
    @property
    def safe_manager(self):
        """Lazy SafeKuzuManager connection - only connect when needed."""
        if self._safe_manager is None:
            self._safe_manager = get_safe_kuzu_manager()
        return self._safe_manager
    
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
                'password_must_change': getattr(user, 'password_must_change', False),
                'created_at_str': getattr(user, 'created_at', datetime.now(timezone.utc)).isoformat() if hasattr(getattr(user, 'created_at', datetime.now(timezone.utc)), 'isoformat') else datetime.now(timezone.utc).isoformat()
            }
            
            # Create user using SafeKuzuManager with direct Cypher query
            create_query = """
            CREATE (u:User {
                id: $id,
                username: $username,
                email: $email,
                password_hash: $password_hash,
                display_name: $display_name,
                bio: $bio,
                timezone: $timezone,
                is_admin: $is_admin,
                is_active: $is_active,
                password_must_change: $password_must_change,
                created_at: CASE WHEN $created_at_str IS NULL OR $created_at_str = '' THEN NULL ELSE timestamp($created_at_str) END
            })
            RETURN u.id as id
            """
            
            result = self.safe_manager.execute_query(create_query, user_data)
            result_data = _convert_query_result_to_list(result)
            
            if result_data:
                logger.info(f"✅ Created user: {getattr(user, 'username', 'unknown')} (ID: {getattr(user, 'id', 'unknown')})")
                return user
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to create user: {e}")
            return None
    
    async def get_by_id(self, user_id: str) -> Optional[Any]:
        """Get a user by ID."""
        try:
            query = "MATCH (u:User {id: $user_id}) RETURN u"
            result = self.safe_manager.execute_query(query, {"user_id": user_id})
            result_data = _convert_query_result_to_list(result)
            
            if result_data:
                return result_data[0]['u']
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get user {user_id}: {e}")
            return None
    
    async def get_by_username(self, username: str) -> Optional[Any]:
        """Get a user by username."""
        try:
            query = "MATCH (u:User {username: $username}) RETURN u"
            result = self.safe_manager.execute_query(query, {"username": username})
            result_data = _convert_query_result_to_list(result)
            
            if result_data:
                user_data = result_data[0]['u']
                logger.debug(f"Found user by username: {username}")
                return dict(user_data) if hasattr(user_data, '__dict__') else user_data
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get user by username {username}: {e}")
            return None
    
    async def get_by_email(self, email: str) -> Optional[Any]:
        """Get a user by email."""
        try:
            query = "MATCH (u:User {email: $email}) RETURN u"
            result = self.safe_manager.execute_query(query, {"email": email})
            result_data = _convert_query_result_to_list(result)
            
            if result_data:
                user_data = result_data[0]['u']
                logger.debug(f"Found user by email: {email}")
                return dict(user_data) if hasattr(user_data, '__dict__') else user_data
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get user by email {email}: {e}")
            return None
    
    async def get_all(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Get all users with pagination."""
        try:
            query = f"MATCH (u:User) RETURN u SKIP {offset} LIMIT {limit}"
            result = self.safe_manager.execute_query(query)
            result_data = _convert_query_result_to_list(result)
            
            users = []
            for row in result_data:
                user_data = row['u']
                users.append(dict(user_data) if hasattr(user_data, '__dict__') else user_data)
            
            return users
            
        except Exception as e:
            logger.error(f"❌ Failed to get users: {e}")
            return []

    async def update(self, user_id: str, updates: Dict[str, Any]) -> Optional[Any]:
        """Update an existing user with the provided fields.
        
        Note: This method does not enforce authorization checks. Authorization
        must be handled by the service layer before calling this method.
        """
        try:
            # Validate user_id parameter
            if not user_id or not isinstance(user_id, str):
                logger.error("❌ Invalid user_id provided for update")
                return None
            
            # First check if user exists
            existing_user = await self.get_by_id(user_id)
            if not existing_user:
                logger.error(f"❌ User {user_id} not found for update")
                return None
            
            # Build SET clause dynamically for provided fields
            # Note: is_admin and is_active are security-sensitive fields
            # Authorization should be enforced at the service/route layer
            allowed_fields = [
                'username', 'email', 'password_hash', 'display_name', 'bio', 
                'timezone', 'is_admin', 'is_active', 'password_must_change',
                'failed_login_attempts', 'share_current_reading', 'share_reading_activity',
                'share_library', 'reading_streak_offset'
            ]
            
            set_clauses = []
            params = {'user_id': user_id}
            
            for field in allowed_fields:
                if field in updates:
                    set_clauses.append(f"u.{field} = ${field}")
                    params[field] = updates[field]
            
            # Handle timestamp fields separately
            timestamp_fields = ['locked_until', 'last_login', 'password_changed_at', 'updated_at']
            for field in timestamp_fields:
                if field in updates:
                    value = updates[field]
                    if value is not None:
                        # Convert datetime to ISO string for Kuzu timestamp
                        if hasattr(value, 'isoformat'):
                            params[f'{field}_str'] = value.isoformat()
                        else:
                            params[f'{field}_str'] = value
                        set_clauses.append(f"u.{field} = timestamp(${field}_str)")
                    else:
                        set_clauses.append(f"u.{field} = NULL")
            
            if not set_clauses:
                logger.warning(f"No valid fields to update for user {user_id}")
                return existing_user
            
            # Build and execute update query
            update_query = f"""
            MATCH (u:User {{id: $user_id}})
            SET {', '.join(set_clauses)}
            RETURN u
            """
            
            result = self.safe_manager.execute_query(update_query, params)
            result_data = _convert_query_result_to_list(result)
            
            if result_data:
                logger.info(f"✅ Updated user: {user_id}")
                return result_data[0]['u']
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to update user {user_id}: {e}")
            return None

    async def delete(self, user_id: str) -> bool:
        """Delete a user by ID (DETACH DELETE). Returns True only if user existed."""
        try:
            # First check existence
            check_query = "MATCH (u:User {id: $user_id}) RETURN u.id as id"
            result = self.safe_manager.execute_query(check_query, {"user_id": user_id})
            data = _convert_query_result_to_list(result)
            if not data:
                logger.info(f"[USER_DELETE_DEBUG] Repo.delete user_id={user_id} NOT FOUND (nothing to delete)")
                return False
            del_query = "MATCH (u:User {id: $user_id}) DETACH DELETE u"
            self.safe_manager.execute_query(del_query, {"user_id": user_id})
            logger.info(f"[USER_DELETE_DEBUG] Repo.delete success user_id={user_id}")
            return True
        except Exception as e:
            logger.error(f"[USER_DELETE_DEBUG] Repo.delete exception user_id={user_id} error={e}")
            return False

    async def count_admins(self) -> int:
        """Return count of admin users."""
        try:
            query = "MATCH (u:User {is_admin: true}) RETURN COUNT(u) as c"
            result = self.safe_manager.execute_query(query)
            data = _convert_query_result_to_list(result)
            if data:
                return int(data[0].get('c', 0))
            return 0
        except Exception as e:
            logger.error(f"❌ Failed counting admin users: {e}")
            return 0


class KuzuPersonRepository:
    """Clean person repository using simplified Kuzu schema."""
    
    def __init__(self):
        # Lazy initialization - don't connect during startup
        self._safe_manager = None
    
    @property
    def safe_manager(self):
        """Lazy SafeKuzuManager connection - only connect when needed."""
        if self._safe_manager is None:
            self._safe_manager = get_safe_kuzu_manager()
        return self._safe_manager
    
    @property
    def db(self):
        """Backward compatibility: provide legacy db interface."""
        # DEPRECATED: Use safe_manager.get_connection() context manager instead
        # This property is only for backward compatibility during migration
        if not hasattr(self, '_db') or self._db is None:
            # Create a compatibility wrapper that provides the old interface
            self._db = KuzuRepositoryAdapter(self.safe_manager)
        return self._db
    
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
                created_at = person.get('created_at', datetime.now(timezone.utc))
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
                created_at = getattr(person, 'created_at', datetime.now(timezone.utc))
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
                'created_at_str': created_at.isoformat() if hasattr(created_at, 'isoformat') else datetime.now(timezone.utc).isoformat(),
                'updated_at_str': datetime.now(timezone.utc).isoformat()
            }
            
            success = self.safe_manager.execute_query("""
                CREATE (p:Person {
                    id: $id,
                    name: $name,
                    normalized_name: $normalized_name,
                    birth_year: $birth_year,
                    death_year: $death_year,
                    birth_place: $birth_place,
                    bio: $bio,
                    website: $website,
                    openlibrary_id: $openlibrary_id,
                    image_url: $image_url,
                    created_at: CASE WHEN $created_at_str IS NULL OR $created_at_str = '' THEN NULL ELSE timestamp($created_at_str) END,
                    updated_at: CASE WHEN $updated_at_str IS NULL OR $updated_at_str = '' THEN NULL ELSE timestamp($updated_at_str) END
                })
                RETURN p.id as id
            """, person_data)
            
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
        # Lazy initialization - don't connect during startup
        self._safe_manager = None
    
    @property
    def safe_manager(self):
        """Lazy SafeKuzuManager connection - only connect when needed."""
        if self._safe_manager is None:
            self._safe_manager = get_safe_kuzu_manager()
        return self._safe_manager
    
    @property
    def db(self):
        """Backward compatibility: provide legacy db interface."""
        # DEPRECATED: Use safe_manager.get_connection() context manager instead
        # This property is only for backward compatibility during migration
        if not hasattr(self, '_db') or self._db is None:
            # Create a compatibility wrapper that provides the old interface
            self._db = KuzuRepositoryAdapter(self.safe_manager)
        return self._db
    
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
                # Track ownership quantity (default 1)
                'quantity': getattr(book, 'quantity', 1) or 1,
                # Newly ensured fields that were previously not persisted
                'subtitle': getattr(book, 'subtitle', None),
                'asin': getattr(book, 'asin', None),
                'description': getattr(book, 'description', ''),
                'published_date': getattr(book, 'published_date', None),
                'page_count': getattr(book, 'page_count', 0),
                'language': getattr(book, 'language', 'en'),
                # Store NULL (None) rather than empty string when no cover yet
                'cover_url': (getattr(book, 'cover_url', None) or None),
                'google_books_id': getattr(book, 'google_books_id', None),
                'openlibrary_id': getattr(book, 'openlibrary_id', None),
                'average_rating': getattr(book, 'average_rating', 0.0),
                'rating_count': getattr(book, 'rating_count', 0),
                'media_type': getattr(book, 'media_type', None),
                'series': series_value,
                'series_volume': getattr(book, 'series_volume', None),
                'series_order': getattr(book, 'series_order', None),
                'custom_metadata': getattr(book, 'custom_metadata', None),
                # Use *_str variants so adapter casts to TIMESTAMP via timestamp($param)
                'created_at_str': getattr(book, 'created_at', datetime.now(timezone.utc)).isoformat() if hasattr(getattr(book, 'created_at', datetime.now(timezone.utc)), 'isoformat') else datetime.now(timezone.utc).isoformat(),
                'updated_at_str': getattr(book, 'updated_at', datetime.now(timezone.utc)).isoformat() if hasattr(getattr(book, 'updated_at', datetime.now(timezone.utc)), 'isoformat') else datetime.now(timezone.utc).isoformat()
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
            auto_fetch_metadata = getattr(contribution, 'auto_fetch_metadata', True)
            person_id = await self._ensure_person_exists(person, auto_fetch=auto_fetch_metadata)
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
    
    async def _ensure_person_exists(self, person: Any, *, auto_fetch: bool = True) -> Optional[str]:
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
            if auto_fetch and not openlibrary_id and not bio and not birth_year and not image_url:
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
                'created_at_str': getattr(person, 'created_at', datetime.now(timezone.utc)).isoformat() if hasattr(getattr(person, 'created_at', datetime.now(timezone.utc)), 'isoformat') else datetime.now(timezone.utc).isoformat(),
                'updated_at_str': datetime.now(timezone.utc).isoformat()
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
                'created_at_str': person_data['created_at_str']
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
                # Detect hierarchical category paths like "Fiction / Science Fiction / Space Opera"
                if ('/' in category_name) or ('>' in category_name):
                    try:
                        # Split on common separators and normalize whitespace
                        import re
                        parts = [p.strip() for p in re.split(r"[>/]", category_name) if p.strip()]
                        if not parts:
                            continue
                        leaf_category_id = await self._ensure_category_path_exists(parts)
                        if leaf_category_id:
                            # Link the book to the most specific (leaf) category
                            success = self.db.create_relationship(
                                'Book', book_id, 'CATEGORIZED_AS', 'Category', leaf_category_id, {}
                            )
                            if success:
                                logger.info(f"✅ Linked book {book_id} to leaf category path '{category_name}' (leaf id: {leaf_category_id})")
                            else:
                                logger.error(f"❌ Failed to link book {book_id} to leaf category id {leaf_category_id}")
                        else:
                            logger.warning(f"⚠️ Could not resolve category path for: {category_name}")
                    except Exception as e:
                        logger.error(f"❌ Failed processing hierarchical category '{category_name}': {e}")
                        # Fallback to flat handling
                        await self._create_category_relationship_by_name(book_id, category_name)
                else:
                    # Flat category name
                    await self._create_category_relationship_by_name(book_id, category_name)
                
        except Exception as e:
            logger.error(f"❌ Failed to create category relationships from raw data: {e}")

    async def _ensure_category_path_exists(self, parts: list[str]) -> Optional[str]:
        """Ensure a hierarchical category path exists; return the leaf category id.

        parts: e.g., ["Fiction", "Science Fiction", "Space Opera"]
        Creates (or finds) each Category with proper parent_id and PARENT_CATEGORY links.
        """
        try:
            parent_id: Optional[str] = None
            leaf_id: Optional[str] = None
            level = 0
            for name in parts:
                normalized_name = name.strip().lower()
                if not normalized_name:
                    continue
                # Find existing category matching name and parent context
                if parent_id is None:
                    query = (
                        "MATCH (c:Category) WHERE c.normalized_name = $normalized_name AND c.parent_id IS NULL "
                        "RETURN c.id LIMIT 1"
                    )
                    params = {"normalized_name": normalized_name}
                else:
                    query = (
                        "MATCH (c:Category) WHERE c.normalized_name = $normalized_name AND c.parent_id = $parent_id "
                        "RETURN c.id LIMIT 1"
                    )
                    params = {"normalized_name": normalized_name, "parent_id": parent_id}
                results = self.db.query(query, params)
                if results and (results[0].get('result') or results[0].get('col_0')):
                    category_id = results[0].get('result') or results[0]['col_0']
                else:
                    # Create new category node with parent link metadata
                    category_id = str(uuid.uuid4())
                    category_data = {
                        'id': category_id,
                        'name': name,
                        'normalized_name': normalized_name,
                        'description': '',
                        'parent_id': parent_id,
                        'level': level,
                        'color': '',
                        'icon': '',
                        'book_count': 0,
                        'user_book_count': 0,
                        'created_at_str': datetime.now(timezone.utc).isoformat(),
                        'updated_at_str': datetime.now(timezone.utc).isoformat()
                    }
                    created = self.db.create_node('Category', category_data)
                    if not created:
                        logger.error(f"❌ Failed to create category in path: {name} (level {level})")
                        return None
                    # If has parent, create explicit PARENT_CATEGORY relationship (parent -> child)
                    if parent_id:
                        self.db.create_relationship('Category', parent_id, 'PARENT_CATEGORY', 'Category', category_id, {})
                    logger.info(f"📁 Created category '{name}' (id={category_id}) under parent_id={parent_id}")
                # Advance to next level
                leaf_id = category_id
                parent_id = category_id
                level += 1
            return leaf_id
        except Exception as e:
            logger.error(f"❌ Failed ensuring category path exists for {parts}: {e}")
            return None
    
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
                'created_at_str': datetime.now(timezone.utc).isoformat(),
                'updated_at_str': datetime.now(timezone.utc).isoformat()
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
                'created_at_str': datetime.now(timezone.utc).isoformat()
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
            
            logger.info(f"🔍 [DEBUG] Executing get_all_persons query")
            results = self.db.query(query)
            logger.info(f"🔍 [DEBUG] Query returned {len(results)} results")
            
            persons = []
            for i, result in enumerate(results):
                logger.info(f"🔍 [DEBUG] Result {i}: {result}")
                
                # Handle both result formats for two column queries
                person_data = None
                book_count = 0
                
                if 'col_0' in result and 'col_1' in result:
                    person_data = dict(result['col_0'])
                    book_count = result['col_1'] or 0
                    logger.info(f"🔍 [DEBUG] Using col_0/col_1 format: {person_data['name'] if 'name' in person_data else 'unknown'} with {book_count} books")
                elif 'result' in result:
                    # Fallback for single column format
                    person_data = dict(result['result'])
                    book_count = 0
                    logger.info(f"🔍 [DEBUG] Using result format: {person_data['name'] if 'name' in person_data else 'unknown'}")
                
                if person_data:
                    person_data['book_count'] = book_count
                    persons.append(person_data)
            
            logger.info(f"🔍 [DEBUG] Returning {len(persons)} persons")
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
            
            logger.info(f"🔍 [DEBUG] Executing get_all_categories query")
            results = self.db.query(query)
            logger.info(f"🔍 [DEBUG] Categories query returned {len(results)} results")
            
            categories = []
            for i, result in enumerate(results):
                logger.info(f"🔍 [DEBUG] Category result {i}: {result}")
                
                # Handle both result formats for single column queries
                if 'result' in result:
                    category_data = dict(result['result'])
                    logger.info(f"🔍 [DEBUG] Using result format: {category_data['name'] if 'name' in category_data else 'unknown'}")
                    categories.append(category_data)
                elif 'col_0' in result:
                    category_data = dict(result['col_0'])
                    logger.info(f"🔍 [DEBUG] Using col_0 format: {category_data['name'] if 'name' in category_data else 'unknown'}")
                    categories.append(category_data)
            
            logger.info(f"🔍 [DEBUG] Returning {len(categories)} categories")
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
        # Lazy initialization - don't connect during startup
        self._safe_manager = None

    @property
    def safe_manager(self):
        """Lazy SafeKuzuManager connection - only connect when needed."""
        if self._safe_manager is None:
            self._safe_manager = get_safe_kuzu_manager()
        return self._safe_manager
    
    @property
    def db(self):
        """Backward compatibility: provide legacy db interface."""
        # DEPRECATED: Use safe_manager.get_connection() context manager instead
        # This property is only for backward compatibility during migration
        if not hasattr(self, '_db') or self._db is None:
            # Create a compatibility wrapper that provides the old interface
            self._db = KuzuRepositoryAdapter(self.safe_manager)
        return self._db
    
    async def add_book_to_library(self, user_id: str, book_id: str, 
                                 reading_status: str = "plan_to_read",
                                 ownership_status: str = "owned",
                                 media_type: str = "physical",
                                 notes: str = "",
                                 location_id: Optional[str] = None) -> bool:
        """Add a book to user's library (universal library mode -> create personal metadata overlay).

        In universal mode we no longer create OWNS relationships; instead we ensure a
        HAS_PERSONAL_METADATA relationship exists and store initial fields in its JSON blob.
        """
        try:
            from app.services.personal_metadata_service import personal_metadata_service
            custom_updates = {
                'reading_status': str(reading_status),
                'ownership_status': str(ownership_status),
                'media_type': str(media_type),
                'source': 'manual'
            }
            if location_id:
                # Location is now handled via STORED_AT edges; retain for backward compatibility in metadata
                custom_updates['legacy_location_id'] = str(location_id)
            personal_metadata_service.update_personal_metadata(
                user_id,
                book_id,
                personal_notes=notes or None,
                custom_updates=custom_updates,
                merge=True,
            )
            logger.info(f"✅ Added (personal overlay) book {book_id} for user {user_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to add book to library (personal metadata path): {e}")
            return False
    
    async def get_user_books(self, user_id: str, reading_status: Optional[str] = None, 
                           limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Get books for user based on personal metadata (OWNS deprecated).

        Strategy: pull all books and overlay personal metadata in Python, then filter.
        Acceptable for moderate dataset sizes; optimize later by extending HAS_PERSONAL_METADATA schema.
        """
        try:
            from app.services.personal_metadata_service import personal_metadata_service
            # Fetch all books (limit applied post-filter)
            query = """
            MATCH (b:Book)
            RETURN b
            SKIP $offset LIMIT $limit
            """
            results = self.db.query(query, {"offset": offset, "limit": limit})
            books: List[Dict[str, Any]] = []
            for result in results:
                if 'col_0' not in result:
                    continue
                book_node = dict(result['col_0'])
                pm = personal_metadata_service.get_personal_metadata(user_id, book_node.get('id', ''))
                if reading_status and pm.get('reading_status') != reading_status:
                    continue
                books.append({'book': book_node, 'personal': pm})
            return books
        except Exception as e:
            logger.error(f"❌ Failed to get user books (personal metadata path): {e}")
            return []
    
    async def update_reading_status(self, user_id: str, book_id: str, new_status: str) -> bool:
        """Update reading status via personal metadata service (OWNS deprecated)."""
        try:
            from app.services.personal_metadata_service import personal_metadata_service
            personal_metadata_service.update_personal_metadata(user_id, book_id, custom_updates={'reading_status': new_status})
            return True
        except Exception as e:
            logger.error(f"❌ Failed to update reading status (personal metadata): {e}")
            return False

    async def create_ownership(self, user_id: str, book_id: str, 
                             reading_status, ownership_status, media_type,
                             location_id: Optional[str] = None, source: str = "manual",
                             notes: str = "", date_added = None, 
                             custom_metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Deprecated: create ownership now maps to creating personal metadata overlay.

        Kept for backward compatibility. Ignores date_added (book global creation time used) and
        stores all fields in HAS_PERSONAL_METADATA JSON blob.
        """
        try:
            from app.services.personal_metadata_service import personal_metadata_service
            # Accept enums or raw values
            reading_status_str = getattr(reading_status, 'value', str(reading_status))
            ownership_status_str = getattr(ownership_status, 'value', str(ownership_status))
            media_type_str = getattr(media_type, 'value', str(media_type))
            custom_updates = {
                'reading_status': reading_status_str,
                'ownership_status': ownership_status_str,
                'media_type': media_type_str,
                'source': source or 'manual'
            }
            if location_id:
                custom_updates['legacy_location_id'] = str(location_id)
            if custom_metadata and isinstance(custom_metadata, dict):
                for k, v in custom_metadata.items():
                    custom_updates[k] = v
            personal_metadata_service.update_personal_metadata(
                user_id,
                book_id,
                personal_notes=notes or None,
                custom_updates=custom_updates,
                merge=True,
            )
            logger.info(f"✅ (Deprecated OWNS) stored personal metadata for user {user_id} book {book_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed legacy create_ownership (personal path): {e}")
            return False
    
    async def remove_ownership(self, user_id: str, book_id: str) -> bool:
        """Remove personal metadata relationship (legacy ownership removal)."""
        try:
            query = """
            MATCH (u:User {id: $user_id})-[r:HAS_PERSONAL_METADATA]->(b:Book {id: $book_id})
            DELETE r
            RETURN 1 as deleted
            """
            result = self.db.query(query, {"user_id": user_id, "book_id": book_id})
            if result:
                logger.info(f"✅ Removed personal metadata (legacy ownership) for user {user_id} book {book_id}")
                return True
            logger.warning(f"⚠️ No personal metadata to remove for user {user_id} book {book_id}")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to remove personal metadata (legacy ownership): {e}")
            return False
    
    async def get_user_statistics(self, user_id: str) -> Dict[str, Any]:
        """Get user statistics based on HAS_PERSONAL_METADATA (OWNS deprecated).

        Aggregates counts from the personal metadata JSON blob.
        """
        try:
            from app.services.personal_metadata_service import personal_metadata_service
            stats: Dict[str, Any] = {
                'total_books': 0,
                'plan_to_read': 0,
                'currently_reading': 0,
                'completed': 0,
                'on_hold': 0,
                'dropped': 0,
                'owned': 0,
                'wishlist': 0,
                'borrowed': 0,
                'loaned': 0,
                'physical': 0,
                'ebook': 0,
                'audiobook': 0,
            }
            # Query only books that have personal metadata for this user
            query = """
            MATCH (u:User {id: $user_id})-[r:HAS_PERSONAL_METADATA]->(b:Book)
            RETURN b.id, r.personal_custom_fields, r.personal_notes
            """
            results = self.db.query(query, {"user_id": user_id})
            if not results:
                return stats
            for row in results:
                # row may be dict with col_0 etc.
                if isinstance(row, dict):
                    book_id = row.get('col_0') or row.get('b.id') or row.get('book_id')
                    blob_raw = row.get('col_1') or row.get('personal_custom_fields')
                else:
                    # unsupported structure
                    continue
                if not book_id:
                    continue
                stats['total_books'] += 1
                meta = {}
                if blob_raw:
                    try:
                        import json
                        meta = json.loads(blob_raw) if isinstance(blob_raw, str) else {}
                    except Exception:
                        meta = {}
                # reading_status
                rs = meta.get('reading_status')
                if rs in stats:
                    stats[rs] += 1
                # ownership_status
                os_val = meta.get('ownership_status')
                if os_val in stats:
                    stats[os_val] += 1
                # media_type
                mt = meta.get('media_type')
                if mt in stats:
                    stats[mt] += 1
            return stats
        except Exception as e:
            logger.error(f"❌ Failed to get user statistics (personal metadata): {e}")
            return {}
    
    async def get_reading_timeline(self, user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Get user's reading timeline based on personal metadata updates."""
        try:
            query = """
            MATCH (u:User {id: $user_id})-[r:HAS_PERSONAL_METADATA]->(b:Book)
            RETURN b, r
            ORDER BY COALESCE(r.updated_at, b.created_at) DESC
            LIMIT $limit
            """
            results = self.db.query(query, {"user_id": user_id, "limit": limit})
            timeline: List[Dict[str, Any]] = []
            if not results:
                return timeline
            for row in results:
                if not isinstance(row, dict):
                    continue
                book_node = row.get('col_0') or row.get('b')
                rel_data = row.get('col_1') or row.get('r')
                if not book_node:
                    continue
                # Convert nodes/relationships (which may expose items()) into plain dicts
                def _to_plain(obj):
                    try:
                        if obj and hasattr(obj, 'items'):
                            return {str(k): v for k, v in obj.items()}  # type: ignore[attr-defined]
                    except Exception:
                        return {}
                    return {}
                book_data = _to_plain(book_node)
                rel_dict = _to_plain(rel_data)
                # Parse JSON blob (may be bytes)
                blob_raw = rel_dict.get('personal_custom_fields') if isinstance(rel_dict, dict) else None
                if isinstance(blob_raw, (bytes, bytearray, memoryview)):
                    try:
                        blob_raw = bytes(blob_raw).decode('utf-8', errors='ignore')
                    except Exception:
                        blob_raw = None
                meta = {}
                if blob_raw and isinstance(blob_raw, str):
                    try:
                        import json
                        meta = json.loads(blob_raw)
                    except Exception:
                        meta = {}
                timeline.append({
                    'book': book_data,
                    'activity_type': 'personal_update',
                    'activity_date': rel_dict.get('updated_at') if isinstance(rel_dict, dict) else None or book_data.get('created_at'),
                    'reading_status': meta.get('reading_status'),
                    'ownership_status': meta.get('ownership_status'),
                    'media_type': meta.get('media_type'),
                    'notes': (rel_dict.get('personal_notes') if isinstance(rel_dict, dict) else None) or meta.get('personal_notes') or ''
                })
            return timeline
        except Exception as e:
            logger.error(f"❌ Failed to get reading timeline (personal metadata): {e}")
            return []


class KuzuLocationRepository:
    """Clean location repository."""
    
    def __init__(self):
        # Lazy initialization - don't connect during startup
        self._safe_manager = None
        self._db = None

    @property
    def safe_manager(self):
        """Lazy SafeKuzuManager connection - only connect when needed."""
        if self._safe_manager is None:
            self._safe_manager = get_safe_kuzu_manager()
        return self._safe_manager
    
    @property
    def db(self):
        """Lazy database connection - only connect when needed."""
        if self._db is None:
            # Create a compatibility wrapper that provides the old interface
            self._db = KuzuRepositoryAdapter(self.safe_manager)
        return self._db
    
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
                'created_at': getattr(location, 'created_at', datetime.now(timezone.utc)).isoformat() if hasattr(getattr(location, 'created_at', datetime.now(timezone.utc)), 'isoformat') else datetime.now(timezone.utc).isoformat()
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
        # Lazy initialization - don't connect during startup
        self._safe_manager = None
        self._db = None

    @property
    def safe_manager(self):
        """Lazy SafeKuzuManager connection - only connect when needed."""
        if self._safe_manager is None:
            self._safe_manager = get_safe_kuzu_manager()
        return self._safe_manager
    
    @property
    def db(self):
        """Lazy database connection - only connect when needed."""
        if self._db is None:
            # Create a compatibility wrapper that provides the old interface
            self._db = KuzuRepositoryAdapter(self.safe_manager)
        return self._db
    
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
            # Use _str suffix for timestamps to enable proper conversion in create_node
            created_at = getattr(category, 'created_at', datetime.now(timezone.utc))
            updated_at = getattr(category, 'updated_at', datetime.now(timezone.utc))
            created_at_str = created_at.isoformat() if hasattr(created_at, 'isoformat') else datetime.now(timezone.utc).isoformat()
            updated_at_str = updated_at.isoformat() if hasattr(updated_at, 'isoformat') else datetime.now(timezone.utc).isoformat()
            
            # Convert aliases list to string (schema defines aliases as STRING)
            aliases_list = getattr(category, 'aliases', [])
            aliases_str = '\n'.join(aliases_list) if isinstance(aliases_list, list) else str(aliases_list or '')
            
            category_data = {
                'id': category_id,
                'name': category_name,
                'normalized_name': category_name.strip().lower(),
                'description': getattr(category, 'description', '') or '',
                'parent_id': parent_id,  # Include parent_id
                'level': getattr(category, 'level', 0),  # Default to root level
                'color': getattr(category, 'color', '') or '',
                'icon': getattr(category, 'icon', '') or '',
                'aliases': aliases_str,
                'book_count': getattr(category, 'book_count', 0),
                'user_book_count': getattr(category, 'user_book_count', 0),
                'created_at_str': created_at_str,
                'updated_at_str': updated_at_str
            }
            
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
                    created_at=getattr(category, 'created_at', datetime.now(timezone.utc)),
                    updated_at=getattr(category, 'updated_at', datetime.now(timezone.utc))
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
            
            # Convert updated_at to ISO string for timestamp() function
            updated_at_str = datetime.now(timezone.utc).isoformat()
            
            # Convert aliases list to string (schema defines aliases as STRING)
            aliases_list = getattr(category, 'aliases', [])
            aliases_str = '\n'.join(aliases_list) if isinstance(aliases_list, list) else str(aliases_list or '')
            
            # Prepare update data
            category_data = {
                'id': category_id,
                'name': getattr(category, 'name', ''),
                'normalized_name': getattr(category, 'name', '').strip().lower(),
                'description': getattr(category, 'description', '') or '',
                'parent_id': getattr(category, 'parent_id', None),
                'level': getattr(category, 'level', 0),
                'color': getattr(category, 'color', '') or '',
                'icon': getattr(category, 'icon', '') or '',
                'aliases': aliases_str,
                'updated_at_str': updated_at_str
            }
            
            # Update using Kuzu query with timestamp() function for proper conversion
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
                c.updated_at = timestamp($updated_at_str)
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
        # Lazy initialization - don't connect during startup
        self._safe_manager = None
        self._db = None

    @property
    def safe_manager(self):
        """Lazy SafeKuzuManager connection - only connect when needed."""
        if self._safe_manager is None:
            self._safe_manager = get_safe_kuzu_manager()
        return self._safe_manager
    
    @property
    def db(self):
        """Lazy database connection - only connect when needed."""
        if self._db is None:
            # Create a compatibility wrapper that provides the old interface
            self._db = KuzuRepositoryAdapter(self.safe_manager)
        return self._db
    
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
                # Shareable concept removed; retained field for backward compatibility default False
                'is_shareable': False,
                'is_global': getattr(field_def, 'is_global', False),
                'default_value': getattr(field_def, 'default_value', ''),
                'usage_count': getattr(field_def, 'usage_count', 0),
                'created_at': getattr(field_def, 'created_at', datetime.now(timezone.utc)).isoformat() if hasattr(getattr(field_def, 'created_at', datetime.now(timezone.utc)), 'isoformat') else datetime.now(timezone.utc).isoformat()
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
                'is_shareable': False,
                'updated_at': datetime.now(timezone.utc).isoformat()
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
            # Shareable filter removed; return all definitions
            query_conditions = ["1=1"]
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
                query_conditions.append("(cf.created_by_user_id = $user_id OR cf.is_global = true)")
                params['user_id'] = user_id
            else:
                query_conditions.append("cf.is_global = true")
            
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
            WHERE cf.is_global = true
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
        # Lazy initialization - don't connect during startup
        self._safe_manager = None
        self._db = None

    @property
    def safe_manager(self):
        """Lazy SafeKuzuManager connection - only connect when needed."""
        if self._safe_manager is None:
            self._safe_manager = get_safe_kuzu_manager()
        return self._safe_manager
    
    @property
    def db(self):
        """Lazy database connection - only connect when needed."""
        if self._db is None:
            # Create a compatibility wrapper that provides the old interface
            self._db = KuzuRepositoryAdapter(self.safe_manager)
        return self._db
    
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
                'created_at': getattr(template, 'created_at', datetime.now(timezone.utc)).isoformat() if hasattr(getattr(template, 'created_at', datetime.now(timezone.utc)), 'isoformat') else datetime.now(timezone.utc).isoformat()
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
                'updated_at': datetime.now(timezone.utc).isoformat()
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
                "updated_at": datetime.now(timezone.utc).isoformat()
            })
        except Exception as e:
            logger.warning(f"Could not increment usage count for template {template_id}: {e}")
