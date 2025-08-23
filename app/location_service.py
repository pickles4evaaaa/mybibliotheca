"""
Location management service for tracking book locations.

This service manages locations as completely independent entities from users.
Only books have relationships with locations via STORED_AT relationships.
Users access locations indirectly through their books.
The system uses proper graph relationships, not properties on nodes.
"""

from typing import List, Optional, Dict, Any, Set
import uuid
from datetime import datetime, timezone
import json
from dataclasses import asdict
import os as _os_for_import_verbosity

from .domain.models import Location
from .debug_system import debug_log, get_debug_manager
from .infrastructure.kuzu_graph import safe_execute_kuzu_query

# Quiet logging by default; enable with VERBOSE=true or IMPORT_VERBOSE=true
_IMPORT_VERBOSE = (
    (_os_for_import_verbosity.getenv('VERBOSE') or 'false').lower() == 'true'
    or (_os_for_import_verbosity.getenv('IMPORT_VERBOSE') or 'false').lower() == 'true'
)

def _dprint(*args, **kwargs):
    if _IMPORT_VERBOSE:
        __builtins__.print(*args, **kwargs)

# Redirect module print to conditional debug print
print = _dprint


def _convert_query_result_to_list(result) -> List[Dict[str, Any]]:
    """Convert Kuzu QueryResult to a list of simple dict rows."""
    if result is None:
        return []
    rows: List[Dict[str, Any]] = []
    try:
        if hasattr(result, 'has_next') and hasattr(result, 'get_next'):
            while result.has_next():
                row = result.get_next()
                if len(row) == 1:
                    rows.append({'result': row[0]})
                else:
                    row_dict: Dict[str, Any] = {}
                    for i, value in enumerate(row):
                        row_dict[f'col_{i}'] = value
                    rows.append(row_dict)
    except Exception as e:
        _dprint(f"[LOCATION_SERVICE] Error converting result: {e}")
    return rows

def _extract_single_value(result, default: Any = 0) -> Any:
    rows = _convert_query_result_to_list(result)
    if not rows:
        return default
    row0 = rows[0]
    if 'result' in row0:
        return row0['result']
    # fallback
    return row0.get('col_0', default)


def _decode_value(v: Any) -> Any:
    """Decode bytes-like values to Python types and parse common scalars.
    - bytes/bytearray/memoryview -> UTF-8 string
    - 'true'/'false' strings -> bool
    - ISO timestamp strings for created_at/updated_at handled separately
    """
    try:
        if isinstance(v, (bytes, bytearray, memoryview)):
            v = bytes(v).decode('utf-8', errors='ignore')
        # Normalize simple booleans from strings
        if isinstance(v, str):
            low = v.strip().lower()
            if low in ('true', 'false'):
                return low == 'true'
        return v
    except Exception:
        return v


def _ensure_datetime(v: Any) -> Optional[datetime]:
    """Ensure value is a datetime. Accepts datetime, ISO string, or bytes-like."""
    try:
        if v is None:
            return None
        if isinstance(v, datetime):
            return v
        if isinstance(v, (bytes, bytearray, memoryview)):
            v = bytes(v).decode('utf-8', errors='ignore')
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v)
            except Exception:
                return None
        return None
    except Exception:
        return None


def _node_to_dict(node: Any) -> Dict[str, Any]:
    """Convert Kuzu node object to a plain dict with str keys and decoded values."""
    data: Dict[str, Any] = {}
    if node is None:
        return data
    # Try items() first (mapping-like)
    try:
        items = node.items()  # type: ignore[attr-defined]
    except Exception:
        items = None
    if items is not None:
        for k, v in items:  # type: ignore[assignment]
            if isinstance(k, memoryview):
                key = bytes(k).decode('utf-8', errors='ignore')
            elif isinstance(k, (bytes, bytearray)):
                key = k.decode('utf-8', errors='ignore')
            else:
                key = str(k)
            val = _decode_value(v)
            data[key] = val
        # Post-process timestamps
        if 'created_at' in data:
            parsed = _ensure_datetime(data['created_at'])
            if parsed:
                data['created_at'] = parsed
        if 'updated_at' in data:
            parsed = _ensure_datetime(data['updated_at'])
            if parsed:
                data['updated_at'] = parsed
        return data
    # Fallback: try sequence of 2-tuples
    try:
        for pair in list(node):  # type: ignore[call-arg]
            if isinstance(pair, (list, tuple)) and len(pair) == 2:
                k, v = pair
                if isinstance(k, memoryview):
                    key = bytes(k).decode('utf-8', errors='ignore')
                elif isinstance(k, (bytes, bytearray)):
                    key = k.decode('utf-8', errors='ignore')
                else:
                    key = str(k)
                val = _decode_value(v)
                data[key] = val
    except Exception:
        pass
    # Post-process timestamps
    if 'created_at' in data:
        parsed = _ensure_datetime(data['created_at'])
        if parsed:
            data['created_at'] = parsed
    if 'updated_at' in data:
        parsed = _ensure_datetime(data['updated_at'])
        if parsed:
            data['updated_at'] = parsed
    return data


class LocationService:
    """Service for managing user locations and book-location relationships."""
    
    def __init__(self, kuzu_db_path: Optional[str] = None):
        # Use global safe connection management instead of separate instance
        self.debug_manager = get_debug_manager()
        debug_log(f"LocationService initialized with global connection management", "LOCATION")
    
    def create_location(self, name: str, description: Optional[str] = None, 
                       location_type: str = "home", address: Optional[str] = None,
                       is_default: bool = False) -> Location:
        """Create a new location. Locations are independent of users."""
        debug_log(f"Creating location: name='{name}', type='{location_type}', is_default={is_default}", 
                 "LOCATION", {"location_name": name, "location_type": location_type})
        
        location_id = str(uuid.uuid4())
        
        # If this is explicitly set as default, clear other defaults
        if is_default:
            debug_log(f"Clearing other default locations", "LOCATION")
            clear_defaults_query = """
            MATCH (l:Location) 
            SET l.is_default = false
            """
            safe_execute_kuzu_query(clear_defaults_query, {}, operation="clear_default_locations")
            debug_log(f"Setting as default location", "LOCATION")
            
        location = Location(
            id=location_id,
            user_id="",  # Locations are not owned by users
            name=name,
            description=description,
            location_type=location_type,
            address=address,
            is_default=is_default,
            is_active=True,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
        )
        
        # Store in KuzuDB without any user association
        create_query = """
        CREATE (l:Location {
            id: $id,
            name: $name,
            description: $description,
            location_type: $location_type,
            address: $address,
            is_default: $is_default,
            is_active: $is_active,
            created_at: $created_at,
            updated_at: $updated_at
        })
        """
        
        location_data = {
            "id": location.id,
            "name": location.name,
            "description": location.description,
            "location_type": location.location_type,
            "address": location.address,
            "is_default": location.is_default,
            "is_active": location.is_active,
            "created_at": location.created_at,
            "updated_at": location.updated_at
        }
        
        # Create the location node (completely independent of users)
        safe_execute_kuzu_query(create_query, location_data, operation="location_operation")
        
        return location
    
    def get_location(self, location_id: str) -> Optional[Location]:
        """Get a specific location by ID (universal access)."""
        print(f"DEBUG: get_location called with location_id: {location_id}")
        
        query = """
        MATCH (l:Location {id: $location_id})
        RETURN l
        LIMIT 1
        """
        
        print(f"DEBUG: Executing query: {query}")
        results = safe_execute_kuzu_query(query, {"location_id": location_id}, operation="location_operation")
        rows = _convert_query_result_to_list(results)
        if not rows:
            print("DEBUG: No results found, returning None")
            return None
        location_node = rows[0].get('result') or rows[0].get('col_0')
        location_data = _node_to_dict(location_node)
        if not location_data:
            return None
        
        print("DEBUG: Creating Location object...")
        location = Location(
            id=location_data['id'],
            name=location_data['name'],
            description=location_data.get('description') or '',
            location_type=location_data.get('location_type') or 'general',
            address=location_data.get('address') or '',
            is_default=location_data.get('is_default', False),
            is_active=location_data.get('is_active', True),
            created_at=_ensure_datetime(location_data.get('created_at')) or datetime.now(timezone.utc),
            updated_at=_ensure_datetime(location_data.get('updated_at')) or _ensure_datetime(location_data.get('created_at')) or datetime.now(timezone.utc)
        )
        print(f"DEBUG: Created location: {location}")
        return location
    
    def get_user_locations(self, user_id: str, active_only: bool = True) -> List[Location]:
        """Get all locations that contain books (via STORED_AT relationships)."""
        print(f"🏠 [GET_USER_LOCATIONS] Fetching locations with books (active_only: {active_only})")
        
        # Query locations that have books stored (universal)
        query = """
        MATCH (b:Book)-[stored:STORED_AT]->(l:Location)
        """
        if active_only:
            query += " WHERE l.is_active = true"
        query += " RETURN DISTINCT l ORDER BY l.is_default DESC, l.name ASC"
        
        result = safe_execute_kuzu_query(query, {}, operation="location_operation")
        
        locations: List[Location] = []
        rows = _convert_query_result_to_list(result)
        for r in rows:
            node = r.get('result') or r.get('col_0')
            location_data = _node_to_dict(node)
            if not location_data:
                continue
            location = Location(
                id=location_data['id'],
                user_id="",  # Locations don't belong to users
                name=location_data['name'],
                description=location_data.get('description'),
                location_type=location_data['location_type'],
                address=location_data.get('address'),
                is_default=location_data['is_default'],
                is_active=location_data.get('is_active', True),
                created_at=_ensure_datetime(location_data.get('created_at')) or datetime.now(timezone.utc),
                updated_at=_ensure_datetime(location_data.get('updated_at')) or _ensure_datetime(location_data.get('created_at')) or datetime.now(timezone.utc)
            )
            locations.append(location)
        
        return locations
    
    def get_all_locations(self, active_only: bool = True) -> List[Location]:
        """Get all locations in the system, regardless of whether they have books."""
        
        # Query all locations in the system
        query = "MATCH (l:Location)"
        if active_only:
            query += " WHERE l.is_active = true"
        query += " RETURN l ORDER BY l.is_default DESC, l.name ASC"
        
        result = safe_execute_kuzu_query(query, {}, operation="location_operation")
        
        locations: List[Location] = []
        rows = _convert_query_result_to_list(result)
        for r in rows:
            node = r.get('result') or r.get('col_0')
            location_data = _node_to_dict(node)
            if not location_data:
                continue
            location = Location(
                id=location_data['id'],
                user_id="",  # Locations don't belong to users
                name=location_data['name'],
                description=location_data.get('description'),
                location_type=location_data['location_type'],
                address=location_data.get('address'),
                is_default=location_data['is_default'],
                is_active=location_data.get('is_active', True),
                created_at=_ensure_datetime(location_data.get('created_at')) or datetime.now(timezone.utc),
                updated_at=_ensure_datetime(location_data.get('updated_at')) or _ensure_datetime(location_data.get('created_at')) or datetime.now(timezone.utc)
            )
            locations.append(location)
        
        return locations
    
    def update_location(self, location_id: str, **updates) -> Optional[Location]:
        """Update a location."""
        print(f"🏠 [UPDATE_LOCATION] Updating location {location_id} with: {updates}")
        
        location = self.get_location(location_id)
        if not location:
            print(f"🏠 [UPDATE_LOCATION] Location {location_id} not found")
            return None
            
        # Handle setting as default
        if updates.get('is_default') and not location.is_default:
            print(f"🏠 [UPDATE_LOCATION] Setting location {location_id} as default, clearing others")
            # Clear other defaults in the system
            clear_defaults_query = """
            MATCH (l:Location) 
            WHERE l.id <> $location_id 
            SET l.is_default = false
            """
            safe_execute_kuzu_query(clear_defaults_query, {"location_id": location_id}, operation="location_operation")
        
        # Build update query
        set_clauses = []
        params: Dict[str, Any] = {"location_id": location_id}
        
        for key, value in updates.items():
            if hasattr(location, key):
                set_clauses.append(f"l.{key} = ${key}")
                params[key] = value
                print(f"🏠 [UPDATE_LOCATION] Will update {key}: -> '{value}'")
        
        if set_clauses:
            set_clauses.append("l.updated_at = $updated_at")
            # KuzuDB requires datetime objects for TIMESTAMP fields, not strings
            params["updated_at"] = datetime.now(timezone.utc)
            
            update_query = f"""
            MATCH (l:Location) 
            WHERE l.id = $location_id 
            SET {', '.join(set_clauses)}
            """
            
            # Execute with proper typing for KuzuDB
            safe_execute_kuzu_query(update_query, params, operation="location_operation")
        # Return updated location
        return self.get_location(location_id)
    
    def delete_location(self, location_id: str) -> bool:
        """Delete a location."""
        print(f"🏠 [DELETE_LOCATION] Deleting location {location_id}")
        
        location = self.get_location(location_id)
        if not location:
            print(f"🏠 [DELETE_LOCATION] Location {location_id} not found")
            return False
        
        # Delete from KuzuDB
        delete_query = "MATCH (l:Location) WHERE l.id = $location_id DELETE l"
        safe_execute_kuzu_query(delete_query, {"location_id": location_id}, operation="location_operation")
        
        print(f"🏠 [DELETE_LOCATION] Deleted location {location_id}: '{location.name}'")
        return True
    
    def get_default_location(self) -> Optional[Location]:
        """Get the system default location (universal access)."""
        
        # Simply find the default location in the system (locations are universal)
        default_query = """
        MATCH (l:Location)
        WHERE l.is_default = true
        RETURN l
        LIMIT 1
        """
        result = safe_execute_kuzu_query(default_query, {}, operation="location_operation")
        rows = _convert_query_result_to_list(result)
        if rows:
            node = rows[0].get('result') or rows[0].get('col_0')
            location_data = _node_to_dict(node)
            if not location_data:
                return None
            location = Location(
                id=location_data['id'],
                user_id="",
                name=location_data['name'],
                description=location_data.get('description'),
                location_type=location_data['location_type'],
                address=location_data.get('address'),
                is_default=location_data['is_default'],
                is_active=location_data.get('is_active', True),
                created_at=_ensure_datetime(location_data.get('created_at')) or datetime.now(timezone.utc),
                updated_at=_ensure_datetime(location_data.get('updated_at')) or _ensure_datetime(location_data.get('created_at')) or datetime.now(timezone.utc)
            )
            return location
        
        # No default found, use first available        # If no default location exists, get any available location as fallback
        all_locations_query = """
        MATCH (l:Location)
        WHERE l.is_active = true
        RETURN l
        LIMIT 1
        """
        result = safe_execute_kuzu_query(all_locations_query, {}, operation="location_operation")
        rows = _convert_query_result_to_list(result)
        if rows:
            node = rows[0].get('result') or rows[0].get('col_0')
            location_data = _node_to_dict(node)
            if not location_data:
                return None
            location = Location(
                id=location_data['id'],
                user_id="",
                name=location_data['name'],
                description=location_data.get('description'),
                location_type=location_data['location_type'],
                address=location_data.get('address'),
                is_default=location_data['is_default'],
                is_active=location_data.get('is_active', True),
                created_at=_ensure_datetime(location_data.get('created_at')) or datetime.now(timezone.utc),
                updated_at=_ensure_datetime(location_data.get('updated_at')) or _ensure_datetime(location_data.get('created_at')) or datetime.now(timezone.utc)
            )
            print(f"🏠 [GET_DEFAULT] No default found, using first available location: '{location.name}' (ID: {location.id})")
            return location
        
        print(f"🏠 [GET_DEFAULT] No locations found at all")
        return None
    
    def setup_default_locations(self) -> List[Location]:
        """Set up default locations for the system."""
        print(f"🏠 [SETUP_DEFAULT] Setting up default locations")
        
        locations = []
        
        # Create default home location
        home = self.create_location(
            name="Home",
            description="Primary residence",
            location_type="home",
            is_default=True
        )
        locations.append(home)
        
        print(f"🏠 [SETUP_DEFAULT] Created {len(locations)} default locations")
        return locations
    
    def get_location_book_count(self, location_id: str, user_id: Optional[str] = None) -> int:
        """Get the number of books at a specific location.
        
        Args:
            location_id: The location to count books for
            user_id: Optional user filter. If provided, only count books for this user.
                    If None, count all books at the location.
        """
        debug_log(f"Getting book count for location {location_id}, user filter: {user_id}", "LOCATION", 
                 {"location_id": location_id, "user_id": user_id})
        
        try:
            if user_id:
                # Count books stored at this location (universal)
                query = """
                MATCH (b:Book)-[stored:STORED_AT]->(l:Location {id: $location_id})
                RETURN COUNT(b) as book_count
                """
                params = {'location_id': location_id}
            else:
                # Count all books at this location regardless of user
                query = """
                MATCH (b:Book)-[:STORED_AT]->(l:Location {id: $location_id})
                RETURN COUNT(b) as book_count
                """
                params = {'location_id': location_id}
            
            result = safe_execute_kuzu_query(query, params, operation="location_operation")
            val = _extract_single_value(result, 0)
            count = int(val) if val is not None else 0
            
            debug_log(f"Location {location_id} has {count} books (user filter: {user_id})", "LOCATION", 
                     {"location_id": location_id, "user_id": user_id, "book_count": count})
            return count
            
        except Exception as e:
            debug_log(f"Error getting book count for location {location_id}: {e}", "ERROR", 
                     {"location_id": location_id, "user_id": user_id, "error": str(e)})
            import traceback
            traceback.print_exc()
            return 0
    
    def get_all_location_book_counts(self, user_id: Optional[str] = None) -> Dict[str, int]:
        """Get book counts for all locations.
        
        Args:
            user_id: Optional user filter. If provided, only count books for this user.
                    If None, count all books at each location.
        """
        debug_log(f"Getting book counts for all locations (user filter: {user_id})", "LOCATION", {"user_id": user_id})
        
        # Get all locations first - use the new method to get all locations
        locations = self.get_all_locations()
        
        counts = {}
        for location in locations:
            if location.id:
                counts[location.id] = self.get_location_book_count(location.id, user_id)
        
        debug_log(f"Location book counts: {counts}", "LOCATION", {"user_id": user_id, "counts": counts})
        return counts
    
    def get_books_at_location(self, location_id: str, user_id: Optional[str] = None) -> List[str]:
        """Get list of book IDs at a specific location.
        
        Args:
            location_id: The location to get books for
            user_id: Optional user filter. If provided, only get books for this user.
                    If None, get all books at the location.
        """
        debug_log(f"Getting books at location {location_id} (user filter: {user_id})", "LOCATION", 
                 {"location_id": location_id, "user_id": user_id})
        
        try:
            # Get books stored at this location (universal)
            query = """
            MATCH (b:Book)-[stored:STORED_AT]->(l:Location {id: $location_id})
            RETURN b.id as book_id
            """
            params = {'location_id': location_id}
            
            result = safe_execute_kuzu_query(query, params, operation="location_operation")
            book_ids: List[str] = []
            rows = _convert_query_result_to_list(result)
            for r in rows:
                book_id = r.get('result') or r.get('col_0')
                if book_id:
                    book_ids.append(book_id)
                    debug_log(f"Found book {book_id} at location", "LOCATION", 
                             {"book_id": book_id, "location_id": location_id})
            
            debug_log(f"Found {len(book_ids)} books at location {location_id}", "LOCATION", 
                     {"location_id": location_id, "user_id": user_id, "book_count": len(book_ids), "book_ids": book_ids})
            return book_ids
            
        except Exception as e:
            debug_log(f"Error getting books at location {location_id}: {e}", "ERROR", 
                     {"location_id": location_id, "user_id": user_id, "error": str(e)})
            import traceback
            traceback.print_exc()
            return []
    
    def add_book_to_location(self, book_id: str, location_id: str, user_id: str) -> bool:
        """Add a book to a location for a specific user.
        
        Creates a STORED_AT relationship between the book and location.
        Books can exist in multiple locations.
        """
        debug_log(f"Adding book {book_id} to location {location_id} for user {user_id}", "LOCATION")
        
        try:
            # Verify the location exists and belongs to the user
            location = self.get_location(location_id)
            if not location:
                debug_log(f"Location {location_id} not found", "LOCATION")
                return False
            
            # Check if this book-location combination already exists
            check_query = """
            MATCH (b:Book {id: $book_id})-[stored:STORED_AT]->(l:Location {id: $location_id})
            RETURN COUNT(stored) as count
            """
            
            result = safe_execute_kuzu_query(check_query, {
                "book_id": book_id,
                "location_id": location_id
            }, operation="check_book_location")
            val = _extract_single_value(result, 0)
            if int(val or 0) > 0:
                debug_log(f"Book {book_id} already stored at location {location_id}", "LOCATION")
                return True  # Already exists, that's fine
            
            # Create the STORED_AT relationship
            create_query = """
            MATCH (b:Book {id: $book_id}), (l:Location {id: $location_id})
            CREATE (b)-[:STORED_AT {created_at: $created_at}]->(l)
            """
            
            safe_execute_kuzu_query(create_query, {
                "book_id": book_id,
                "location_id": location_id,
                "created_at": datetime.now(timezone.utc)
            }, operation="add_book_to_location")
            
            debug_log(f"✅ Book {book_id} added to location {location_id} for user {user_id}", "LOCATION")
            return True
            
        except Exception as e:
            debug_log(f"❌ Error adding book to location: {e}", "LOCATION")
            import traceback
            traceback.print_exc()
            return False

    def remove_book_from_location(self, book_id: str, location_id: str, user_id: str) -> bool:
        """Remove a book from a location for a specific user.
        
        Deletes the STORED_AT relationship between the book and location for this user.
        """
        debug_log(f"Removing book {book_id} from location {location_id}", "LOCATION")
        
        try:
            # Delete the STORED_AT relationship
            delete_query = """
            MATCH (b:Book {id: $book_id})-[stored:STORED_AT]->(l:Location {id: $location_id})
            DELETE stored
            """
            
            safe_execute_kuzu_query(delete_query, {
                "book_id": book_id,
                "location_id": location_id
            }, operation="remove_book_from_location")
            
            debug_log(f"✅ Book {book_id} removed from location {location_id}", "LOCATION")
            return True
            
        except Exception as e:
            debug_log(f"❌ Error removing book from location: {e}", "LOCATION")
            import traceback
            traceback.print_exc()
            return False

    def get_book_locations(self, book_id: str, user_id: Optional[str] = None) -> List[Location]:
        """Get all locations where a book is stored.
        
        Args:
            book_id: The book to find locations for
            user_id: Optional user filter. If provided, only get locations for this user.
                    If None, get all locations where the book is stored.
        """
        debug_log(f"Getting locations for book {book_id}", "LOCATION")
        
        try:
            # Get locations for this book (universal)
            query = """
            MATCH (b:Book {id: $book_id})-[stored:STORED_AT]->(l:Location)
            RETURN l
            """
            params = {'book_id': book_id}
            
            result = safe_execute_kuzu_query(query, params, operation="location_operation")
            locations: List[Location] = []
            rows = _convert_query_result_to_list(result)
            for r in rows:
                node = r.get('result') or r.get('col_0')
                location_data = _node_to_dict(node)
                if not location_data:
                    continue
                location = Location(
                    id=location_data['id'],
                    user_id="",  # Will be filled by relationship if needed
                    name=location_data['name'],
                    description=location_data.get('description'),
                    location_type=location_data['location_type'],
                    address=location_data.get('address'),
                    is_default=location_data['is_default'],
                    is_active=True,
                    created_at=_ensure_datetime(location_data.get('created_at')) or datetime.now(timezone.utc),
                    updated_at=_ensure_datetime(location_data.get('updated_at')) or _ensure_datetime(location_data.get('created_at')) or datetime.now(timezone.utc)
                )
                locations.append(location)
                
            debug_log(f"Found {len(locations)} locations for book {book_id}", "LOCATION")
            return locations
            
        except Exception as e:
            debug_log(f"Error getting locations for book {book_id}: {e}", "LOCATION")
            import traceback
            traceback.print_exc()
            return []

    def set_book_location(self, book_id: str, location_id: Optional[str], user_id: str) -> bool:
        """Set a book's primary location by managing STORED_AT relationships.
        
        This is a convenience method that:
        1. If location_id is None, removes the book from all user's locations
        2. If location_id is provided, ensures the book is only at that location for this user
        """
        debug_log(f"Setting book {book_id} location to {location_id} for user {user_id}", "LOCATION")
        
        try:
            # First, remove from all current locations for this user
            current_locations = self.get_book_locations(book_id, user_id)
            for location in current_locations:
                if location.id:
                    self.remove_book_from_location(book_id, location.id, user_id)
            
            # Then add to the new location if specified
            if location_id:
                return self.add_book_to_location(book_id, location_id, user_id)
            else:
                debug_log(f"✅ Book {book_id} removed from all locations for user {user_id}", "LOCATION")
                return True
            
        except Exception as e:
            debug_log(f"❌ Error setting book location: {e}", "LOCATION")
            return False

    def migrate_user_books_to_default_location(self, user_id: str) -> int:
        """Migrate books that have no location to the user's default location.
        
    Legacy note: previously matched (u)-[:OWNS]->(b). In universal library mode all
    books are global; we instead look for books that have personal metadata for the
    user (HAS_PERSONAL_METADATA) but lack any STORED_AT relationship. This keeps
    semantics similar (only migrate books the user interacted with).
        """
        debug_log(f"Migrating books without location for user {user_id}", "LOCATION")
        
        # Get default location (universal)
        default_location = self.get_default_location()
        if not default_location:
            debug_log(f"No default location found for user {user_id}, cannot migrate", "LOCATION")
            return 0
        
        try:
            # Find books with personal metadata for user lacking any location
            query = """
            MATCH (u:User {id: $user_id})-[:HAS_PERSONAL_METADATA]->(b:Book)
            WHERE NOT EXISTS { MATCH (b)-[:STORED_AT]->(:Location) }
            RETURN b.id as book_id
            """
            
            result = safe_execute_kuzu_query(query, {'user_id': user_id}, operation="location_operation")
            rows = _convert_query_result_to_list(result)
            migration_count = 0
            for r in rows:
                book_id = r.get('result') or r.get('col_0')
                if book_id and default_location.id and self.add_book_to_location(str(book_id), default_location.id, user_id):
                    migration_count += 1
                    debug_log(f"Migrated book {book_id} to default location {default_location.name}", "LOCATION")
            
            debug_log(f"Successfully migrated {migration_count} books to default location for user {user_id}", "LOCATION")
            return migration_count
            
        except Exception as e:
            debug_log(f"Error during migration: {e}", "LOCATION")
            import traceback
            traceback.print_exc()
            return 0
