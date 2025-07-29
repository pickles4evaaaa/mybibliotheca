"""
Location management service for tracking book locations.

This service manages locations as completely independent entities from users.
Only books have relationships with locations via STORED_AT relationships.
Users access locations indirectly through their books.
The system uses proper graph relationships, not properties on nodes.
"""

from typing import List, Optional, Dict, Any, Set
import uuid
from datetime import datetime
import json
from dataclasses import asdict

from .domain.models import Location
from .debug_system import debug_log, get_debug_manager
from .utils.safe_kuzu_manager import SafeKuzuManager


class LocationService:
    """Service for managing user locations and book-location relationships."""
    
    def __init__(self, kuzu_db_path: Optional[str] = None):
        self.safe_kuzu = SafeKuzuManager(kuzu_db_path)
        self.debug_manager = get_debug_manager()
        debug_log(f"LocationService initialized with SafeKuzuManager", "LOCATION")
    
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
            self.safe_kuzu.execute_query(clear_defaults_query, {}, operation="clear_default_locations")
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
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
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
        
        print(f"🏠 [CREATE_LOCATION] Storing location data: {location_data}")
        
        # Create the location node (completely independent of users)
        self.safe_kuzu.execute_query(create_query, location_data, operation="location_operation")
        
        print(f"🏠 [CREATE_LOCATION] Created location {location_id}: '{name}' (default: {is_default})")
        return location
    
    def get_location(self, location_id: str) -> Optional[Location]:
        """Get a location by ID."""
        print(f"🏠 [GET_LOCATION] Fetching location {location_id}")
        
        # Get location by ID
        query = """
        MATCH (l:Location {id: $location_id}) 
        RETURN l
        """
        result = self.safe_kuzu.execute_query(query, {"location_id": location_id}, operation="location_operation")
        
        if not result.has_next():
            print(f"🏠 [GET_LOCATION] Location {location_id} not found")
            return None
            
        row = result.get_next()
        location_data = dict(row[0])
        
        location = Location(
            id=location_data['id'],
            user_id="",  # Locations don't belong to users
            name=location_data['name'],
            description=location_data.get('description'),
            location_type=location_data['location_type'],
            address=location_data.get('address'),
            is_default=location_data['is_default'],
            is_active=location_data.get('is_active', True),
            created_at=datetime.fromisoformat(location_data['created_at']) if isinstance(location_data['created_at'], str) else location_data['created_at'],
            updated_at=datetime.fromisoformat(location_data.get('updated_at', location_data['created_at'])) if isinstance(location_data.get('updated_at', location_data['created_at']), str) else location_data.get('updated_at', location_data['created_at'])
        )
        
        print(f"🏠 [GET_LOCATION] Found location {location_id}: '{location.name}' (default: {location.is_default})")
        return location
    
    def get_user_locations(self, user_id: str, active_only: bool = True) -> List[Location]:
        """Get all locations that contain books for a user (via STORED_AT relationships)."""
        print(f"🏠 [GET_USER_LOCATIONS] Fetching locations with books for user {user_id} (active_only: {active_only})")
        
        # Query locations that have books stored by this user
        query = """
        MATCH (b:Book)-[stored:STORED_AT]->(l:Location)
        WHERE stored.user_id = $user_id
        """
        if active_only:
            query += " AND l.is_active = true"
        query += " RETURN DISTINCT l ORDER BY l.is_default DESC, l.name ASC"
        
        result = self.safe_kuzu.execute_query(query, {"user_id": user_id}, operation="location_operation")
        
        locations = []
        while result.has_next():
            row = result.get_next()
            location_data = dict(row[0])
            location = Location(
                id=location_data['id'],
                user_id="",  # Locations don't belong to users
                name=location_data['name'],
                description=location_data.get('description'),
                location_type=location_data['location_type'],
                address=location_data.get('address'),
                is_default=location_data['is_default'],
                is_active=location_data.get('is_active', True),
                created_at=datetime.fromisoformat(location_data['created_at']) if isinstance(location_data['created_at'], str) else location_data['created_at'],
                updated_at=datetime.fromisoformat(location_data.get('updated_at', location_data['created_at'])) if isinstance(location_data.get('updated_at', location_data['created_at']), str) else location_data.get('updated_at', location_data['created_at'])
            )
            locations.append(location)
            print(f"🏠 [GET_USER_LOCATIONS] Added location: '{location.name}' (default: {location.is_default})")
        
        print(f"🏠 [GET_USER_LOCATIONS] Returning {len(locations)} locations for user {user_id}")
        return locations
    
    def get_all_locations(self, active_only: bool = True) -> List[Location]:
        """Get all locations in the system, regardless of whether they have books."""
        print(f"🏠 [GET_ALL_LOCATIONS] Fetching all locations (active_only: {active_only})")
        
        # Query all locations in the system
        query = "MATCH (l:Location)"
        if active_only:
            query += " WHERE l.is_active = true"
        query += " RETURN l ORDER BY l.is_default DESC, l.name ASC"
        
        result = self.safe_kuzu.execute_query(query, {}, operation="location_operation")
        
        locations = []
        while result.has_next():
            row = result.get_next()
            location_data = dict(row[0])
            location = Location(
                id=location_data['id'],
                user_id="",  # Locations don't belong to users
                name=location_data['name'],
                description=location_data.get('description'),
                location_type=location_data['location_type'],
                address=location_data.get('address'),
                is_default=location_data['is_default'],
                is_active=location_data.get('is_active', True),
                created_at=datetime.fromisoformat(location_data['created_at']) if isinstance(location_data['created_at'], str) else location_data['created_at'],
                updated_at=datetime.fromisoformat(location_data.get('updated_at', location_data['created_at'])) if isinstance(location_data.get('updated_at', location_data['created_at']), str) else location_data.get('updated_at', location_data['created_at'])
            )
            locations.append(location)
            print(f"🏠 [GET_ALL_LOCATIONS] Added location: '{location.name}' (default: {location.is_default})")
        
        print(f"🏠 [GET_ALL_LOCATIONS] Returning {len(locations)} total locations")
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
            self.safe_kuzu.execute_query(clear_defaults_query, {"location_id": location_id}, operation="location_operation")
        
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
            params["updated_at"] = datetime.utcnow()
            
            update_query = f"""
            MATCH (l:Location) 
            WHERE l.id = $location_id 
            SET {', '.join(set_clauses)}
            """
            
            # Execute with proper typing for KuzuDB
            self.safe_kuzu.execute_query(update_query, params, operation="location_operation")
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
        self.safe_kuzu.execute_query(delete_query, {"location_id": location_id}, operation="location_operation")
        
        print(f"🏠 [DELETE_LOCATION] Deleted location {location_id}: '{location.name}'")
        return True
    
    def get_default_location(self, user_id: str) -> Optional[Location]:
        """Get the default location."""
        print(f"🏠 [GET_DEFAULT] Looking for default location for user {user_id}")
        
        # First try to find a default location that has books for this user
        query = """
        MATCH (b:Book)-[stored:STORED_AT]->(l:Location)
        WHERE stored.user_id = $user_id AND l.is_default = true
        RETURN DISTINCT l
        LIMIT 1
        """
        result = self.safe_kuzu.execute_query(query, {"user_id": user_id}, operation="location_operation")
        
        if result.has_next():
            row = result.get_next()
            location_data = dict(row[0])
            location = Location(
                id=location_data['id'],
                user_id="",
                name=location_data['name'],
                description=location_data.get('description'),
                location_type=location_data['location_type'],
                address=location_data.get('address'),
                is_default=location_data['is_default'],
                is_active=location_data.get('is_active', True),
                created_at=datetime.fromisoformat(location_data['created_at']) if isinstance(location_data['created_at'], str) else location_data['created_at'],
                updated_at=datetime.fromisoformat(location_data.get('updated_at', location_data['created_at'])) if isinstance(location_data.get('updated_at', location_data['created_at']), str) else location_data.get('updated_at', location_data['created_at'])
            )
            print(f"🏠 [GET_DEFAULT] Found default location for user {user_id}: '{location.name}' (ID: {location.id})")
            return location
        
        # If no default location with books, try to find any default location in the system
        default_query = """
        MATCH (l:Location)
        WHERE l.is_default = true
        RETURN l
        LIMIT 1
        """
        result = self.safe_kuzu.execute_query(default_query, {}, operation="location_operation")
        
        if result.has_next():
            row = result.get_next()
            location_data = dict(row[0])
            location = Location(
                id=location_data['id'],
                user_id="",
                name=location_data['name'],
                description=location_data.get('description'),
                location_type=location_data['location_type'],
                address=location_data.get('address'),
                is_default=location_data['is_default'],
                is_active=location_data.get('is_active', True),
                created_at=datetime.fromisoformat(location_data['created_at']) if isinstance(location_data['created_at'], str) else location_data['created_at'],
                updated_at=datetime.fromisoformat(location_data.get('updated_at', location_data['created_at'])) if isinstance(location_data.get('updated_at', location_data['created_at']), str) else location_data.get('updated_at', location_data['created_at'])
            )
            print(f"🏠 [GET_DEFAULT] Found system default location: '{location.name}' (ID: {location.id})")
            return location
        
        # If no default location at all, get any location with books for this user
        locations = self.get_user_locations(user_id)
        if locations:
            default_location = locations[0]
            print(f"🏠 [GET_DEFAULT] No default found, using first location with books for user {user_id}: '{default_location.name}' (ID: {default_location.id})")
            return default_location
        
        # If no locations with books, get any available location
        all_locations = self.get_all_locations()
        if all_locations:
            default_location = all_locations[0]
            print(f"🏠 [GET_DEFAULT] No locations with books, using first available location: '{default_location.name}' (ID: {default_location.id})")
            return default_location
        
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
                # Count books stored at this location for a specific user
                query = """
                MATCH (b:Book)-[stored:STORED_AT]->(l:Location {id: $location_id})
                WHERE stored.user_id = $user_id
                RETURN COUNT(b) as book_count
                """
                params = {'location_id': location_id, 'user_id': user_id}
            else:
                # Count all books at this location regardless of user
                query = """
                MATCH (b:Book)-[:STORED_AT]->(l:Location {id: $location_id})
                RETURN COUNT(b) as book_count
                """
                params = {'location_id': location_id}
            
            result = self.safe_kuzu.execute_query(query, params, operation="location_operation")
            
            count = 0
            if result.has_next():
                count_result = result.get_next()
                if count_result and len(count_result) > 0:
                    count = int(count_result[0])
            
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
            if user_id:
                # Get books stored at this location for a specific user
                query = """
                MATCH (b:Book)-[stored:STORED_AT]->(l:Location {id: $location_id})
                WHERE stored.user_id = $user_id
                RETURN b.id as book_id
                """
                params = {'location_id': location_id, 'user_id': user_id}
            else:
                # Get all books at this location regardless of user
                query = """
                MATCH (b:Book)-[:STORED_AT]->(l:Location {id: $location_id})
                RETURN b.id as book_id
                """
                params = {'location_id': location_id}
            
            result = self.safe_kuzu.execute_query(query, params, operation="location_operation")
            book_ids = []
            
            while result.has_next():
                row = result.get_next()
                book_id = row[0]
                
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
            
            # Check if this book-location-user combination already exists
            check_query = """
            MATCH (b:Book {id: $book_id})-[stored:STORED_AT]->(l:Location {id: $location_id})
            WHERE stored.user_id = $user_id
            RETURN COUNT(stored) as count
            """
            
            result = self.safe_kuzu.execute_query(check_query, {
                "book_id": book_id,
                "location_id": location_id,
                "user_id": user_id
            }, operation="check_book_location")
            
            if result.has_next() and result.get_next()[0] > 0:
                debug_log(f"Book {book_id} already stored at location {location_id} for user {user_id}", "LOCATION")
                return True  # Already exists, that's fine
            
            # Create the STORED_AT relationship
            create_query = """
            MATCH (b:Book {id: $book_id}), (l:Location {id: $location_id})
            CREATE (b)-[:STORED_AT {user_id: $user_id, created_at: $created_at}]->(l)
            """
            
            self.safe_kuzu.execute_query(create_query, {
                "book_id": book_id,
                "location_id": location_id,
                "user_id": user_id,
                "created_at": datetime.utcnow()
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
        debug_log(f"Removing book {book_id} from location {location_id} for user {user_id}", "LOCATION")
        
        try:
            # Delete the STORED_AT relationship
            delete_query = """
            MATCH (b:Book {id: $book_id})-[stored:STORED_AT]->(l:Location {id: $location_id})
            WHERE stored.user_id = $user_id
            DELETE stored
            """
            
            self.safe_kuzu.execute_query(delete_query, {
                "book_id": book_id,
                "location_id": location_id,
                "user_id": user_id
            }, operation="remove_book_from_location")
            
            debug_log(f"✅ Book {book_id} removed from location {location_id} for user {user_id}", "LOCATION")
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
        debug_log(f"Getting locations for book {book_id} (user filter: {user_id})", "LOCATION")
        
        try:
            if user_id:
                # Get locations for this book for a specific user
                query = """
                MATCH (b:Book {id: $book_id})-[stored:STORED_AT]->(l:Location)
                WHERE stored.user_id = $user_id
                RETURN l
                """
                params = {'book_id': book_id, 'user_id': user_id}
            else:
                # Get all locations for this book regardless of user
                query = """
                MATCH (b:Book {id: $book_id})-[:STORED_AT]->(l:Location)
                RETURN l
                """
                params = {'book_id': book_id}
            
            result = self.safe_kuzu.execute_query(query, params, operation="location_operation")
            locations = []
            
            while result.has_next():
                row = result.get_next()
                location_data = dict(row[0])
                
                location = Location(
                    id=location_data['id'],
                    user_id="",  # Will be filled by relationship if needed
                    name=location_data['name'],
                    description=location_data.get('description'),
                    location_type=location_data['location_type'],
                    address=location_data.get('address'),
                    is_default=location_data['is_default'],
                    is_active=True,
                    created_at=datetime.fromisoformat(location_data['created_at']) if isinstance(location_data['created_at'], str) else location_data['created_at'],
                    updated_at=datetime.fromisoformat(location_data.get('updated_at', location_data['created_at'])) if isinstance(location_data.get('updated_at', location_data['created_at']), str) else location_data.get('updated_at', location_data['created_at'])
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
        
        This helps transition from the old OWNS.location_id system to STORED_AT relationships.
        """
        debug_log(f"Migrating books without location for user {user_id}", "LOCATION")
        
        # Get default location
        default_location = self.get_default_location(user_id)
        if not default_location:
            debug_log(f"No default location found for user {user_id}, cannot migrate", "LOCATION")
            return 0
        
        try:
            # Find books owned by user that have no STORED_AT relationships for this user
            query = """
            MATCH (u:User {id: $user_id})-[:OWNS]->(b:Book)
            WHERE NOT EXISTS {
                MATCH (b)-[stored:STORED_AT]->(:Location)
                WHERE stored.user_id = $user_id
            }
            RETURN b.id as book_id
            """
            
            result = self.safe_kuzu.execute_query(query, {'user_id': user_id}, operation="location_operation")
            
            migration_count = 0
            while result.has_next():
                row = result.get_next()
                book_id = row[0]
                
                if book_id and default_location.id and self.add_book_to_location(book_id, default_location.id, user_id):
                    migration_count += 1
                    debug_log(f"Migrated book {book_id} to default location {default_location.name}", "LOCATION")
            
            debug_log(f"Successfully migrated {migration_count} books to default location for user {user_id}", "LOCATION")
            return migration_count
            
        except Exception as e:
            debug_log(f"Error during migration: {e}", "LOCATION")
            import traceback
            traceback.print_exc()
            return 0
