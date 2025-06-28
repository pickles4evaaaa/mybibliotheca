"""
Location management service for tracking book locations.
"""

from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime
import json
from dataclasses import asdict

from .domain.models import Location
from .debug_system import debug_log, get_debug_manager


class LocationService:
    """Service for managing user locations."""
    
    def __init__(self, kuzu_connection):
        self.kuzu_conn = kuzu_connection
        self.debug_manager = get_debug_manager()
        debug_log(f"LocationService initialized with Kuzu connection: {type(kuzu_connection)}", "LOCATION")
    
    def create_location(self, user_id: str, name: str, description: Optional[str] = None, 
                       location_type: str = "home", address: Optional[str] = None,
                       is_default: bool = False) -> Location:
        """Create a new location for a user."""
        debug_log(f"Creating location for user {user_id}: name='{name}', type='{location_type}', is_default={is_default}", 
                 "LOCATION", {"user_id": user_id, "location_name": name, "location_type": location_type})
        
        location_id = str(uuid.uuid4())
        
        # If this is the first location or explicitly set as default, make it default
        existing_locations = self.get_user_locations(user_id)
        debug_log(f"User {user_id} has {len(existing_locations)} existing locations", "LOCATION")
        
        if not existing_locations or is_default:
            # Clear other defaults if setting this as default
            if is_default:
                debug_log(f"Clearing other defaults for user {user_id}", "LOCATION")
                for existing_location in existing_locations:
                    # Update existing locations to not be default
                    update_query = """
                    MATCH (l:Location) 
                    WHERE l.id = $location_id 
                    SET l.is_default = false
                    """
                    self.kuzu_conn.execute(update_query, {"location_id": existing_location.id})
            is_default = True
            debug_log(f"Setting as default location for user {user_id}", "LOCATION")
            
        location = Location(
            id=location_id,
            user_id=user_id,
            name=name,
            description=description,
            location_type=location_type,
            address=address,
            is_default=is_default,
            is_active=True,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # Store in KuzuDB using new schema (no user_id property, use relationship)
        create_query = """
        CREATE (l:Location {
            id: $id,
            name: $name,
            description: $description,
            location_type: $location_type,
            is_default: $is_default,
            created_at: $created_at
        })
        """
        
        location_data = {
            "id": location.id,
            "name": location.name,
            "description": location.description,
            "location_type": location.location_type,
            "is_default": location.is_default,
            "created_at": location.created_at
            # Note: Removed user_id and is_active as they're not in the new schema
        }
        
        print(f"🏠 [CREATE_LOCATION] Storing location data: {location_data}")
        
        # Create the location node
        self.kuzu_conn.execute(create_query, location_data)
        
        # Create the relationship between user and location
        relationship_query = """
        MATCH (u:User {id: $user_id}), (l:Location {id: $location_id})
        CREATE (u)-[:LOCATED_AT {is_primary: $is_primary}]->(l)
        """
        
        self.kuzu_conn.execute(relationship_query, {
            "user_id": user_id,
            "location_id": location.id,
            "is_primary": location.is_default
        })
        
        print(f"🏠 [CREATE_LOCATION] Created location {location_id} for user {user_id}: '{name}' (default: {is_default})")
        return location
    
    def get_location(self, location_id: str) -> Optional[Location]:
        """Get a location by ID."""
        print(f"🏠 [GET_LOCATION] Fetching location {location_id}")
        
        # Get location and its associated user through the LOCATED_AT relationship
        query = """
        MATCH (u:User)-[:LOCATED_AT]->(l:Location) 
        WHERE l.id = $location_id 
        RETURN l, u.id as user_id
        """
        result = self.kuzu_conn.execute(query, {"location_id": location_id})
        
        if not result.has_next():
            print(f"🏠 [GET_LOCATION] Location {location_id} not found")
            return None
            
        row = result.get_next()
        location_data = dict(row[0])
        user_id = row[1]  # user_id from the query
        
        location = Location(
            id=location_data['id'],
            user_id=user_id,  # Get from relationship
            name=location_data['name'],
            description=location_data.get('description'),
            location_type=location_data['location_type'],
            address=location_data.get('address'),
            is_default=location_data['is_default'],
            is_active=True,  # Default since not stored anymore
            created_at=location_data['created_at'],
            updated_at=location_data.get('updated_at', location_data['created_at'])
        )
        
        print(f"🏠 [GET_LOCATION] Found location {location_id}: '{location.name}' (default: {location.is_default})")
        return location
    
    def get_user_locations(self, user_id: str, active_only: bool = True) -> List[Location]:
        """Get all locations for a user."""
        print(f"🏠 [GET_USER_LOCATIONS] Fetching locations for user {user_id} (active_only: {active_only})")
        
        query = "MATCH (u:User {id: $user_id})-[:LOCATED_AT]->(l:Location)"
        # Note: In the new schema, we use is_default instead of is_active
        # For now, we'll return all locations when active_only is requested
        query += " RETURN l ORDER BY l.is_default DESC, l.created_at ASC"
        
        result = self.kuzu_conn.execute(query, {"user_id": user_id})
        
        locations = []
        while result.has_next():
            location_data = dict(result.get_next()[0])
            location = Location(
                id=location_data['id'],
                user_id='',  # Not stored in new schema, use empty string
                name=location_data['name'],
                description=location_data.get('description'),
                location_type=location_data['location_type'],
                address=location_data.get('address'),
                is_default=location_data['is_default'],
                is_active=True,  # Default to True since we don't store this anymore
                created_at=location_data['created_at'],
                updated_at=location_data.get('updated_at', location_data['created_at'])
            )
            locations.append(location)
            print(f"🏠 [GET_USER_LOCATIONS] Added location: '{location.name}' (default: {location.is_default})")
        
        print(f"🏠 [GET_USER_LOCATIONS] Returning {len(locations)} locations for user {user_id}")
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
            # Clear other defaults for this user by finding the user through the relationship
            clear_defaults_query = """
            MATCH (u:User)-[:LOCATED_AT]->(target_loc:Location {id: $location_id})
            MATCH (u)-[:LOCATED_AT]->(l:Location) 
            WHERE l.id <> $location_id 
            SET l.is_default = false
            """
            self.kuzu_conn.execute(clear_defaults_query, {"location_id": location_id})
        
        # Build update query
        set_clauses = []
        params = {"location_id": location_id}
        
        for key, value in updates.items():
            if hasattr(location, key):
                set_clauses.append(f"l.{key} = ${key}")
                params[key] = value
                print(f"🏠 [UPDATE_LOCATION] Will update {key}: -> '{value}'")
        
        if set_clauses:
            set_clauses.append("l.updated_at = $updated_at")
            params["updated_at"] = datetime.utcnow()
            
            update_query = f"""
            MATCH (l:Location) 
            WHERE l.id = $location_id 
            SET {', '.join(set_clauses)}
            """
            
            self.kuzu_conn.execute(update_query, params)
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
        self.kuzu_conn.execute(delete_query, {"location_id": location_id})
        
        print(f"🏠 [DELETE_LOCATION] Deleted location {location_id}: '{location.name}' for user {location.user_id}")
        return True
    
    def get_default_location(self, user_id: str) -> Optional[Location]:
        """Get the user's default location."""
        print(f"🏠 [GET_DEFAULT] Looking for default location for user {user_id}")
        
        locations = self.get_user_locations(user_id)
        for location in locations:
            if location.is_default:
                print(f"🏠 [GET_DEFAULT] Found default location for user {user_id}: '{location.name}' (ID: {location.id})")
                return location
        
        # If no default found, return first location if any exist
        if locations:
            default_location = locations[0]
            print(f"🏠 [GET_DEFAULT] No default found, using first location for user {user_id}: '{default_location.name}' (ID: {default_location.id})")
            return default_location
        
        print(f"🏠 [GET_DEFAULT] No locations found for user {user_id}")
        return None
    
    def setup_default_locations(self, user_id: str) -> List[Location]:
        """Set up default locations for a new user."""
        print(f"🏠 [SETUP_DEFAULT] Setting up default locations for new user {user_id}")
        
        locations = []
        
        # Create default home location
        home = self.create_location(
            user_id=user_id,
            name="Home",
            description="Primary residence",
            location_type="home",
            is_default=True
        )
        locations.append(home)
        
        print(f"🏠 [SETUP_DEFAULT] Created {len(locations)} default locations for user {user_id}")
        return locations
    
    def get_location_book_count(self, location_id: str, user_id: str) -> int:
        """Get the number of books at a specific location for a user."""
        debug_log(f"Getting book count for location {location_id}, user {user_id}", "LOCATION", 
                 {"location_id": location_id, "user_id": user_id})
        
        try:
            # Use direct Cypher query to avoid data type mismatch issues
            from app.infrastructure.kuzu_graph import get_graph_storage
            
            storage = get_graph_storage()
            
            # Query for books owned by user that contain this location_id
            # Handle location_id as either a string or potentially in a list format
            query = """
            MATCH (u:User {id: $user_id})-[r:OWNS]->(b:Book)
            WHERE r.location_id = $location_id
            RETURN COUNT(b) as book_count
            """
            
            result = storage.query(query, {'user_id': user_id, 'location_id': location_id})
            
            count = 0
            if result and len(result) > 0 and 'col_0' in result[0]:
                count = int(result[0]['col_0']) if result[0]['col_0'] is not None else 0
            
            debug_log(f"Location {location_id} has {count} books for user {user_id}", "LOCATION", 
                     {"location_id": location_id, "user_id": user_id, "book_count": count})
            return count
            
        except Exception as e:
            debug_log(f"Error getting book count for location {location_id}: {e}", "ERROR", 
                     {"location_id": location_id, "user_id": user_id, "error": str(e)})
            import traceback
            traceback.print_exc()
            return 0
    
    def get_all_location_book_counts(self, user_id: str) -> Dict[str, int]:
        """Get book counts for all user locations."""
        debug_log(f"Getting book counts for all locations for user {user_id}", "LOCATION", {"user_id": user_id})
        
        locations = self.get_user_locations(user_id)
        counts = {}
        
        for location in locations:
            counts[location.id] = self.get_location_book_count(location.id, user_id)
        
        debug_log(f"Location book counts: {counts}", "LOCATION", {"user_id": user_id, "counts": counts})
        return counts
    
    def get_books_at_location(self, location_id: str, user_id: str) -> List[str]:
        """Get list of book IDs at a specific location for a user."""
        debug_log(f"Getting books at location {location_id} for user {user_id}", "LOCATION", 
                 {"location_id": location_id, "user_id": user_id})
        
        try:
            # Use direct Cypher query to avoid data type mismatch issues
            from app.infrastructure.kuzu_graph import get_graph_storage
            
            storage = get_graph_storage()
            
            # Query for books owned by user that contain this location_id
            # Handle location_id as either a string or potentially in a list format
            query = """
            MATCH (u:User {id: $user_id})-[r:OWNS]->(b:Book)
            WHERE r.location_id = $location_id
            RETURN b.id as book_id
            """
            
            result = storage.query(query, {'user_id': user_id, 'location_id': location_id})
            book_ids = []
            
            for record in result:
                if 'col_0' in record and record['col_0']:
                    book_id = record['col_0']
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
