"""
Location management service for tracking book locations.
"""

from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime
import json
import redis
from dataclasses import asdict

from .domain.models import Location
from .debug_system import debug_log, get_debug_manager


class LocationService:
    """Service for managing user locations."""
    
    def __init__(self, redis_client):
        self.redis = redis_client
        self.debug_manager = get_debug_manager()
        debug_log(f"LocationService initialized with Redis client: {type(redis_client)}", "LOCATION")
    
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
                for loc_id in existing_locations:
                    self.redis.hset(f"location:{loc_id}", "is_default", "false")
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
        
        # Store in Redis
        location_data = asdict(location)
        # Convert datetime objects to strings for Redis storage
        location_data['created_at'] = location.created_at.isoformat()
        location_data['updated_at'] = location.updated_at.isoformat()
        
        # Convert boolean values to strings for Redis storage
        location_data['is_default'] = str(location_data['is_default']).lower()
        location_data['is_active'] = str(location_data['is_active']).lower()
        
        # Filter out None values (Redis doesn't accept None)
        location_data = {k: v for k, v in location_data.items() if v is not None}
        
        print(f"🏠 [CREATE_LOCATION] Storing location data: {location_data}")
        
        self.redis.hmset(f"location:{location_id}", location_data)
        self.redis.sadd(f"user:{user_id}:locations", location_id)
        
        print(f"🏠 [CREATE_LOCATION] Created location {location_id} for user {user_id}: '{name}' (default: {is_default})")
        return location
    
    def get_location(self, location_id: str) -> Optional[Location]:
        """Get a location by ID."""
        print(f"🏠 [GET_LOCATION] Fetching location {location_id}")
        
        location_data = self.redis.hgetall(f"location:{location_id}")
        if not location_data:
            print(f"🏠 [GET_LOCATION] Location {location_id} not found")
            return None
            
        # Convert byte strings to strings and handle datetime conversion
        location_dict = {}
        for key, value in location_data.items():
            key = key.decode('utf-8') if isinstance(key, bytes) else key
            value = value.decode('utf-8') if isinstance(value, bytes) else value
            
            # Convert boolean strings back to booleans
            if key in ('is_default', 'is_active'):
                value = value.lower() == 'true'
            # Convert datetime strings back to datetime objects
            elif key in ('created_at', 'updated_at'):
                value = datetime.fromisoformat(value)
                
            location_dict[key] = value
            
        location = Location(**location_dict)
        print(f"🏠 [GET_LOCATION] Found location {location_id}: '{location.name}' (default: {location.is_default})")
        return location
    
    def get_user_locations(self, user_id: str, active_only: bool = True) -> List[Location]:
        """Get all locations for a user."""
        print(f"🏠 [GET_USER_LOCATIONS] Fetching locations for user {user_id} (active_only: {active_only})")
        
        location_ids = self.redis.smembers(f"user:{user_id}:locations")
        if not location_ids:
            print(f"🏠 [GET_USER_LOCATIONS] No location IDs found for user {user_id}")
            return []
            
        print(f"🏠 [GET_USER_LOCATIONS] Found {len(location_ids)} location IDs for user {user_id}")
        
        locations = []
        for location_id in location_ids:
            location_id = location_id.decode('utf-8') if isinstance(location_id, bytes) else location_id
            location = self.get_location(location_id)
            if location:
                if not active_only or location.is_active:
                    locations.append(location)
                    print(f"🏠 [GET_USER_LOCATIONS] Added location: '{location.name}' (default: {location.is_default}, active: {location.is_active})")
                else:
                    print(f"🏠 [GET_USER_LOCATIONS] Skipped inactive location: '{location.name}'")
            else:
                print(f"🏠 [GET_USER_LOCATIONS] Location {location_id} not found, removing from user set")
                self.redis.srem(f"user:{user_id}:locations", location_id)
        
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
            print(f"🏠 [UPDATE_LOCATION] Setting location {location_id} as default, clearing others for user {location.user_id}")
            # Clear other defaults for this user
            user_locations = self.get_user_locations(location.user_id, active_only=False)
            for loc in user_locations:
                if loc.id != location_id and loc.is_default:
                    print(f"🏠 [UPDATE_LOCATION] Clearing default from location {loc.id}: '{loc.name}'")
                    self.redis.hset(f"location:{loc.id}", "is_default", "false")
        
        # Update fields
        for key, value in updates.items():
            if hasattr(location, key):
                old_value = getattr(location, key)
                setattr(location, key, value)
                print(f"🏠 [UPDATE_LOCATION] Updated {key}: '{old_value}' -> '{value}'")
                
        location.updated_at = datetime.utcnow()
        
        # Store updated location
        location_data = asdict(location)
        location_data['created_at'] = location.created_at.isoformat()
        location_data['updated_at'] = location.updated_at.isoformat()
        
        # Convert boolean values to strings for Redis storage
        location_data['is_default'] = str(location_data['is_default']).lower()
        location_data['is_active'] = str(location_data['is_active']).lower()
        
        # Filter out None values (Redis doesn't accept None)
        location_data = {k: v for k, v in location_data.items() if v is not None}
        
        self.redis.hmset(f"location:{location_id}", location_data)
        print(f"🏠 [UPDATE_LOCATION] Updated location {location_id}: '{location.name}' (default: {location.is_default})")
        return location
    
    def delete_location(self, location_id: str) -> bool:
        """Delete a location."""
        print(f"🏠 [DELETE_LOCATION] Deleting location {location_id}")
        
        location = self.get_location(location_id)
        if not location:
            print(f"🏠 [DELETE_LOCATION] Location {location_id} not found")
            return False
        
        # Remove from Redis
        self.redis.delete(f"location:{location_id}")
        self.redis.srem(f"user:{location.user_id}:locations", location_id)
        
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
            # Use graph storage to get user-book relationships
            from app.infrastructure.redis_graph import RedisGraphStorage, RedisGraphConnection
            import os
            
            redis_url = os.getenv('REDIS_URL', 'redis://redis-graph:6379/0')
            connection = RedisGraphConnection(redis_url=redis_url)
            storage = RedisGraphStorage(connection)
            
            # Get all books owned by the user
            relationships = storage.get_relationships('user', user_id, 'owns')
            count = 0
            
            for rel in relationships:
                properties = rel.get('properties', {})
                locations = properties.get('locations', [])
                
                # Handle both string and list formats for locations
                if isinstance(locations, str):
                    try:
                        import json
                        locations = json.loads(locations)
                    except (json.JSONDecodeError, TypeError):
                        locations = [locations] if locations else []
                elif not isinstance(locations, list):
                    locations = []
                
                if location_id in locations:
                    count += 1
                    debug_log(f"Found book at location: {rel.get('to_id')}", "LOCATION", 
                             {"book_id": rel.get('to_id'), "location_id": location_id})
            
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
            # Use graph storage to get user-book relationships
            from app.infrastructure.redis_graph import RedisGraphStorage, RedisGraphConnection
            import os
            
            redis_url = os.getenv('REDIS_URL', 'redis://redis-graph:6379/0')
            connection = RedisGraphConnection(redis_url=redis_url)
            storage = RedisGraphStorage(connection)
            
            # Get all books owned by the user
            relationships = storage.get_relationships('user', user_id, 'owns')
            book_ids = []
            
            for rel in relationships:
                properties = rel.get('properties', {})
                locations = properties.get('locations', [])
                
                # Handle both string and list formats for locations
                if isinstance(locations, str):
                    try:
                        import json
                        locations = json.loads(locations)
                    except (json.JSONDecodeError, TypeError):
                        locations = [locations] if locations else []
                elif not isinstance(locations, list):
                    locations = []
                
                if location_id in locations:
                    book_id = rel.get('to_id')
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
