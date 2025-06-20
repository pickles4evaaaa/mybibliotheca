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


class LocationService:
    """Service for managing user locations."""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        
    def create_location(self, user_id: str, name: str, description: Optional[str] = None, 
                       location_type: str = "home", address: Optional[str] = None,
                       is_default: bool = False) -> Location:
        """Create a new location for a user."""
        location_id = str(uuid.uuid4())
        
        # If this is the first location or explicitly set as default, make it default
        existing_locations = self.get_user_locations(user_id)
        if not existing_locations or is_default:
            # Clear other defaults if setting this as default
            if is_default:
                for loc_id in existing_locations:
                    self.redis.hset(f"location:{loc_id}", "is_default", "false")
            is_default = True
            
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
        
        self.redis.hmset(f"location:{location_id}", location_data)
        self.redis.sadd(f"user:{user_id}:locations", location_id)
        
        return location
    
    def get_location(self, location_id: str) -> Optional[Location]:
        """Get a location by ID."""
        location_data = self.redis.hgetall(f"location:{location_id}")
        if not location_data:
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
            
        return Location(**location_dict)
    
    def get_user_locations(self, user_id: str, active_only: bool = True) -> List[Location]:
        """Get all locations for a user."""
        location_ids = self.redis.smembers(f"user:{user_id}:locations")
        locations = []
        
        for loc_id in location_ids:
            loc_id = loc_id.decode('utf-8') if isinstance(loc_id, bytes) else loc_id
            location = self.get_location(loc_id)
            if location and (not active_only or location.is_active):
                locations.append(location)
                
        # Sort by default first, then by name
        locations.sort(key=lambda x: (not x.is_default, x.name.lower()))
        return locations
    
    def update_location(self, location_id: str, **updates) -> Optional[Location]:
        """Update a location."""
        location = self.get_location(location_id)
        if not location:
            return None
            
        # Handle setting as default
        if updates.get('is_default') and not location.is_default:
            # Clear other defaults for this user
            user_locations = self.get_user_locations(location.user_id, active_only=False)
            for loc in user_locations:
                if loc.id != location_id and loc.is_default:
                    self.redis.hset(f"location:{loc.id}", "is_default", "false")
        
        # Update fields
        for key, value in updates.items():
            if hasattr(location, key):
                setattr(location, key, value)
                
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
        return location
    
    def delete_location(self, location_id: str) -> bool:
        """Delete a location (soft delete by setting inactive)."""
        location = self.get_location(location_id)
        if not location:
            return False
            
        # Don't allow deletion of default location if it's the only one
        user_locations = self.get_user_locations(location.user_id, active_only=True)
        if location.is_default and len(user_locations) == 1:
            return False  # Cannot delete the only location
            
        # If deleting default location, set another as default
        if location.is_default and len(user_locations) > 1:
            for loc in user_locations:
                if loc.id != location_id:
                    self.update_location(loc.id, is_default=True)
                    break
        
        # Soft delete
        self.update_location(location_id, is_active=False)
        return True
    
    def get_default_location(self, user_id: str) -> Optional[Location]:
        """Get the user's default location."""
        locations = self.get_user_locations(user_id)
        for location in locations:
            if location.is_default:
                return location
        return locations[0] if locations else None
    
    def setup_default_locations(self, user_id: str) -> List[Location]:
        """Set up default locations for a new user."""
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
        
        return locations
