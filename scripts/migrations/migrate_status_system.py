"""
Migration utility for the new flexible status system.
Converts existing want_to_read/library_only flags to the new status system.
"""

import os
import sys
import redis
from datetime import datetime

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.domain.models import ReadingStatus, OwnershipStatus, MediaType
from app.location_service import LocationService


def migrate_book_statuses():
    """Migrate existing book statuses to new system."""
    redis_url = os.getenv('REDIS_URL', 'redis://redis-graph:6379/0')
    redis_client = redis.from_url(redis_url)
    location_service = LocationService(redis_client)
    
    print("Starting book status migration...")
    
    # Get all user IDs
    user_keys = redis_client.keys("user:*:books")
    migrated_count = 0
    users_migrated = 0
    
    for user_key in user_keys:
        user_id = user_key.decode('utf-8').split(':')[1]
        print(f"Migrating books for user {user_id}")
        
        # Set up default location for user if they don't have any
        existing_locations = location_service.get_user_locations(user_id)
        if not existing_locations:
            location_service.setup_default_locations(user_id)
            print(f"  Created default location for user {user_id}")
        
        default_location = location_service.get_default_location(user_id)
        default_location_id = default_location.id if default_location else None
        
        # Get user's books
        book_ids = redis_client.smembers(user_key)
        
        for book_id in book_ids:
            book_id = book_id.decode('utf-8')
            relationship_key = f"user:{user_id}:book:{book_id}"
            relationship_data = redis_client.hgetall(relationship_key)
            
            if not relationship_data:
                continue
                
            # Convert bytes to strings
            rel_data = {}
            for k, v in relationship_data.items():
                k = k.decode('utf-8') if isinstance(k, bytes) else k
                v = v.decode('utf-8') if isinstance(v, bytes) else v
                rel_data[k] = v
            
            # Determine new reading status
            want_to_read = rel_data.get('want_to_read', 'false').lower() == 'true'
            library_only = rel_data.get('library_only', 'false').lower() == 'true'
            finish_date = rel_data.get('finish_date')
            start_date = rel_data.get('start_date')
            
            if finish_date and finish_date != 'None':
                new_reading_status = ReadingStatus.READ.value
            elif start_date and start_date != 'None' and not finish_date:
                new_reading_status = ReadingStatus.READING.value
            elif want_to_read:
                new_reading_status = ReadingStatus.PLAN_TO_READ.value
            elif library_only:
                new_reading_status = ReadingStatus.LIBRARY_ONLY.value
            else:
                new_reading_status = ReadingStatus.PLAN_TO_READ.value
            
            # Update relationship with new fields
            updates = {
                'reading_status': new_reading_status,
                'ownership_status': OwnershipStatus.OWNED.value,
                'media_type': MediaType.PHYSICAL.value,
                'locations': f'["{default_location_id}"]' if default_location_id else '[]',
                'primary_location_id': default_location_id or '',
                'updated_at': datetime.utcnow().isoformat()
            }
            
            # Add new fields while preserving existing ones
            for key, value in updates.items():
                redis_client.hset(relationship_key, key, value)
            
            migrated_count += 1
            
        users_migrated += 1
        print(f"  Migrated {len(book_ids)} books for user {user_id}")
    
    print(f"Migration complete! Migrated {migrated_count} books for {users_migrated} users.")


def rollback_migration():
    """Rollback migration by removing new fields."""
    redis_url = os.getenv('REDIS_URL', 'redis://redis-graph:6379/0')
    redis_client = redis.from_url(redis_url)
    
    print("Rolling back book status migration...")
    
    # Fields to remove
    new_fields = [
        'reading_status', 'ownership_status', 'media_type', 
        'locations', 'primary_location_id',
        'borrowed_from', 'borrowed_from_user_id', 'borrowed_date', 'borrowed_due_date',
        'loaned_to', 'loaned_to_user_id', 'loaned_date', 'loaned_due_date'
    ]
    
    # Get all user IDs
    user_keys = redis_client.keys("user:*:books")
    rollback_count = 0
    
    for user_key in user_keys:
        user_id = user_key.decode('utf-8').split(':')[1]
        book_ids = redis_client.smembers(user_key)
        
        for book_id in book_ids:
            book_id = book_id.decode('utf-8')
            relationship_key = f"user:{user_id}:book:{book_id}"
            
            # Remove new fields
            redis_client.hdel(relationship_key, *new_fields)
            rollback_count += 1
    
    print(f"Rollback complete! Processed {rollback_count} book relationships.")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--rollback":
        rollback_migration()
    else:
        migrate_book_statuses()
