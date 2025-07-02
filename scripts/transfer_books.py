#!/usr/bin/env python3
"""
🚨 OBSOLETE: Redis-based Book Transfer Tool

This script is OBSOLETE and no longer works with the current Kuzu-based system.

Use the new Kuzu-based transfer tool instead:
    scripts/transfer_books_kuzu.py

DEPRECATED: Book Ownership Transfer Utility
===============================

This script transfers book ownership from one user to another in Redis.
Useful for fixing migration user mapping issues.
"""

print("🚨 OBSOLETE SCRIPT")
print("=" * 50)
print("This Redis-based transfer script is obsolete.")
print("The system now uses Kuzu graph database.")
print("")
print("✅ Use the new script instead:")
print("   python scripts/transfer_books_kuzu.py --help")
print("")
print("This old script has been preserved for reference but")
print("will not work with the current system.")
exit(1)

# === OBSOLETE CODE BELOW (PRESERVED FOR REFERENCE) ===

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
import redis
from app.infrastructure.redis_graph import RedisGraphConnection
from app.infrastructure.redis_repositories import RedisBookRepository, RedisUserRepository
from app.infrastructure.graph_storage import RedisGraphStorage
from config import Config

async def transfer_books(from_user_id: str, to_user_id: str):
    """Transfer all books from one user to another."""
    
    # Setup Redis connection
    redis_connection = RedisGraphConnection(Config.REDIS_URL)
    graph_store = RedisGraphStorage(redis_connection)
    book_repo = RedisBookRepository(graph_store)
    user_repo = RedisUserRepository(graph_store)
    
    print(f"🔄 Transferring books from '{from_user_id}' to '{to_user_id}'...")
    
    # Check that both users exist
    from_user = await user_repo.get_by_id(from_user_id)
    to_user = await user_repo.get_by_id(to_user_id)
    
    if not from_user:
        print(f"❌ Source user '{from_user_id}' not found!")
        return False
        
    if not to_user:
        print(f"❌ Target user '{to_user_id}' not found!")
        return False
    
    print(f"✅ Found source user: {from_user.username} ({from_user.id})")
    print(f"✅ Found target user: {to_user.username} ({to_user.id})")
    
    # Get all books for the source user
    # This is a simplified approach - you might need to adjust based on your actual repository methods
    try:
        # Use Redis directly to find and update relationships
        redis_client = redis.from_url(Config.REDIS_URL)
        
        # Find all keys related to the source user
        pattern = f"*{from_user_id}*"
        keys = redis_client.keys(pattern)
        
        print(f"🔍 Found {len(keys)} keys related to source user")
        
        transferred = 0
        for key in keys:
            key_str = key.decode('utf-8')
            
            # Update user-book relationships
            if 'relationship' in key_str and 'book' in key_str:
                # Get the relationship data
                data = redis_client.hgetall(key)
                if data:
                    # Update the user_id in the relationship
                    decoded_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()}
                    if 'user_id' in decoded_data:
                        decoded_data['user_id'] = to_user_id
                        
                        # Create new key with target user ID
                        new_key = key_str.replace(from_user_id, to_user_id)
                        
                        # Save with new key
                        redis_client.hset(new_key, mapping=decoded_data)
                        
                        # Delete old key
                        redis_client.delete(key)
                        
                        transferred += 1
                        print(f"  ✅ Transferred relationship: {key_str}")
        
        print(f"🎉 Successfully transferred {transferred} book relationships!")
        print(f"📚 Books are now associated with user: {to_user.username}")
        return True
        
    except Exception as e:
        print(f"❌ Error during transfer: {e}")
        return False

def main():
    """Main CLI interface."""
    if len(sys.argv) != 3:
        print("Usage: python transfer_books.py <from_user_id> <to_user_id>")
        print("Example: python transfer_books.py user1 74d6e5ea-1e61-4d59-bd8f-737ab4a705a8")
        sys.exit(1)
    
    from_user_id = sys.argv[1]
    to_user_id = sys.argv[2]
    
    success = asyncio.run(transfer_books(from_user_id, to_user_id))
    
    if success:
        print("\n✅ Transfer completed successfully!")
        print("🔄 Restart your application to see the changes.")
    else:
        print("\n❌ Transfer failed!")
        sys.exit(1)

if __name__ == '__main__':
    main()
