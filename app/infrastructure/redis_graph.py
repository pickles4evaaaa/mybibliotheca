"""
Redis graph database connection and configuration.

Handles Redis connection setup, connection pooling, and basic graph operations
using Redis as the graph database backend.
"""

import os
import json
import redis
import logging
from typing import Optional, Dict, Any, List
from dataclasses import asdict
from datetime import datetime, date

from ..domain.models import Book, User, Author


logger = logging.getLogger(__name__)


class RedisGraphConnection:
    """Redis connection manager for graph operations."""
    
    def __init__(self, redis_url: str = None, password: str = None):
        self.redis_url = redis_url or os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        self.password = password
        self._client = None
        self._connection_pool = None
        
    def connect(self) -> redis.Redis:
        """Establish Redis connection."""
        if self._client is None:
            try:
                # Parse Redis URL
                if '://' in self.redis_url:
                    self._client = redis.from_url(self.redis_url, decode_responses=True)
                else:
                    # Fallback for simple connections
                    self._client = redis.Redis(
                        host='localhost',
                        port=6379,
                        db=0,
                        password=self.password,
                        decode_responses=True
                    )
                
                # Test connection
                self._client.ping()
                logger.info("Redis connection established successfully")
                
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")
                raise
                
        return self._client
    
    def disconnect(self):
        """Close Redis connection."""
        if self._client:
            self._client.close()
            self._client = None
            logger.info("Redis connection closed")
    
    @property
    def client(self) -> redis.Redis:
        """Get Redis client, connecting if needed."""
        if self._client is None:
            return self.connect()
        return self._client


class RedisGraphStorage:
    """Redis-based graph storage implementation.
    
    Uses Redis JSON documents for nodes and Redis Sets for relationships.
    This approach provides the graph capabilities we need while leveraging
    Redis's mature ecosystem and performance.
    """
    
    def __init__(self, connection: RedisGraphConnection):
        self.connection = connection
        self.redis = connection.client
        
    # Node Operations
    
    def store_node(self, node_type: str, node_id: str, data: Dict[str, Any]) -> bool:
        """Store a node as a JSON document."""
        try:
            key = f"node:{node_type}:{node_id}"
            # Add metadata
            data['_type'] = node_type
            data['_id'] = node_id
            data['_created_at'] = data.get('_created_at', datetime.utcnow().isoformat())
            data['_updated_at'] = datetime.utcnow().isoformat()
            
            # Store as JSON
            self.redis.json().set(key, '$', data)
            
            # Add to type index
            self.redis.sadd(f"index:type:{node_type}", node_id)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to store node {node_type}:{node_id}: {e}")
            return False
    
    def get_node(self, node_type: str, node_id: str) -> Optional[Dict[str, Any]]:
        """Get a node by type and ID."""
        try:
            key = f"node:{node_type}:{node_id}"
            data = self.redis.json().get(key, '$')
            if data and isinstance(data, list) and len(data) > 0:
                return data[0]
            return None
            
        except Exception as e:
            logger.error(f"Failed to get node {node_type}:{node_id}: {e}")
            return None
    
    def update_node(self, node_type: str, node_id: str, updates: Dict[str, Any]) -> bool:
        """Update specific fields of a node."""
        try:
            key = f"node:{node_type}:{node_id}"
            
            # Check if node exists
            if not self.redis.exists(key):
                return False
                
            # Update timestamp
            updates['_updated_at'] = datetime.utcnow().isoformat()
            
            # Recursively serialize any datetime/date objects in updates
            def serialize_value(value):
                if isinstance(value, datetime):
                    return value.isoformat()
                elif isinstance(value, date):
                    return value.isoformat()
                elif isinstance(value, dict):
                    return {k: serialize_value(v) for k, v in value.items()}
                elif isinstance(value, list):
                    return [serialize_value(item) for item in value]
                else:
                    return value
            
            serialized_updates = {}
            for field, value in updates.items():
                serialized_updates[field] = serialize_value(value)
            
            # Update fields
            for field, value in serialized_updates.items():
                self.redis.json().set(key, f'$.{field}', value)
                
            return True
            
        except Exception as e:
            logger.error(f"Failed to update node {node_type}:{node_id}: {e}")
            return False
    
    def delete_node(self, node_type: str, node_id: str) -> bool:
        """Delete a node and its relationships."""
        try:
            key = f"node:{node_type}:{node_id}"
            
            # Remove from type index
            self.redis.srem(f"index:type:{node_type}", node_id)
            
            # Delete node
            result = self.redis.delete(key)
            
            # TODO: Clean up relationships (will implement when needed)
            
            return result > 0
            
        except Exception as e:
            logger.error(f"Failed to delete node {node_type}:{node_id}: {e}")
            return False
    
    def find_nodes_by_type(self, node_type: str, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Find all nodes of a specific type."""
        try:
            # Get node IDs from type index
            node_ids = list(self.redis.smembers(f"index:type:{node_type}"))
            
            # Apply pagination
            paginated_ids = node_ids[offset:offset + limit]
            
            # Get node data
            nodes = []
            for node_id in paginated_ids:
                node_data = self.get_node(node_type, node_id)
                if node_data:
                    nodes.append(node_data)
                    
            return nodes
            
        except Exception as e:
            logger.error(f"Failed to find nodes by type {node_type}: {e}")
            return []
    
    # Relationship Operations
    
    def create_relationship(self, from_type: str, from_id: str, relationship: str, 
                          to_type: str, to_id: str, properties: Dict[str, Any] = None) -> bool:
        """Create a relationship between two nodes."""
        try:
            # Store forward relationship
            forward_key = f"rel:{from_type}:{from_id}:{relationship}"
            rel_data = {
                'to_type': to_type,
                'to_id': to_id,
                'properties': properties or {},
                'created_at': datetime.utcnow().isoformat()
            }
            self.redis.sadd(forward_key, json.dumps(rel_data))
            
            # Store reverse relationship for efficient traversal
            reverse_key = f"rel_reverse:{to_type}:{to_id}:{relationship}"
            reverse_data = {
                'from_type': from_type,
                'from_id': from_id,
                'properties': properties or {},
                'created_at': datetime.utcnow().isoformat()
            }
            self.redis.sadd(reverse_key, json.dumps(reverse_data))
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to create relationship {from_type}:{from_id} -[{relationship}]-> {to_type}:{to_id}: {e}")
            return False
    
    def get_relationships(self, from_type: str, from_id: str, relationship: str = None) -> List[Dict[str, Any]]:
        """Get outgoing relationships from a node."""
        try:
            if relationship:
                keys = [f"rel:{from_type}:{from_id}:{relationship}"]
            else:
                # Get all relationship types for this node
                pattern = f"rel:{from_type}:{from_id}:*"
                keys = self.redis.keys(pattern)
            
            relationships = []
            for key in keys:
                rel_strings = self.redis.smembers(key)
                for rel_string in rel_strings:
                    try:
                        rel_data = json.loads(rel_string)
                        # Extract relationship type from key
                        rel_type = key.split(':')[-1]
                        rel_data['relationship'] = rel_type
                        relationships.append(rel_data)
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid relationship data in {key}: {rel_string}")
                        
            return relationships
            
        except Exception as e:
            logger.error(f"Failed to get relationships for {from_type}:{from_id}: {e}")
            return []
    
    def delete_relationship(self, from_type: str, from_id: str, relationship: str,
                          to_type: str, to_id: str) -> bool:
        """Delete a specific relationship."""
        try:
            # Create relationship data to match
            rel_data = {
                'to_type': to_type,
                'to_id': to_id,
                'properties': {},  # We'll need to handle this better
                'created_at': ''   # We'll need to handle this better
            }
            
            # Remove forward relationship
            forward_key = f"rel:{from_type}:{from_id}:{relationship}"
            # Note: This is a simplified approach. In production, we'd need better matching
            # For now, we'll remove all relationships to this target
            rel_strings = self.redis.smembers(forward_key)
            for rel_string in rel_strings:
                try:
                    data = json.loads(rel_string)
                    if data['to_type'] == to_type and data['to_id'] == to_id:
                        self.redis.srem(forward_key, rel_string)
                except json.JSONDecodeError:
                    continue
            
            # Remove reverse relationship
            reverse_key = f"rel_reverse:{to_type}:{to_id}:{relationship}"
            rel_strings = self.redis.smembers(reverse_key)
            for rel_string in rel_strings:
                try:
                    data = json.loads(rel_string)
                    if data['from_type'] == from_type and data['from_id'] == from_id:
                        self.redis.srem(reverse_key, rel_string)
                except json.JSONDecodeError:
                    continue
                    
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete relationship {from_type}:{from_id} -[{relationship}]-> {to_type}:{to_id}: {e}")
            return False
    
    # Search Operations
    
    def search_nodes(self, node_type: str, search_fields: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Simple search implementation (will be enhanced later)."""
        try:
            # For now, get all nodes and filter in Python
            # In production, we'd use Redis Search module or better indexing
            all_nodes = self.find_nodes_by_type(node_type)
            
            results = []
            for node in all_nodes:
                match = True
                for field, value in search_fields.items():
                    node_value = node.get(field)
                    if isinstance(value, str) and isinstance(node_value, str):
                        if value.lower() not in node_value.lower():
                            match = False
                            break
                    elif node_value != value:
                        match = False
                        break
                        
                if match:
                    results.append(node)
                    
            return results
            
        except Exception as e:
            logger.error(f"Failed to search nodes: {e}")
            return []
    
    # Utility Operations
    
    def health_check(self) -> Dict[str, Any]:
        """Check Redis connection and basic operations."""
        try:
            # Test basic operations
            self.redis.ping()
            
            # Get some basic stats
            info = self.redis.info()
            memory_usage = info.get('used_memory_human', 'unknown')
            connected_clients = info.get('connected_clients', 'unknown')
            
            return {
                'status': 'healthy',
                'memory_usage': memory_usage,
                'connected_clients': connected_clients,
                'redis_version': info.get('redis_version', 'unknown')
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e)
            }
    
    def clear_all_data(self):
        """Clear all data (for testing only)."""
        if os.getenv('FLASK_DEBUG') == 'true':
            self.redis.flushdb()
            logger.warning("All Redis data cleared (debug mode)")
        else:
            logger.error("clear_all_data() only allowed in debug mode")
    
    # Utility Methods for Services
    
    async def scan_keys(self, pattern: str) -> List[str]:
        """Scan for keys matching a pattern."""
        try:
            keys = []
            cursor = 0
            while True:
                cursor, batch = self.redis.scan(cursor=cursor, match=pattern, count=1000)
                keys.extend(batch)
                if cursor == 0:
                    break
            return keys
        except Exception as e:
            logger.error(f"Failed to scan keys with pattern {pattern}: {e}")
            return []
    
    async def set_json(self, key: str, data: Dict[str, Any]) -> bool:
        """Set JSON data at key."""
        try:
            self.redis.json().set(key, '$', data)
            return True
        except Exception as e:
            logger.error(f"Failed to set JSON at key {key}: {e}")
            return False
    
    async def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        """Get JSON data from Redis key (async wrapper)."""
        try:
            # Use JSON.GET for RedisJSON keys
            data = self.redis.json().get(key, '$')
            if data and len(data) > 0:
                return data[0]  # JSON.GET with '$' returns a list
            return None
        except Exception as e:
            logger.error(f"Failed to get JSON from key {key}: {e}")
            return None
    
    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Alias for get_json for compatibility."""
        return await self.get_json(key)
    
    # Sorted Set Operations for Reading Logs
    
    async def add_to_sorted_set(self, key: str, member: str, score: float) -> bool:
        """Add a member to a sorted set with a score."""
        try:
            self.redis.zadd(key, {member: score})
            return True
        except Exception as e:
            logger.error(f"Failed to add to sorted set {key}: {e}")
            return False
    
    async def get_sorted_set_size(self, key: str) -> int:
        """Get the size of a sorted set."""
        try:
            return self.redis.zcard(key)
        except Exception as e:
            logger.error(f"Failed to get sorted set size for {key}: {e}")
            return 0
    
    async def get_sorted_set_range_by_score(self, key: str, min_score: float, max_score: float, limit: int = None) -> List[str]:
        """Get members of a sorted set within a score range."""
        try:
            kwargs = {}
            if limit is not None:
                kwargs['start'] = 0
                kwargs['num'] = limit
            return self.redis.zrangebyscore(key, min_score, max_score, **kwargs)
        except Exception as e:
            logger.error(f"Failed to get sorted set range by score for {key}: {e}")
            return []
    
    async def get_sorted_set_range(self, key: str, start: int = 0, end: int = -1) -> List[str]:
        """Get members of a sorted set by rank."""
        try:
            return self.redis.zrange(key, start, end)
        except Exception as e:
            logger.error(f"Failed to get sorted set range for {key}: {e}")
            return []
    
    async def delete_key(self, key: str) -> bool:
        """Delete a key from Redis."""
        try:
            result = self.redis.delete(key)
            return result > 0
        except Exception as e:
            logger.error(f"Failed to delete key {key}: {e}")
            return False
    
    async def exists(self, key: str) -> bool:
        """Check if key exists (async wrapper)."""
        try:
            return bool(self.redis.exists(key))
        except Exception as e:
            logger.error(f"Failed to check existence of key {key}: {e}")
            return False


# Global connection instance
_redis_connection = None


def get_redis_connection() -> RedisGraphConnection:
    """Get the global Redis connection instance."""
    global _redis_connection
    if _redis_connection is None:
        _redis_connection = RedisGraphConnection()
    return _redis_connection


def get_graph_storage() -> RedisGraphStorage:
    """Get a Redis graph storage instance."""
    connection = get_redis_connection()
    return RedisGraphStorage(connection)
