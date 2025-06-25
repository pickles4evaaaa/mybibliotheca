"""
Kuzu graph database connection and configuration.

Handles Kuzu connection set            # Try to drop existing tables (ignore errors if they don't exist)
            drop_tables = ["OWNS", "WROTE", "PUBLISHED", "CATEGORIZED_AS", "IN_SERIES", "LOGGED", 
                          "Book", "User", "Author", "Publisher", "Category", "Series", "ReadingLog", 
                          "Location", "ImportMapping"]and basic graph operations using Kuzu as the graph database backend.
Kuzu is an embedded graph database optimized for complex graph analytics and queries.
"""

import os
import json
import kuzu
import logging
from typing import Optional, Dict, Any, List, Union
from dataclasses import asdict
from datetime import datetime, date
from pathlib import Path

from ..domain.models import Book, User, Author


logger = logging.getLogger(__name__)


def serialize_datetime_values(data: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively serialize datetime objects in a dictionary to ISO format strings."""
    if not isinstance(data, dict):
        return data
    
    serialized = {}
    for key, value in data.items():
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
        elif isinstance(value, date):
            serialized[key] = value.isoformat()
        elif isinstance(value, dict):
            serialized[key] = serialize_datetime_values(value)
        elif isinstance(value, list):
            serialized[key] = [serialize_datetime_values(item) if isinstance(item, dict) else item for item in value]
        else:
            serialized[key] = value
    return serialized


class KuzuGraphConnection:
    """Kuzu connection manager for graph operations."""
    
    def __init__(self, database_path: str = None):
        self.database_path = database_path or os.getenv('KUZU_DB_PATH', 'data/kuzu_db')
        self._database = None
        self._connection = None
        self._is_initialized = False
        
    def connect(self) -> kuzu.Connection:
        """Establish Kuzu connection."""
        if self._connection is None:
            try:
                # Ensure database directory exists
                Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
                
                # Create database and connection
                self._database = kuzu.Database(self.database_path)
                self._connection = kuzu.Connection(self._database)
                
                # Initialize schema if not already done
                if not self._is_initialized:
                    self._initialize_schema()
                    self._is_initialized = True
                
                logger.info(f"Kuzu connection established at {self.database_path}")
                
            except Exception as e:
                logger.error(f"Failed to connect to Kuzu: {e}")
                raise
                
        return self._connection
    
    def _initialize_schema(self):
        """Initialize the graph schema with node and relationship tables."""
        try:
            # Skip if already initialized
            if self._is_initialized:
                return
                
            # Try to drop existing tables (ignore errors if they don't exist)
            drop_tables = ["OWNS", "WROTE", "PUBLISHED", "CATEGORIZED_AS", "IN_SERIES", "LOGGED", 
                          "Book", "User", "Author", "Publisher", "Category", "Series", "ReadingLog", "Location", "ImportMapping"]
            
            for table in drop_tables:
                try:
                    self._connection.execute(f"DROP TABLE {table}")
                except:
                    pass  # Ignore errors if table doesn't exist
            
            # Create node tables
            node_queries = [
                """
                CREATE NODE TABLE User(
                    id STRING,
                    username STRING,
                    email STRING,
                    password_hash STRING,
                    share_current_reading BOOLEAN,
                    share_reading_activity BOOLEAN,
                    share_library BOOLEAN,
                    is_admin BOOLEAN,
                    is_active BOOLEAN,
                    password_must_change BOOLEAN,
                    failed_login_attempts INT64,
                    reading_streak_offset INT64,
                    timezone STRING,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE Book(
                    id STRING,
                    title STRING,
                    subtitle STRING,
                    isbn STRING,
                    isbn13 STRING,
                    description STRING,
                    published_date DATE,
                    page_count INT64,
                    cover_url STRING,
                    language STRING,
                    average_rating DOUBLE,
                    rating_count INT64,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE Author(
                    id STRING,
                    name STRING,
                    normalized_name STRING,
                    biography STRING,
                    birth_date DATE,
                    death_date DATE,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE Publisher(
                    id STRING,
                    name STRING,
                    normalized_name STRING,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE Category(
                    id STRING,
                    name STRING,
                    normalized_name STRING,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE Series(
                    id STRING,
                    name STRING,
                    normalized_name STRING,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE ReadingLog(
                    id STRING,
                    date DATE,
                    notes STRING,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE Location(
                    id STRING,
                    user_id STRING,
                    name STRING,
                    description STRING,
                    location_type STRING,
                    address STRING,
                    is_default BOOLEAN,
                    is_active BOOLEAN,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE ImportMapping(
                    id STRING,
                    name STRING,
                    description STRING,
                    user_id STRING,
                    is_system BOOLEAN,
                    source_type STRING,
                    field_mappings STRING,
                    sample_headers STRING,
                    is_shareable BOOLEAN,
                    usage_count INT64,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """
            ]
            
            # Create relationship tables
            relationship_queries = [
                """
                CREATE REL TABLE OWNS(
                    FROM User TO Book,
                    reading_status STRING,
                    ownership_status STRING,
                    user_rating DOUBLE,
                    user_review STRING,
                    personal_notes STRING,
                    reading_start_date DATE,
                    reading_end_date DATE,
                    locations STRING[],
                    custom_metadata MAP(STRING, STRING),
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE WROTE(
                    FROM Author TO Book,
                    contribution_type STRING,
                    role STRING,
                    created_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE PUBLISHED(
                    FROM Publisher TO Book,
                    publication_date DATE,
                    created_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE CATEGORIZED_AS(
                    FROM Book TO Category,
                    created_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE IN_SERIES(
                    FROM Book TO Series,
                    volume_number INT64,
                    created_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE LOGGED(
                    FROM User TO ReadingLog,
                    book_id STRING,
                    created_at TIMESTAMP
                )
                """
            ]
            
            # Execute all creation queries with individual error handling
            all_queries = node_queries + relationship_queries
            for i, query in enumerate(all_queries):
                try:
                    self._connection.execute(query)
                    logger.debug(f"Successfully executed query {i+1}/{len(all_queries)}")
                except Exception as e:
                    # Check if it's a "already exists" error and skip if so
                    if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
                        logger.debug(f"Table/relationship already exists, skipping query {i+1}")
                        continue
                    else:
                        logger.error(f"Failed to execute query {i+1}: {e}")
                        raise
                
            logger.info("Kuzu schema initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Kuzu schema: {e}")
            raise
    
    def disconnect(self):
        """Close Kuzu connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Kuzu connection closed")
    
    @property
    def connection(self) -> kuzu.Connection:
        """Get Kuzu connection, connecting if needed."""
        if self._connection is None:
            return self.connect()
        return self._connection


class KuzuGraphStorage:
    """Kuzu-based graph storage implementation.
    
    Uses Kuzu's native graph database capabilities for nodes and relationships.
    This provides true graph query capabilities with Cypher-like syntax.
    """
    
    def __init__(self, connection: KuzuGraphConnection):
        self.connection = connection
        self.kuzu_conn = connection.connection
    
    # Node Operations
    
    def store_node(self, node_type: str, node_id: str, data: Dict[str, Any]) -> bool:
        """Store a node in Kuzu."""
        try:
            # Print debug info for TIMESTAMP fields
            for ts_field in ['created_at', 'updated_at']:
                if ts_field in data:
                    print(f"[KUZU_GRAPH][DEBUG] {ts_field} before serialization: {data[ts_field]} (type: {type(data[ts_field])})")
            # Serialize any datetime objects in data (should not convert TIMESTAMP fields to string for Kuzu)
            serialized_data = data.copy()
            # Add metadata
            serialized_data['id'] = node_id
            if not serialized_data.get('created_at'):
                serialized_data['created_at'] = datetime.utcnow()
            if not serialized_data.get('updated_at'):
                serialized_data['updated_at'] = datetime.utcnow()
            # Print debug info after ensuring datetime
            for ts_field in ['created_at', 'updated_at']:
                if ts_field in serialized_data:
                    print(f"[KUZU_GRAPH][DEBUG] {ts_field} for insertion: {serialized_data[ts_field]} (type: {type(serialized_data[ts_field])})")
            
            # Build parameter list for the query
            columns = list(serialized_data.keys())
            placeholders = [f"${col}" for col in columns]
            query = f"""
            CREATE (n:{node_type} {{
                {', '.join(f"{col}: ${col}" for col in columns)}
            }})
            """
            self.kuzu_conn.execute(query, serialized_data)
            return True
            
        except Exception as e:
            logger.error(f"Failed to store {node_type} node {node_id}: {e}")
            return False
    
    def get_node(self, node_type: str, node_id: str) -> Optional[Dict[str, Any]]:
        """Get a node by type and ID."""
        try:
            query = f"MATCH (n:{node_type}) WHERE n.id = $node_id RETURN n"
            result = self.kuzu_conn.execute(query, {"node_id": node_id})
            
            if result.has_next():
                node_data = result.get_next()[0]
                return dict(node_data)
            return None
            
        except Exception as e:
            logger.error(f"Failed to get {node_type} node {node_id}: {e}")
            return None
    
    def update_node(self, node_type: str, node_id: str, updates: Dict[str, Any]) -> bool:
        """Update specific fields of a node."""
        try:
            # Serialize updates
            serialized_updates = serialize_datetime_values(updates)
            serialized_updates['updated_at'] = datetime.utcnow().isoformat()
            
            # Build SET clause
            set_clauses = [f"n.{key} = ${key}" for key in serialized_updates.keys()]
            
            query = f"""
            MATCH (n:{node_type}) 
            WHERE n.id = $node_id 
            SET {', '.join(set_clauses)}
            """
            
            params = {"node_id": node_id, **serialized_updates}
            self.kuzu_conn.execute(query, params)
            return True
            
        except Exception as e:
            logger.error(f"Failed to update {node_type} node {node_id}: {e}")
            return False
    
    def delete_node(self, node_type: str, node_id: str) -> bool:
        """Delete a node and its relationships."""
        try:
            query = f"""
            MATCH (n:{node_type}) 
            WHERE n.id = $node_id 
            DETACH DELETE n
            """
            
            self.kuzu_conn.execute(query, {"node_id": node_id})
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete {node_type} node {node_id}: {e}")
            return False
    
    def get_nodes_by_type(self, node_type: str, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Get all nodes of a specific type with pagination."""
        try:
            query = f"""
            MATCH (n:{node_type}) 
            RETURN n 
            SKIP {offset} LIMIT {limit}
            """
            
            result = self.kuzu_conn.execute(query)
            nodes = []
            while result.has_next():
                node_data = result.get_next()[0]
                nodes.append(dict(node_data))
            return nodes
            
        except Exception as e:
            logger.error(f"Failed to get {node_type} nodes: {e}")
            return []
    
    def find_nodes_by_type(self, node_type: str, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Alias for get_nodes_by_type to maintain compatibility."""
        return self.get_nodes_by_type(node_type, limit, offset)
    
    # Relationship Operations
    
    def create_relationship(self, from_type: str, from_id: str, rel_type: str, to_type: str, to_id: str, properties: Dict[str, Any] = None) -> bool:
        """Create a relationship between two nodes."""
        try:
            properties = properties or {}
            serialized_props = serialize_datetime_values(properties)
            serialized_props['created_at'] = serialized_props.get('created_at', datetime.utcnow().isoformat())
            
            # Build properties clause
            if serialized_props:
                prop_clauses = [f"{key}: ${key}" for key in serialized_props.keys()]
                props_str = f"{{ {', '.join(prop_clauses)} }}"
            else:
                props_str = ""
            
            query = f"""
            MATCH (from:{from_type}), (to:{to_type})
            WHERE from.id = $from_id AND to.id = $to_id
            CREATE (from)-[r:{rel_type} {props_str}]->(to)
            """
            
            params = {"from_id": from_id, "to_id": to_id, **serialized_props}
            self.kuzu_conn.execute(query, params)
            return True
            
        except Exception as e:
            logger.error(f"Failed to create relationship {from_id} -{rel_type}-> {to_id}: {e}")
            return False
    
    def get_relationships(self, from_type: str, from_id: str, rel_type: str = None) -> List[Dict[str, Any]]:
        """Get all relationships from a node."""
        try:
            if rel_type:
                query = f"""
                MATCH (from:{from_type})-[r:{rel_type}]->(to)
                WHERE from.id = $from_id
                RETURN r, to
                """
            else:
                query = f"""
                MATCH (from:{from_type})-[r]->(to)
                WHERE from.id = $from_id
                RETURN r, to
                """
            
            result = self.kuzu_conn.execute(query, {"from_id": from_id})
            relationships = []
            while result.has_next():
                row = result.get_next()
                rel_data = dict(row[0])
                to_data = dict(row[1])
                relationships.append({
                    "relationship": rel_data,
                    "target": to_data
                })
            return relationships
            
        except Exception as e:
            logger.error(f"Failed to get relationships for {from_id}: {e}")
            return []
    
    def delete_relationship(self, from_type: str, from_id: str, rel_type: str, to_type: str, to_id: str) -> bool:
        """Delete a specific relationship."""
        try:
            query = f"""
            MATCH (from:{from_type})-[r:{rel_type}]->(to:{to_type})
            WHERE from.id = $from_id AND to.id = $to_id
            DELETE r
            """
            
            self.kuzu_conn.execute(query, {"from_id": from_id, "to_id": to_id})
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete relationship {from_id} -{rel_type}-> {to_id}: {e}")
            return False
    
    # Advanced Graph Queries
    
    def execute_cypher(self, query: str, parameters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Execute a custom Cypher query."""
        try:
            parameters = parameters or {}
            result = self.kuzu_conn.execute(query, parameters)
            
            rows = []
            while result.has_next():
                row = result.get_next()
                # Convert result to dictionary format
                rows.append({f"col_{i}": val for i, val in enumerate(row)})
            return rows
            
        except Exception as e:
            logger.error(f"Failed to execute Cypher query: {e}")
            return []
    
    def count_nodes(self, node_type: str) -> int:
        """Count nodes of a specific type."""
        try:
            query = f"MATCH (n:{node_type}) RETURN COUNT(n) as count"
            result = self.kuzu_conn.execute(query)
            if result.has_next():
                return result.get_next()[0]
            return 0
        except Exception as e:
            logger.error(f"Failed to count {node_type} nodes: {e}")
            return 0
    
    def count_relationships(self, rel_type: str) -> int:
        """Count relationships of a specific type."""
        try:
            query = f"MATCH ()-[r:{rel_type}]->() RETURN COUNT(r) as count"
            result = self.kuzu_conn.execute(query)
            if result.has_next():
                return result.get_next()[0]
            return 0
        except Exception as e:
            logger.error(f"Failed to count {rel_type} relationships: {e}")
            return 0


# Global connection instance
_kuzu_connection = None


def get_kuzu_connection() -> KuzuGraphConnection:
    """Get the global Kuzu connection instance."""
    global _kuzu_connection
    if _kuzu_connection is None:
        _kuzu_connection = KuzuGraphConnection()
    return _kuzu_connection


def get_graph_storage() -> KuzuGraphStorage:
    """Get a Kuzu graph storage instance."""
    connection = get_kuzu_connection()
    return KuzuGraphStorage(connection)
