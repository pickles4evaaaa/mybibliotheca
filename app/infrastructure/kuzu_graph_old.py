"""
Kuzu Graph Database - Clean Architecture

A simplified, graph-native design that leverages Kuzu's strengths.
Focus on simple nodes and clear relationships.
"""

import os
import json
import kuzu
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, date
from pathlib import Path

logger = logging.getLogger(__name__)


class KuzuGraphDB:
    """Simplified Kuzu graph database with clean schema design."""
    
    def __init__(self, database_path: str = None):
        self.database_path = database_path or os.getenv('KUZU_DB_PATH', 'data/kuzu')
        self._database = None
        self._connection = None
        
    def connect(self) -> kuzu.Connection:
        """Establish connection and initialize schema."""
        if self._connection is None:
            try:
                Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
                self._database = kuzu.Database(self.database_path)
                self._connection = kuzu.Connection(self._database)
                self._initialize_schema()
                logger.info(f"Kuzu connected at {self.database_path}")
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
            
            # Check environment variable for forced reset
            force_reset = os.getenv('KUZU_FORCE_RESET', 'false').lower() == 'true'
            
            if not force_reset:
                # Check if database already has User table (indicating it's been initialized)
                try:
                    result = self._connection.execute("MATCH (u:User) RETURN COUNT(u) as count LIMIT 1")
                    if result.has_next():
                        user_count = result.get_next()[0]
                        if user_count > 0:
                            logger.info(f"🗄️ Database already contains {user_count} users - skipping schema initialization")
                            self._is_initialized = True
                            return
                        else:
                            logger.info("🗄️ Database exists but is empty - will initialize schema")
                    else:
                        logger.info("🗄️ Database is empty - will initialize schema")
                except Exception as e:
                    # If User table doesn't exist, we need to initialize schema
                    logger.info(f"🗄️ User table doesn't exist - will initialize schema: {e}")
            else:
                logger.warning("⚠️ KUZU_FORCE_RESET=true - forcing schema reset (all data will be lost)")
            
            # Only drop and recreate if we're really starting fresh
            # This should only happen on first run or if explicitly requested
            logger.info("🔧 Initializing fresh Kuzu schema...")
            
            # Try to drop existing tables (ignore errors if they don't exist)
            drop_tables = ["OWNS", "WRITTEN_BY", "CONTRIBUTED", "AUTHORED", "PUBLISHED_BY", "PUBLISHED", "CATEGORIZED_AS", "PART_OF_SERIES", "IN_SERIES", "LOGGED", "PARENT_CATEGORY",
                          "Book", "User", "Author", "Person", "Publisher", "Category", "Series", "ReadingLog", 
                          "Location", "ImportMapping", "ImportJob", "CustomFieldDefinition", "ImportTask"]
            
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
                    locked_until TIMESTAMP,
                    last_login TIMESTAMP,
                    password_changed_at TIMESTAMP,
                    reading_streak_offset INT64,
                    timezone STRING,
                    display_name STRING,
                    bio STRING,
                    location STRING,
                    website STRING,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE Book(
                    id STRING,
                    title STRING,
                    normalized_title STRING,
                    subtitle STRING,
                    isbn13 STRING,
                    isbn10 STRING,
                    asin STRING,
                    description STRING,
                    published_date DATE,
                    page_count INT64,
                    language STRING,
                    cover_url STRING,
                    google_books_id STRING,
                    openlibrary_id STRING,
                    average_rating DOUBLE,
                    rating_count INT64,
                    series_volume STRING,
                    series_order INT64,
                    custom_metadata STRING,
                    raw_categories STRING,
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
                    birth_year INT64,
                    death_year INT64,
                    bio STRING,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE Person(
                    id STRING,
                    name STRING,
                    normalized_name STRING,
                    birth_year INT64,
                    death_year INT64,
                    birth_place STRING,
                    bio STRING,
                    website STRING,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE Publisher(
                    id STRING,
                    name STRING,
                    normalized_name STRING,
                    founded_year INT64,
                    country STRING,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE Category(
                    id STRING,
                    name STRING,
                    normalized_name STRING,
                    parent_id STRING,
                    description STRING,
                    level INT64,
                    color STRING,
                    icon STRING,
                    aliases STRING,
                    book_count INT64,
                    user_book_count INT64,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE Series(
                    id STRING,
                    name STRING,
                    normalized_name STRING,
                    description STRING,
                    total_books INT64,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE ReadingLog(
                    id STRING,
                    user_id STRING,
                    book_id STRING,
                    date DATE,
                    pages_read INT64,
                    minutes_read INT64,
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
                CREATE NODE TABLE ImportJob(
                    id STRING,
                    task_id STRING,
                    user_id STRING,
                    csv_file_path STRING,
                    field_mappings STRING,
                    default_reading_status STRING,
                    duplicate_handling STRING,
                    custom_fields_enabled BOOLEAN,
                    status STRING,
                    processed INT64,
                    success INT64,
                    errors INT64,
                    total INT64,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    current_book STRING,
                    error_messages STRING,
                    recent_activity STRING,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    expires_at TIMESTAMP,
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
                """,
                """
                CREATE NODE TABLE CustomFieldDefinition(
                    id STRING,
                    name STRING,
                    display_name STRING,
                    field_type STRING,
                    description STRING,
                    created_by_user_id STRING,
                    is_shareable BOOLEAN,
                    is_global BOOLEAN,
                    default_value STRING,
                    placeholder_text STRING,
                    help_text STRING,
                    predefined_options STRING,
                    allow_custom_options BOOLEAN,
                    rating_min INT64,
                    rating_max INT64,
                    rating_labels STRING,
                    usage_count INT64,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE ImportTask(
                    id STRING,
                    user_id STRING,
                    task_type STRING,
                    status STRING,
                    progress INT64,
                    total_items INT64,
                    processed_items INT64,
                    file_path STRING,
                    parameters STRING,
                    results STRING,
                    error_message STRING,
                    created_at TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE CustomField(
                    id STRING,
                    user_id STRING,
                    name STRING,
                    display_name STRING,
                    field_type STRING,
                    description STRING,
                    default_value STRING,
                    placeholder_text STRING,
                    help_text STRING,
                    predefined_options STRING,
                    allow_custom_options BOOLEAN,
                    rating_min INT64,
                    rating_max INT64,
                    rating_labels STRING,
                    is_required BOOLEAN,
                    is_active BOOLEAN,
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
                    date_added TIMESTAMP,
                    start_date TIMESTAMP,
                    finish_date TIMESTAMP,
                    ownership_status STRING,
                    media_type STRING,
                    borrowed_from STRING,
                    borrowed_from_user_id STRING,
                    borrowed_date TIMESTAMP,
                    borrowed_due_date TIMESTAMP,
                    loaned_to STRING,
                    loaned_to_user_id STRING,
                    loaned_date TIMESTAMP,
                    loaned_due_date TIMESTAMP,
                    primary_location_id STRING,
                    user_rating DOUBLE,
                    rating_date TIMESTAMP,
                    user_review STRING,
                    review_date TIMESTAMP,
                    is_review_spoiler BOOLEAN,
                    personal_notes STRING,
                    pace STRING,
                    character_driven BOOLEAN,
                    source STRING,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE WRITTEN_BY(
                    FROM Book TO Person,
                    contribution_type STRING,
                    role STRING,
                    created_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE AUTHORED(
                    FROM Person TO Book,
                    contribution_type STRING,
                    role STRING,
                    created_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE PUBLISHED_BY(
                    FROM Book TO Publisher,
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
                CREATE REL TABLE PART_OF_SERIES(
                    FROM Book TO Series,
                    volume_number INT64,
                    series_order INT64,
                    created_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE LOGGED(
                    FROM User TO ReadingLog,
                    book_id STRING,
                    created_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE PARENT_CATEGORY(
                    FROM Category TO Category,
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
                
            logger.info("✅ Kuzu schema initialized successfully")
            self._is_initialized = True
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kuzu schema: {e}")
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
            
            # Handle special field conversions for Kuzu
            serialized_data = data.copy()
            
            # Filter out relationship fields that shouldn't be stored directly
            # These are populated by service layer but shouldn't be stored in the database
            relationship_fields = ['parent', 'children', 'authors', 'contributors', 'publisher', 'series', 'categories']
            for field in relationship_fields:
                if field in serialized_data:
                    del serialized_data[field]
            
            # Filter out None values and empty strings that can cause ANY type errors in Kuzu
            clean_data = {}
            for k, v in serialized_data.items():
                if v is not None and v != '' and v != []:
                    clean_data[k] = v
                else:
                    logger.debug(f"[KUZU_GRAPH][DEBUG] Filtering out NULL/empty field {k}={v} for {node_type}")
            serialized_data = clean_data
            
            # Debug: Print what fields we're actually storing
            logger.info(f"[KUZU_GRAPH][DEBUG] Storing {node_type} with fields: {list(serialized_data.keys())}")
            
            # Handle published_date specifically - convert to date if needed
            if 'published_date' in serialized_data and serialized_data['published_date'] is not None:
                pub_date = serialized_data['published_date']
                if isinstance(pub_date, str):
                    try:
                        # Try parsing as ISO date
                        if 'T' in pub_date:
                            # Full datetime string, extract date part
                            parsed_dt = datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
                            serialized_data['published_date'] = parsed_dt.date()
                        else:
                            # Just date string
                            serialized_data['published_date'] = datetime.fromisoformat(pub_date).date()
                    except (ValueError, TypeError):
                        print(f"[KUZU_GRAPH][WARNING] Could not parse published_date: {pub_date}, setting to None")
                        serialized_data['published_date'] = None
                elif isinstance(pub_date, datetime):
                    # Convert datetime to date
                    serialized_data['published_date'] = pub_date.date()
                # If it's already a date object, leave it as is
            
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
            query = f"MATCH (n:{node_type}) WHERE n.id = $node_id RETURN n.id, n"
            result = self.kuzu_conn.execute(query, {"node_id": node_id})
            
            if result.has_next():
                row = result.get_next()
                returned_id = row[0]
                node_obj = row[1]
                
                try:
                    node_data = dict(node_obj)
                    # Ensure the ID is properly set
                    if 'id' not in node_data or node_data['id'] != returned_id:
                        node_data['id'] = returned_id
                    return node_data
                except Exception as conv_error:
                    logger.warning(f"Could not convert {node_type} node to dict: {conv_error}")
                    # Fallback: return minimal dict with ID
                    return {'id': returned_id}
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
            # Use RETURN n.* to get all properties as separate columns
            query = f"""
            MATCH (n:{node_type}) 
            RETURN n.id, n
            SKIP {offset} LIMIT {limit}
            """
            
            result = self.kuzu_conn.execute(query)
            nodes = []
            while result.has_next():
                row = result.get_next()
                node_id = row[0]  # The ID
                node_obj = row[1]  # The full node object
                
                try:
                    # Try to convert node object to dict
                    node_data = dict(node_obj)
                    # Ensure the ID is properly set
                    if 'id' not in node_data or node_data['id'] != node_id:
                        node_data['id'] = node_id
                    nodes.append(node_data)
                except Exception as conv_error:
                    logger.warning(f"Could not convert {node_type} node to dict: {conv_error}")
                    # Fallback: create a minimal dict with just the ID
                    nodes.append({'id': node_id})
                    
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
            
            # Debug: Print incoming properties
            logger.info(f"[KUZU_GRAPH][DEBUG] Creating {rel_type} relationship with properties: {properties}")
            
            # Convert ISO timestamp strings back to datetime objects for Kuzu
            processed_props = {}
            for key, value in properties.items():
                # Check if this looks like a datetime field and the value is a string
                is_datetime_field = (
                    key.endswith(('_at', '_date', '_time')) or 
                    key.startswith(('date_', 'time_')) or
                    key in ['date_added', 'date_finished', 'date_started', 'borrowed_date', 'loaned_date', 'borrowed_due_date', 'loaned_due_date', 'rating_date', 'review_date']
                )
                
                # Special handling for date-only fields (should be DATE type, not TIMESTAMP)
                is_date_only_field = key in ['publication_date']
                
                if isinstance(value, str) and (is_datetime_field or is_date_only_field) and value:
                    try:
                        # Try to parse ISO datetime strings back to datetime objects
                        if 'T' in value or value.count('-') >= 2:
                            # Looks like an ISO datetime string
                            if value.endswith('Z'):
                                value = value[:-1] + '+00:00'
                            parsed_datetime = datetime.fromisoformat(value)
                            
                            # For date-only fields, extract just the date part
                            if is_date_only_field:
                                processed_props[key] = parsed_datetime.date()
                                logger.info(f"[KUZU_GRAPH][DEBUG] Converted {key} from '{value}' to date: {parsed_datetime.date()}")
                            else:
                                processed_props[key] = parsed_datetime
                                logger.info(f"[KUZU_GRAPH][DEBUG] Converted {key} from '{value}' to datetime: {parsed_datetime}")
                        else:
                            processed_props[key] = value
                    except (ValueError, TypeError) as e:
                        # If parsing fails, keep as string
                        processed_props[key] = value
                        logger.warning(f"[KUZU_GRAPH][DEBUG] Failed to parse {key}='{value}' as datetime: {e}")
                else:
                    # Handle special data type conversions
                    if key == 'custom_metadata' and isinstance(value, dict):
                        # Convert dict to JSON string for storage as STRING
                        if not value:  # Empty dict
                            processed_props[key] = None
                        else:
                            # Store as JSON string
                            processed_props[key] = json.dumps(value)
                    elif key in ['locations', 'user_tags', 'moods'] and isinstance(value, list):
                        # Convert list to JSON string for storage as STRING
                        if not value:  # Empty list
                            processed_props[key] = None
                        else:
                            # Store as JSON string
                            processed_props[key] = json.dumps(value)
                    else:
                        processed_props[key] = value
            
            # Debug: Print processed properties
            logger.info(f"[KUZU_GRAPH][DEBUG] Processed properties: {processed_props}")
            
            # Filter out None values that can cause type issues
            processed_props = {k: v for k, v in processed_props.items() if v is not None}
            
            # Ensure created_at is set as datetime object
            if 'created_at' not in processed_props:
                processed_props['created_at'] = datetime.utcnow()
            elif isinstance(processed_props['created_at'], str):
                try:
                    if processed_props['created_at'].endswith('Z'):
                        processed_props['created_at'] = processed_props['created_at'][:-1] + '+00:00'
                    processed_props['created_at'] = datetime.fromisoformat(processed_props['created_at'])
                except (ValueError, TypeError):
                    processed_props['created_at'] = datetime.utcnow()
            
            # Build properties clause
            if processed_props:
                prop_clauses = [f"{key}: ${key}" for key in processed_props.keys()]
                props_str = f"{{ {', '.join(prop_clauses)} }}"
            else:
                props_str = ""
            
            query = f"""
            MATCH (from:{from_type}), (to:{to_type})
            WHERE from.id = $from_id AND to.id = $to_id
            CREATE (from)-[r:{rel_type} {props_str}]->(to)
            """
            
            params = {"from_id": from_id, "to_id": to_id, **processed_props}
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
