"""
Kuzu Graph Database Schema Redesign

A clean, graph-native design that leverages Kuzu's strengths.
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
        """Initialize the clean graph schema."""
        try:
            # Check if we need to force reset
            force_reset = os.getenv('KUZU_FORCE_RESET', 'false').lower() == 'true'
            
            if force_reset:
                logger.info("🔄 Force reset enabled - dropping all tables")
                self._drop_all_tables()
            else:
                # Check if schema already exists
                try:
                    result = self._connection.execute("MATCH (u:User) RETURN COUNT(u) LIMIT 1")
                    if result.has_next():
                        logger.info("📊 Schema already exists - skipping initialization")
                        return
                except:
                    pass  # Schema doesn't exist, continue with creation
            
            logger.info("🏗️ Creating clean Kuzu schema...")
            self._create_node_tables()
            self._create_relationship_tables()
            logger.info("✅ Schema created successfully")
            
        except Exception as e:
            logger.error(f"❌ Schema initialization failed: {e}")
            raise
    
    def _drop_all_tables(self):
        """Drop all existing tables for clean reset."""
        # Relationships first (to avoid dependency issues)
        rel_tables = [
            "ADMINISTERS", "AUTHORED", "CONTRIBUTED", "PUBLISHED_BY", "CATEGORIZED_AS", 
            "PART_OF_SERIES", "OWNS", "LOCATED_AT", "STARTED_READING", "FINISHED_READING",
            "RATED", "REVIEWED", "TAGGED", "NOTED", "BORROWED_FROM", "LOANED_TO",
            "SUBCATEGORY_OF", "HAS_CUSTOM_FIELD", "LOGGED_SESSION"
        ]
        
        # Node tables
        node_tables = [
            "User", "Person", "Book", "Publisher", "Category", "Series", "Location",
            "ReadingSession", "CustomField", "Tag", "Note", "Review", "Rating"
        ]
        
        for table in rel_tables + node_tables:
            try:
                self._connection.execute(f"DROP TABLE {table}")
            except:
                pass  # Ignore if doesn't exist
    
    def _create_node_tables(self):
        """Create all node tables with clean, simple schemas."""
        
        # Core entities
        node_schemas = {
            "User": """
                CREATE NODE TABLE User(
                    id STRING,
                    username STRING,
                    email STRING,
                    password_hash STRING,
                    display_name STRING,
                    bio STRING,
                    timezone STRING,
                    is_admin BOOLEAN,
                    is_active BOOLEAN,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
            """,
            
            "Person": """
                CREATE NODE TABLE Person(
                    id STRING,
                    name STRING,
                    normalized_name STRING,
                    birth_year INT64,
                    death_year INT64,
                    bio STRING,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
            """,
            
            "Book": """
                CREATE NODE TABLE Book(
                    id STRING,
                    title STRING,
                    normalized_title STRING,
                    isbn13 STRING,
                    isbn10 STRING,
                    description STRING,
                    published_date DATE,
                    page_count INT64,
                    language STRING,
                    cover_url STRING,
                    average_rating DOUBLE,
                    rating_count INT64,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
            """,
            
            "Publisher": """
                CREATE NODE TABLE Publisher(
                    id STRING,
                    name STRING,
                    country STRING,
                    founded_year INT64,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
            """,
            
            "Category": """
                CREATE NODE TABLE Category(
                    id STRING,
                    name STRING,
                    normalized_name STRING,
                    description STRING,
                    color STRING,
                    icon STRING,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
            """,
            
            "Series": """
                CREATE NODE TABLE Series(
                    id STRING,
                    name STRING,
                    description STRING,
                    total_books INT64,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
            """,
            
            "Location": """
                CREATE NODE TABLE Location(
                    id STRING,
                    name STRING,
                    description STRING,
                    location_type STRING,
                    is_default BOOLEAN,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
            """,
            
            # Activity tracking entities
            "ReadingSession": """
                CREATE NODE TABLE ReadingSession(
                    id STRING,
                    date DATE,
                    pages_read INT64,
                    minutes_read INT64,
                    notes STRING,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
            """,
            
            "Rating": """
                CREATE NODE TABLE Rating(
                    id STRING,
                    value DOUBLE,
                    max_value DOUBLE,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
            """,
            
            "Review": """
                CREATE NODE TABLE Review(
                    id STRING,
                    content STRING,
                    is_spoiler BOOLEAN,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
            """,
            
            "Tag": """
                CREATE NODE TABLE Tag(
                    id STRING,
                    name STRING,
                    color STRING,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
            """,
            
            "Note": """
                CREATE NODE TABLE Note(
                    id STRING,
                    content STRING,
                    note_type STRING,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
            """,
            
            "CustomField": """
                CREATE NODE TABLE CustomField(
                    id STRING,
                    name STRING,
                    field_type STRING,
                    value STRING,
                    created_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
            """
        }
        
        for table_name, schema in node_schemas.items():
            try:
                self._connection.execute(schema)
                logger.debug(f"✓ Created node table: {table_name}")
            except Exception as e:
                if "already exists" not in str(e).lower():
                    logger.error(f"Failed to create {table_name}: {e}")
                    raise
    
    def _create_relationship_tables(self):
        """Create all relationship tables with clean schemas."""
        
        relationship_schemas = {
            # Administrative relationships
            "ADMINISTERS": """
                CREATE REL TABLE ADMINISTERS(
                    FROM User TO User,
                    role STRING,
                    granted_at TIMESTAMP,
                    granted_by STRING
                )
            """,
            
            # Book authorship and contribution
            "AUTHORED": """
                CREATE REL TABLE AUTHORED(
                    FROM Person TO Book,
                    role STRING,
                    order_index INT64
                )
            """,
            
            "CONTRIBUTED": """
                CREATE REL TABLE CONTRIBUTED(
                    FROM Person TO Book,
                    contribution_type STRING,
                    role_description STRING
                )
            """,
            
            # Publishing relationships
            "PUBLISHED_BY": """
                CREATE REL TABLE PUBLISHED_BY(
                    FROM Book TO Publisher,
                    publication_date DATE,
                    edition STRING
                )
            """,
            
            # Categorization
            "CATEGORIZED_AS": """
                CREATE REL TABLE CATEGORIZED_AS(
                    FROM Book TO Category
                )
            """,
            
            "SUBCATEGORY_OF": """
                CREATE REL TABLE SUBCATEGORY_OF(
                    FROM Category TO Category,
                    level INT64
                )
            """,
            
            # Series relationships
            "PART_OF_SERIES": """
                CREATE REL TABLE PART_OF_SERIES(
                    FROM Book TO Series,
                    volume_number INT64,
                    series_order INT64
                )
            """,
            
            # User ownership and reading
            "OWNS": """
                CREATE REL TABLE OWNS(
                    FROM User TO Book,
                    reading_status STRING,
                    ownership_status STRING,
                    media_type STRING,
                    date_added TIMESTAMP,
                    source STRING
                )
            """,
            
            # Location relationships
            "LOCATED_AT": """
                CREATE REL TABLE LOCATED_AT(
                    FROM User TO Location,
                    is_primary BOOLEAN
                )
            """,
            
            "STORED_AT": """
                CREATE REL TABLE STORED_AT(
                    FROM Book TO Location,
                    user_id STRING
                )
            """,
            
            # Reading activity
            "STARTED_READING": """
                CREATE REL TABLE STARTED_READING(
                    FROM User TO Book,
                    start_date TIMESTAMP,
                    estimated_finish DATE
                )
            """,
            
            "FINISHED_READING": """
                CREATE REL TABLE FINISHED_READING(
                    FROM User TO Book,
                    finish_date TIMESTAMP,
                    completion_status STRING
                )
            """,
            
            "LOGGED_SESSION": """
                CREATE REL TABLE LOGGED_SESSION(
                    FROM User TO ReadingSession,
                    book_id STRING
                )
            """,
            
            # User-generated content
            "RATED": """
                CREATE REL TABLE RATED(
                    FROM User TO Rating,
                    book_id STRING
                )
            """,
            
            "REVIEWED": """
                CREATE REL TABLE REVIEWED(
                    FROM User TO Review,
                    book_id STRING
                )
            """,
            
            "TAGGED": """
                CREATE REL TABLE TAGGED(
                    FROM User TO Tag,
                    book_id STRING
                )
            """,
            
            "NOTED": """
                CREATE REL TABLE NOTED(
                    FROM User TO Note,
                    book_id STRING,
                    page_number INT64
                )
            """,
            
            # Borrowing/lending
            "BORROWED_FROM": """
                CREATE REL TABLE BORROWED_FROM(
                    FROM User TO Book,
                    lender_id STRING,
                    borrowed_date TIMESTAMP,
                    due_date TIMESTAMP,
                    returned_date TIMESTAMP
                )
            """,
            
            "LOANED_TO": """
                CREATE REL TABLE LOANED_TO(
                    FROM User TO Book,
                    borrower_id STRING,
                    loaned_date TIMESTAMP,
                    due_date TIMESTAMP,
                    returned_date TIMESTAMP
                )
            """,
            
            # Custom fields
            "HAS_CUSTOM_FIELD": """
                CREATE REL TABLE HAS_CUSTOM_FIELD(
                    FROM User TO CustomField,
                    book_id STRING,
                    field_name STRING
                )
            """
        }
        
        for rel_name, schema in relationship_schemas.items():
            try:
                self._connection.execute(schema)
                logger.debug(f"✓ Created relationship table: {rel_name}")
            except Exception as e:
                if "already exists" not in str(e).lower():
                    logger.error(f"Failed to create {rel_name}: {e}")
                    raise
    
    # Simplified CRUD operations
    
    def create_node(self, node_type: str, properties: Dict[str, Any]) -> bool:
        """Create a node with the given properties."""
        try:
            # Ensure ID and timestamps
            if 'id' not in properties:
                import uuid
                properties['id'] = str(uuid.uuid4())
            
            if 'created_at' not in properties:
                properties['created_at'] = datetime.utcnow()
            
            # Build query
            prop_names = list(properties.keys())
            prop_placeholders = [f"${prop}" for prop in prop_names]
            
            query = f"""
            CREATE (n:{node_type} {{
                {', '.join(f"{prop}: ${prop}" for prop in prop_names)}
            }})
            """
            
            self._connection.execute(query, properties)
            return True
            
        except Exception as e:
            logger.error(f"Failed to create {node_type} node: {e}")
            return False
    
    def create_relationship(self, from_type: str, from_id: str, rel_type: str, 
                          to_type: str, to_id: str, properties: Dict[str, Any] = None) -> bool:
        """Create a relationship between two nodes."""
        try:
            properties = properties or {}
            
            # Build properties clause
            if properties:
                prop_clauses = [f"{key}: ${key}" for key in properties.keys()]
                props_str = f"{{ {', '.join(prop_clauses)} }}"
            else:
                props_str = ""
            
            query = f"""
            MATCH (from:{from_type} {{id: $from_id}}), (to:{to_type} {{id: $to_id}})
            CREATE (from)-[r:{rel_type} {props_str}]->(to)
            """
            
            params = {"from_id": from_id, "to_id": to_id, **properties}
            self._connection.execute(query, params)
            return True
            
        except Exception as e:
            logger.error(f"Failed to create relationship {rel_type}: {e}")
            return False
    
    def get_node(self, node_type: str, node_id: str) -> Optional[Dict[str, Any]]:
        """Get a node by type and ID."""
        try:
            query = f"MATCH (n:{node_type} {{id: $node_id}}) RETURN n"
            result = self._connection.execute(query, {"node_id": node_id})
            
            if result.has_next():
                node = result.get_next()[0]
                return dict(node)
            return None
            
        except Exception as e:
            logger.error(f"Failed to get {node_type} node {node_id}: {e}")
            return None
    
    def query(self, cypher_query: str, parameters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Execute a Cypher query and return results."""
        try:
            parameters = parameters or {}
            result = self._connection.execute(cypher_query, parameters)
            
            rows = []
            while result.has_next():
                row = result.get_next()
                rows.append({f"col_{i}": val for i, val in enumerate(row)})
            return rows
            
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return []


# Example usage and helper functions
class BookGraphService:
    """High-level service for common book operations using the clean schema."""
    
    def __init__(self, db: KuzuGraphDB):
        self.db = db
        self.conn = db.connect()
    
    def add_book_to_user_library(self, user_id: str, book_id: str, 
                                reading_status: str = "plan_to_read",
                                ownership_status: str = "owned",
                                media_type: str = "physical",
                                location_id: str = None) -> bool:
        """Add a book to a user's library with clean relationships."""
        try:
            # Create the main ownership relationship
            success = self.db.create_relationship(
                "User", user_id, "OWNS", "Book", book_id,
                {
                    "reading_status": reading_status,
                    "ownership_status": ownership_status,
                    "media_type": media_type,
                    "date_added": datetime.utcnow()
                }
            )
            
            # If location specified, create storage relationship
            if location_id and success:
                self.db.create_relationship(
                    "Book", book_id, "STORED_AT", "Location", location_id,
                    {"user_id": user_id}
                )
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to add book to library: {e}")
            return False
    
    def start_reading_book(self, user_id: str, book_id: str) -> bool:
        """Mark a book as started reading."""
        return self.db.create_relationship(
            "User", user_id, "STARTED_READING", "Book", book_id,
            {"start_date": datetime.utcnow()}
        )
    
    def finish_reading_book(self, user_id: str, book_id: str, 
                           completion_status: str = "completed") -> bool:
        """Mark a book as finished reading."""
        return self.db.create_relationship(
            "User", user_id, "FINISHED_READING", "Book", book_id,
            {
                "finish_date": datetime.utcnow(),
                "completion_status": completion_status
            }
        )
    
    def rate_book(self, user_id: str, book_id: str, rating: float, 
                  max_rating: float = 5.0) -> bool:
        """Add a rating for a book."""
        import uuid
        
        # Create rating node
        rating_id = str(uuid.uuid4())
        rating_created = self.db.create_node("Rating", {
            "id": rating_id,
            "value": rating,
            "max_value": max_rating
        })
        
        if rating_created:
            # Link rating to user and book
            return self.db.create_relationship(
                "User", user_id, "RATED", "Rating", rating_id,
                {"book_id": book_id}
            )
        return False
    
    def get_user_books(self, user_id: str, reading_status: str = None) -> List[Dict[str, Any]]:
        """Get all books owned by a user, optionally filtered by reading status."""
        try:
            if reading_status:
                query = """
                MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
                WHERE owns.reading_status = $reading_status
                RETURN b, owns
                """
                params = {"user_id": user_id, "reading_status": reading_status}
            else:
                query = """
                MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
                RETURN b, owns
                """
                params = {"user_id": user_id}
            
            return self.db.query(query, params)
            
        except Exception as e:
            logger.error(f"Failed to get user books: {e}")
            return []
    
    def get_book_authors(self, book_id: str) -> List[Dict[str, Any]]:
        """Get all authors of a book."""
        query = """
        MATCH (p:Person)-[authored:AUTHORED]->(b:Book {id: $book_id})
        RETURN p, authored
        ORDER BY authored.order_index
        """
        return self.db.query(query, {"book_id": book_id})
    
    def get_books_in_category(self, category_id: str) -> List[Dict[str, Any]]:
        """Get all books in a category."""
        query = """
        MATCH (b:Book)-[cat:CATEGORIZED_AS]->(c:Category {id: $category_id})
        RETURN b
        """
        return self.db.query(query, {"category_id": category_id})
    
    def get_reading_stats(self, user_id: str) -> Dict[str, Any]:
        """Get reading statistics for a user."""
        queries = {
            "total_books": """
                MATCH (u:User {id: $user_id})-[:OWNS]->(b:Book)
                RETURN COUNT(b) as count
            """,
            "books_read": """
                MATCH (u:User {id: $user_id})-[:FINISHED_READING]->(b:Book)
                RETURN COUNT(b) as count
            """,
            "currently_reading": """
                MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
                WHERE owns.reading_status = 'reading'
                RETURN COUNT(b) as count
            """,
            "average_rating": """
                MATCH (u:User {id: $user_id})-[:RATED]->(r:Rating)
                RETURN AVG(r.value) as avg_rating
            """
        }
        
        stats = {}
        for key, query in queries.items():
            try:
                result = self.db.query(query, {"user_id": user_id})
                if result:
                    stats[key] = result[0].get('col_0', 0)
                else:
                    stats[key] = 0
            except:
                stats[key] = 0
        
        return stats


# Global instance
_graph_db = None

def get_graph_db() -> KuzuGraphDB:
    """Get the global graph database instance."""
    global _graph_db
    if _graph_db is None:
        _graph_db = KuzuGraphDB()
    return _graph_db

def get_book_service() -> BookGraphService:
    """Get the book service instance."""
    return BookGraphService(get_graph_db())


if __name__ == "__main__":
    # Test the schema
    logging.basicConfig(level=logging.INFO)
    
    # Force reset for testing
    os.environ['KUZU_FORCE_RESET'] = 'true'
    
    db = get_graph_db()
    service = get_book_service()
    
    print("Schema created successfully!")
    print("Ready for clean graph operations.")
