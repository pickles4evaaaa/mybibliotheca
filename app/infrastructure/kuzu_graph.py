"""
Kuzu Graph Database - Clean Architecture

A simplified, graph-native design that leverages Kuzu's strengths.
Focus on simple nodes and clear relationships.
"""

import os
import json
import kuzu  # type: ignore
import logging
import uuid
import traceback
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, date, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


class KuzuGraphDB:
    """Simplified Kuzu graph database with clean schema design."""
    
    def __init__(self, database_path: Optional[str] = None):
        if database_path:
            self.database_path = database_path
        else:
            # Get the directory from environment or use default, then append the database name
            kuzu_dir = os.getenv('KUZU_DB_PATH', 'data/kuzu')
            self.database_path = os.path.join(kuzu_dir, 'bibliotheca.db')
        self._database: Optional[kuzu.Database] = None
        self._connection: Optional[kuzu.Connection] = None
        self._is_initialized = False
    
    def _execute_query(self, query: str, params: Optional[Dict[str, Any]] = None):
        """Execute a query and normalize the result to always return a QueryResult."""
        if self._connection is None:
            raise Exception("Connection not established")
        
        result = self._connection.execute(query, params or {})  # type: ignore
        
        # Handle both single QueryResult and list[QueryResult]
        if isinstance(result, list):
            return result[0] if result else None
        return result
        
    def connect(self) -> kuzu.Connection:
        """Establish connection and initialize schema."""
        if self._connection is None:
            try:
                
                # Check if database path exists and log file info
                db_path = Path(self.database_path)
                
                if db_path.exists():
                    files = list(db_path.glob("*"))
                    # Log file info only in debug mode
                    debug_mode = os.getenv('KUZU_DEBUG', 'false').lower() == 'true'
                    if debug_mode:
                        for file in files[:10]:  # Limit to first 10 files
                            print(f"📁 DB file: {file.name} ({file.stat().st_size} bytes)")
                        if len(files) > 10:
                            print(f"... and {len(files) - 10} more files")
                
                Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
                self._database = kuzu.Database(self.database_path)
                self._connection = kuzu.Connection(self._database)
                logger.info("Database connection established, initializing schema...")
                self._initialize_schema()
                logger.info(f"Kuzu connected at {self.database_path}")
                
                # Log post-connection state
                if db_path.exists():
                    files = list(db_path.glob("*"))
                    total_size = sum(f.stat().st_size for f in files if f.is_file())
                    
            except Exception as e:
                logger.error(f"Failed to connect to Kuzu: {e}")
                import traceback
                traceback.print_exc()
                
                # Check if this is a database recovery error
                if "std::bad_alloc" in str(e) or "Error during recovery" in str(e):
                    logger.error("Database recovery failed - this usually indicates corrupted or incomplete database files")
                    
                    # Do NOT clear the database by default. Require explicit opt-in.
                    allow_clear = os.getenv('KUZU_AUTO_RECOVER_CLEAR', 'false').lower() == 'true'
                    if not allow_clear:
                        logger.error("Safety lock engaged: KUZU_AUTO_RECOVER_CLEAR is not true. Skipping destructive recovery to protect data.")
                        logger.error("To force a rebuild, set KUZU_AUTO_RECOVER_CLEAR=true (optional: KUZU_DEBUG=true) and restart. Back up /app/data/kuzu first.")
                        raise

                    logger.warning("Attempting destructive recovery by clearing database path (opt-in enabled)...")
                    try:
                        db_path = Path(self.database_path)
                        if db_path.exists():
                            # Back up existing DB path first (file or directory)
                            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                            backup_dir = Path(os.getenv('KUZU_BACKUP_DIR', '/app/data/backups'))
                            backup_dir.mkdir(parents=True, exist_ok=True)
                            backup_target = backup_dir / f"kuzu_backup_{ts}"
                            try:
                                import shutil
                                if db_path.is_dir():
                                    shutil.make_archive(str(backup_target), 'gztar', root_dir=db_path)
                                else:
                                    # Single-file DB: copy to backups with timestamp
                                    shutil.copy2(str(db_path), str(backup_target.with_suffix('.db')))
                                logger.info(f"📦 Backed up Kuzu DB to {backup_target}")
                            except Exception as be:
                                logger.warning(f"Backup before recovery failed: {be}")

                            # Now clear the path
                            try:
                                import shutil
                                if db_path.is_dir():
                                    shutil.rmtree(db_path, ignore_errors=True)
                                else:
                                    db_path.unlink(missing_ok=True)  # type: ignore
                                logger.info("Database path cleared, creating fresh database...")
                            except Exception as de:
                                logger.error(f"Failed to clear database path: {de}")
                                raise

                        # Create parent and reinitialize
                        Path(self.database_path).parent.mkdir(parents=True, exist_ok=True)
                        self._database = kuzu.Database(self.database_path)
                        self._connection = kuzu.Connection(self._database)
                        logger.info("Database connection established, initializing schema...")
                        self._initialize_schema()
                        logger.info(f"Kuzu connected at {self.database_path} (recovered)")
                    except Exception as recovery_error:
                        logger.error(f"Database recovery failed: {recovery_error}")
                        print("🔧 Make sure KuzuDB is running and accessible")
                        print("💡 If this is a fresh deployment, the database will be created automatically")
                        print("⚠️  If this persists, check that the data directory has proper permissions")
                        raise recovery_error
                else:
                    print("🔧 Make sure KuzuDB is running and accessible")
                    raise
        return self._connection
    
    def _initialize_schema(self):
        """Initialize the graph schema with node and relationship tables."""
        try:
            # Skip if already initialized
            if self._is_initialized:
                logger.debug("Already initialized, skipping")
                return
            
            logger.info("Starting schema initialization...")
            
            # Check environment variable for forced reset
            force_reset = os.getenv('KUZU_FORCE_RESET', 'false').lower() == 'true'
            debug_mode = os.getenv('KUZU_DEBUG', 'false').lower() == 'true'
            
            # Log database file state before initialization (only in debug mode)
            if debug_mode:
                db_path = Path(self.database_path)
                if db_path.exists():
                    files = list(db_path.glob("*"))
                    for file in files[:5]:  # Show first 5 files
                        print(f"📁 DB file: {file.name} ({file.stat().st_size} bytes)")
            
            # Check if database already has User table and determine initialization strategy
            has_existing_users = False
            if not force_reset:
                try:
                    logger.debug("Checking for existing users...")
                    result = self._execute_query("MATCH (u:User) RETURN COUNT(u) as count LIMIT 1")
                    if result and result.has_next():
                        try:
                            _row = result.get_next()
                            _vals = _row if isinstance(_row, (list, tuple)) else list(_row)
                            try:
                                user_count = int(_vals[0] if _vals else 0)
                            except Exception:
                                # Attempt to coerce string counts
                                user_count = int(str(_vals[0])) if _vals and _vals[0] is not None else 0
                        except Exception:
                            user_count = 0
                        if int(user_count or 0) > 0:
                            # Database has existing users - ensure all tables exist but preserve data
                            has_existing_users = True
                            logger.info(f"Database contains {user_count} users - will ensure all tables exist")
                            
                            # Check for books too
                            try:
                                book_result = self._execute_query("MATCH (b:Book) RETURN COUNT(b) as count LIMIT 1")
                                if book_result and book_result.has_next():
                                    try:
                                        _rowb = book_result.get_next()
                                        _valsb = _rowb if isinstance(_rowb, (list, tuple)) else list(_rowb)
                                        book_count = _valsb[0] if _valsb else 0
                                    except Exception:
                                        book_count = 0
                                    logger.info(f"Database also contains {book_count} books")
                            except Exception as book_e:
                                logger.debug(f"Error checking book count: {book_e}")
                        else:
                            logger.info("Database exists but is empty - will initialize schema")
                    else:
                        logger.info("Database is empty - will initialize schema")
                except Exception as e:
                    # If User table doesn't exist, we need to initialize schema
                    logger.debug(f"User table doesn't exist - will initialize schema: {e}")
            else:
                logger.warning("KUZU_FORCE_RESET=true - forcing schema reset (all data will be lost)")
            
            # If we reach here, we need to initialize the schema
            logger.info("🔧 Initializing Kuzu schema...")
            
            # Only drop tables if we're forcing a reset
            if force_reset:
                logger.info("Dropping existing tables due to forced reset...")
                drop_tables = ["OWNS", "WRITTEN_BY", "CONTRIBUTED", "AUTHORED", "PUBLISHED_BY", "PUBLISHED", "CATEGORIZED_AS", "PART_OF_SERIES", "IN_SERIES", "LOGGED", "PARENT_CATEGORY", "STORED_AT", "HAS_CUSTOM_FIELD",
                              "Book", "User", "Author", "Person", "Publisher", "Category", "Series", "ReadingLog", 
                              "Location", "ImportMapping", "ImportJob", "CustomFieldDefinition", "ImportTask"]
                
                for table in drop_tables:
                    try:
                        self._execute_query(f"DROP TABLE {table}")
                        logger.debug(f"Dropped table: {table}")
                    except Exception as e:
                        logger.debug(f"Table {table} doesn't exist or couldn't be dropped: {e}")
            elif has_existing_users:
                logger.info("🔧 Ensuring all tables exist (preserving existing data)...")

                # Check for and add missing columns to Person table
                try:
                    # Test if openlibrary_id column exists and add missing Person fields
                    if self._connection:
                        self._connection.execute("MATCH (p:Person) RETURN p.openlibrary_id LIMIT 1")
                except Exception as e:
                    if "Cannot find property openlibrary_id" in str(e):
                        # Add missing Person fields one by one
                        person_fields_to_add = [
                            ("openlibrary_id", "STRING"),
                            ("image_url", "STRING"),
                            ("birth_date", "STRING"),
                            ("death_date", "STRING"),
                            ("wikidata_id", "STRING"),
                            ("imdb_id", "STRING"),
                            ("alternate_names", "STRING"),
                            ("fuller_name", "STRING"),
                            ("title", "STRING"),
                            ("official_links", "STRING")
                        ]

                        for field_name, field_type in person_fields_to_add:
                            try:
                                if self._connection:
                                    self._connection.execute(f"ALTER TABLE Person ADD {field_name} {field_type}")
                                    logger.debug(f"Added {field_name} column to Person table")
                            except Exception as alter_e:
                                print(f"Note: Could not add {field_name} to Person table: {alter_e}")
                else:
                    print("Schema appears to be up to date")

                # Independently ensure ReadingLog.updated_at exists even if Person schema is up-to-date
                try:
                    if self._connection:
                        self._connection.execute("MATCH (rl:ReadingLog) RETURN rl.updated_at LIMIT 1")
                except Exception as e:
                    if "Cannot find property updated_at" in str(e):
                        try:
                            if self._connection:
                                self._connection.execute("ALTER TABLE ReadingLog ADD updated_at TIMESTAMP")
                                logger.info("Added updated_at column to ReadingLog table")
                        except Exception as alter_e:
                            print(f"Note: Could not add updated_at to ReadingLog table: {alter_e}")
                # Note: ReadingLog table migration completed

                # Add OPDS column to existing Book nodes if missing
                try:
                    if self._connection:
                        self._connection.execute("MATCH (b:Book) RETURN b.opds_source_id LIMIT 1")
                except Exception as e:
                    if "Cannot find property opds_source_id" in str(e):
                        try:
                            if self._connection:
                                self._connection.execute("ALTER TABLE Book ADD opds_source_id STRING")
                                logger.info("Added opds_source_id column to Book table")
                        except Exception as alter_e:
                            print(f"Note: Could not add opds_source_id to Book table: {alter_e}")

                # Add OPDS metadata columns if missing
                for column_name in ("opds_source_updated_at", "opds_source_entry_hash"):
                    try:
                        if self._connection:
                            self._connection.execute(f"MATCH (b:Book) RETURN b.{column_name} LIMIT 1")
                    except Exception as e:
                        if "Cannot find property" in str(e) and column_name in str(e):
                            try:
                                if self._connection:
                                    self._connection.execute(f"ALTER TABLE Book ADD {column_name} STRING")
                                    logger.info("Added %s column to Book table", column_name)
                            except Exception as alter_e:
                                print(f"Note: Could not add {column_name} to Book table: {alter_e}")
                        
            else:
                logger.info("🔧 Creating new database schema...")
            
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
                    opds_source_id STRING,
                    opds_source_updated_at STRING,
                    opds_source_entry_hash STRING,
                    average_rating DOUBLE,
                    rating_count INT64,
                    series STRING,
                    series_volume STRING,
                    series_order INT64,
                    custom_metadata STRING,
                    raw_categories STRING,
                    audio_length_minutes INT64,
                    audio_is_abridged BOOLEAN,
                    audio_narrators STRING,
                    media_format STRING,
                    audiobookshelf_id STRING,
                    audio_duration_ms INT64,
                    media_type STRING,
                    audiobookshelf_updated_at STRING,
                    quantity INT64,
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
                    birth_date STRING,
                    death_date STRING,
                    birth_year INT64,
                    death_year INT64,
                    birth_place STRING,
                    bio STRING,
                    website STRING,
                    openlibrary_id STRING,
                    image_url STRING,
                    wikidata_id STRING,
                    imdb_id STRING,
                    alternate_names STRING,
                    fuller_name STRING,
                    title STRING,
                    official_links STRING,
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
                    level INT64 DEFAULT 0,
                    color STRING,
                    icon STRING,
                    aliases STRING,
                    book_count INT64 DEFAULT 0,
                    user_book_count INT64 DEFAULT 0,
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
                    user_cover STRING,
                    cover_url STRING,
                    custom_cover BOOLEAN,
                    generated_placeholder BOOLEAN,
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
                    updated_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE Location(
                    id STRING,
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
                    current_page INT64,
                    total_pages INT64,
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
                    location_id STRING,
                    user_rating DOUBLE,
                    rating_date TIMESTAMP,
                    user_review STRING,
                    review_date TIMESTAMP,
                    is_review_spoiler BOOLEAN,
                    personal_notes STRING,
                    pace STRING,
                    character_driven BOOLEAN,
                    source STRING,
                    custom_metadata STRING,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE WRITTEN_BY(
                    FROM Book TO Person,
                    contribution_type STRING,
                    role STRING,
                    order_index INT64,
                    created_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE AUTHORED(
                    FROM Person TO Book,
                    contribution_type STRING,
                    role STRING,
                    order_index INT64,
                    created_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE NARRATED(
                    FROM Person TO Book,
                    role STRING,
                    order_index INT64,
                    created_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE EDITED(
                    FROM Person TO Book,
                    role STRING,
                    order_index INT64,
                    created_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE TRANSLATED(
                    FROM Person TO Book,
                    role STRING,
                    order_index INT64,
                    created_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE ILLUSTRATED(
                    FROM Person TO Book,
                    role STRING,
                    order_index INT64,
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
                    volume_number_double DOUBLE,
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
                CREATE REL TABLE FOR_BOOK(
                    FROM ReadingLog TO Book,
                    created_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE PARENT_CATEGORY(
                    FROM Category TO Category,
                    created_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE STORED_AT(
                    FROM Book TO Location,
                    created_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE HAS_CUSTOM_FIELD(
                    FROM User TO CustomField,
                    book_id STRING,
                    field_name STRING,
                    field_value STRING,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
                """
            ]
            
            # Execute all creation queries with individual error handling
            all_queries = node_queries + relationship_queries
            tables_created = 0
            tables_existed = 0
            
            for i, query in enumerate(all_queries):
                try:
                    self._execute_query(query)
                    tables_created += 1
                    logger.debug(f"Successfully created table/relationship {i+1}/{len(all_queries)}")
                except Exception as e:
                    # Check if it's a "already exists" error and skip if so
                    if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
                        tables_existed += 1
                        continue
                    else:
                        logger.error(f"Failed to execute query {i+1}: {e}")
                        raise
                
            logger.info(f"✅ Kuzu schema ensured: {tables_created} created, {tables_existed} already existed")
            self._is_initialized = True
            
            # Log final database state after initialization
            db_path = Path(self.database_path)
            if db_path.exists():
                files = list(db_path.glob("*"))
                total_size = sum(f.stat().st_size for f in files if f.is_file())
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kuzu schema: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def disconnect(self):
        """Close Kuzu connection."""
        if self._connection:
            
            # Log final database state before closing
            db_path = Path(self.database_path)
            if db_path.exists():
                files = list(db_path.glob("*"))
                total_size = sum(f.stat().st_size for f in files if f.is_file())
                
                # Try to get final counts
                try:
                    user_result = self._connection.execute("MATCH (u:User) RETURN COUNT(u) as count")
                    if isinstance(user_result, list) and user_result:
                        user_result = user_result[0]
                    if user_result and user_result.has_next():  # type: ignore
                        try:
                            _ur = user_result.get_next()  # type: ignore
                            _urv = _ur if isinstance(_ur, (list, tuple)) else list(_ur)
                            try:
                                user_count = int(_urv[0] if _urv else 0)
                            except Exception:
                                user_count = int(str(_urv[0])) if _urv and _urv[0] is not None else 0
                        except Exception:
                            user_count = 0  # type: ignore
                    
                    book_result = self._connection.execute("MATCH (b:Book) RETURN COUNT(b) as count")
                    if isinstance(book_result, list) and book_result:
                        book_result = book_result[0]
                    if book_result and book_result.has_next():  # type: ignore
                        try:
                            _br = book_result.get_next()  # type: ignore
                            _brv = _br if isinstance(_br, (list, tuple)) else list(_br)
                            try:
                                book_count = int(_brv[0] if _brv else 0)
                            except Exception:
                                book_count = int(str(_brv[0])) if _brv and _brv[0] is not None else 0
                        except Exception:
                            book_count = 0  # type: ignore
                    
                    # Only attempt OWNS count when legacy OWNS schema is enabled
                    try:
                        owns_enabled = os.getenv('ENABLE_OWNS_SCHEMA', 'false').lower() in ('1', 'true', 'yes')
                    except Exception:
                        owns_enabled = False
                    if owns_enabled:
                        owns_result = self._connection.execute("MATCH ()-[r:OWNS]->() RETURN COUNT(r) as count")
                        if isinstance(owns_result, list) and owns_result:
                            owns_result = owns_result[0]
                        if owns_result and owns_result.has_next():  # type: ignore
                            try:
                                _or = owns_result.get_next()  # type: ignore
                                _orv = _or if isinstance(_or, (list, tuple)) else list(_or)
                                try:
                                    owns_count = int(_orv[0] if _orv else 0)
                                except Exception:
                                    owns_count = int(str(_orv[0])) if _orv and _orv[0] is not None else 0
                            except Exception:
                                owns_count = 0  # type: ignore
                        
                except Exception as e:
                    print(f"Error getting relationship count: {e}")
            
            # NOTE: KuzuDB uses auto-commit mode, no manual commit needed before closing
            
            self._connection.close()
            self._connection = None
            self._database = None
            logger.info("Kuzu connection closed")
        else:
            logger.info("Connection already closed")
    
    @property
    def connection(self) -> kuzu.Connection:
        """Get Kuzu connection, connecting if needed."""
        if self._connection is None:
            return self.connect()
        return self._connection
    
    # Repository-expected methods that delegate to storage layer
    
    def create_node(self, node_type: str, data: Dict[str, Any]) -> bool:
        """Create a node using the storage layer."""
        storage = KuzuGraphStorage(self)
        node_id = data.get('id', str(uuid.uuid4()))
        return storage.store_node(node_type, node_id, data)
    
    def get_node(self, node_type: str, node_id: str) -> Optional[Dict[str, Any]]:
        """Get a node using the storage layer."""
        storage = KuzuGraphStorage(self)
        return storage.get_node(node_type, node_id)
    
    def update_node(self, node_type: str, node_id: str, updates: Dict[str, Any]) -> bool:
        """Update a node using the storage layer."""
        storage = KuzuGraphStorage(self)
        return storage.update_node(node_type, node_id, updates)
    
    def delete_node(self, node_type: str, node_id: str) -> bool:
        """Delete a node using the storage layer."""
        storage = KuzuGraphStorage(self)
        return storage.delete_node(node_type, node_id)
    
    def get_nodes_by_type(self, node_type: str, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Get nodes by type using the storage layer."""
        storage = KuzuGraphStorage(self)
        return storage.get_nodes_by_type(node_type, limit, offset)
    
    def find_nodes_by_type(self, node_type: str, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Find nodes by type using the storage layer."""
        storage = KuzuGraphStorage(self)
        return storage.find_nodes_by_type(node_type, limit, offset)
    
    def query(self, cypher_query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a Cypher query and return results."""
        try:
            result = self.connection.execute(cypher_query, params or {})
            # Handle both single QueryResult and list[QueryResult]
            if isinstance(result, list) and result:
                result = result[0]
            
            rows = []
            if result:
                storage = KuzuGraphStorage(self)
                while result.has_next():  # type: ignore
                    row = result.get_next()  # type: ignore
                    values = storage._row_values(row)
                    if len(values) == 1:
                        rows.append({'result': values[0]})
                    else:
                        row_dict = {f'col_{i}': value for i, value in enumerate(values)}
                        rows.append(row_dict)
            return rows
        except Exception as e:
            # Don't log expected "already exists" errors as errors
            if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
                pass  # Silently skip expected "already exists" errors
            else:
                logger.error(f"Query execution failed: {e}")
                
                # Try to recover from connection errors
                try:
                    from app.utils.connection_recovery import handle_connection_error
                    if handle_connection_error(str(e)):
                        logger.info("🔄 Connection recovered, but original query still failed")
                except Exception as recovery_error:
                    logger.debug(f"Connection recovery attempt failed: {recovery_error}")
                    
            return []
    
    def create_relationship(self, from_type: str, from_id: str, rel_type: str,
                          to_type: str, to_id: str, properties: Optional[Dict[str, Any]] = None) -> bool:
        """Create a relationship between two nodes."""
        try:
            props_str = ""
            params = {
                "from_id": from_id,
                "to_id": to_id
            }
            
            if properties:
                prop_assignments = []
                for key, value in properties.items():
                    params[key] = value
                    prop_assignments.append(f"{key}: ${key}")
                props_str = f" {{{', '.join(prop_assignments)}}}"
            
            query = f"""
            MATCH (from:{from_type}), (to:{to_type})
            WHERE from.id = $from_id AND to.id = $to_id
            CREATE (from)-[:{rel_type}{props_str}]->(to)
            """
            
            self.connection.execute(query, params)
            return True
            
        except Exception as e:
            logger.error(f"Failed to create relationship {rel_type}: {e}")
            return False
        

class KuzuGraphStorage:
    """Kuzu-based graph storage implementation.
    
    Uses Kuzu's native graph database capabilities for nodes and relationships.
    This provides true graph query capabilities with Cypher-like syntax.
    """
    
    def __init__(self, connection: KuzuGraphDB):
        self.connection = connection
        self.kuzu_conn = connection.connection
    
    def _execute_query(self, query: str, params: Optional[Dict[str, Any]] = None):
        """Execute a query and normalize the result to always return a QueryResult."""
        result = self.kuzu_conn.execute(query, params or {})  # type: ignore
        
        # Handle both single QueryResult and list[QueryResult]
        if isinstance(result, list):
            return result[0] if result else None
        return result

    def _row_values(self, row: Any) -> List[Any]:
        """Safely convert a Kuzu row to a list of values."""
        try:
            if isinstance(row, (list, tuple)):
                return list(row)
            if isinstance(row, dict):
                return list(row.values())
            # Some Kuzu row types are iterable but not list/tuple
            return [*row]
        except Exception:
            try:
                # Best-effort fallback
                return list(row)  # type: ignore
            except Exception:
                return []
    
    def query(self, cypher_query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a Cypher query and return results - compatibility method for services."""
        try:
            params = params or {}
            result = self._execute_query(cypher_query, params)
            
            rows = []
            if result and result.has_next():
                while result.has_next():
                    row = result.get_next()
                    values = self._row_values(row)
                    if len(values) == 1:
                        rows.append({'result': values[0]})
                    else:
                        row_dict = {f'col_{i}': value for i, value in enumerate(values)}
                        rows.append(row_dict)
            return rows
            
        except Exception as e:
            # Don't log expected "already exists" errors as errors
            if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
                pass  # Silently skip expected "already exists" errors
            else:
                logger.error(f"Query execution failed: {e}")
                
                # Try to recover from connection errors
                try:
                    from app.utils.connection_recovery import handle_connection_error
                    if handle_connection_error(str(e)):
                        logger.info("🔄 Connection recovered, but original query still failed")
                except Exception as recovery_error:
                    logger.debug(f"Connection recovery attempt failed: {recovery_error}")
                    
            return []
    
    # Node Operations
    
    def store_node(self, node_type: str, node_id: str, data: Dict[str, Any]) -> bool:
        """Store a node in Kuzu."""
        try:
            print(f"[KUZU_STORAGE] 📝 Storing {node_type} node: {node_id}")
            print(f"[KUZU_STORAGE] 📊 Node data keys: {list(data.keys())}")
            
            # Print debug info for TIMESTAMP fields
            for ts_field in ['created_at', 'updated_at']:
                if ts_field in data:
                    print(f"[KUZU_GRAPH][DEBUG] {ts_field} before serialization: {data[ts_field]} (type: {type(data[ts_field])})")
            
            # Handle special field conversions for Kuzu
            serialized_data = data.copy()
            # Convert ISO datetime strings for created_at and updated_at to datetime objects for Kuzu TIMESTAMP fields
            for ts_field in ['created_at', 'updated_at']:
                if ts_field in serialized_data and isinstance(serialized_data[ts_field], str):
                    try:
                        serialized_data[ts_field] = datetime.fromisoformat(serialized_data[ts_field])
                    except ValueError:
                        logger.warning(f"Could not parse {ts_field}: {serialized_data[ts_field]}, leaving original value")
            
            # Filter out relationship fields that shouldn't be stored directly
            # These are populated by service layer but shouldn't be stored in the database
            relationship_fields = ['parent', 'children', 'authors', 'contributors', 'publisher', 'series', 'categories']
            for field in relationship_fields:
                if field in serialized_data:
                    del serialized_data[field]
            # Convert custom_metadata dict to JSON string so it persists on node
            if 'custom_metadata' in serialized_data and isinstance(serialized_data['custom_metadata'], dict):
                try:
                    serialized_data['custom_metadata'] = json.dumps(serialized_data['custom_metadata'])
                except Exception:
                    # Leave as-is if serialization fails
                    pass
            
            # Filter out None values and empty strings that can cause ANY type errors in Kuzu
            # Exception: parent_id can be None for root categories
            clean_data = {}
            for k, v in serialized_data.items():
                if k == 'parent_id':
                    # For parent_id, allow None (root categories) but filter empty strings
                    if v is not None and v != '':
                        clean_data[k] = v
                    # If None, don't include it (allows NULL in database)
                elif v is not None and v != '' and v != []:
                    clean_data[k] = v
                else:
                    logger.debug(f"[KUZU_GRAPH][DEBUG] Filtering out NULL/empty field {k}={v} for {node_type}")
            serialized_data = clean_data
            # Upsert logic: update if node exists, else create
            existing = self.get_node(node_type, node_id)
            if existing is not None:
                print(f"[KUZU_STORAGE] 🔄 Node {node_type}:{node_id} exists, updating")
                # Perform update with cleaned data
                return self.update_node(node_type, node_id, serialized_data)
            
            # Debug: Print what fields we're actually storing
            logger.info(f"[KUZU_GRAPH][DEBUG] Storing {node_type} with fields: {list(serialized_data.keys())}")
            print(f"[KUZU_STORAGE] 🔧 Final fields to store: {list(serialized_data.keys())}")
            
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
                serialized_data['created_at'] = datetime.now(timezone.utc)
            if not serialized_data.get('updated_at'):
                serialized_data['updated_at'] = datetime.now(timezone.utc)
            
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
            
            print(f"[KUZU_STORAGE] 🚀 Executing CREATE query for {node_type}")
            self.kuzu_conn.execute(query, serialized_data)
            
            # KuzuDB handles persistence automatically via WAL - no manual checkpoint needed
            
            print(f"[KUZU_STORAGE] ✅ Successfully stored {node_type} node: {node_id}")
            
            # Verify the node was actually stored
            verification_query = f"MATCH (n:{node_type}) WHERE n.id = $node_id RETURN COUNT(n) as count"
            verify_result = self._execute_query(verification_query, {"node_id": node_id})
            if verify_result and verify_result.has_next():
                _row = verify_result.get_next()
                _vals = self._row_values(_row)
                count = _vals[0] if _vals else 0
                if int(count) > 0:
                    print(f"[KUZU_STORAGE] ✅ Verification: {node_type} node {node_id} exists in database")
                else:
                    print(f"[KUZU_STORAGE] ❌ Verification failed: {node_type} node {node_id} not found after creation!")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to store {node_type} node {node_id}: {e}")
            print(f"[KUZU_STORAGE] ❌ Failed to store {node_type} node {node_id}: {e}")
            print(f"[KUZU_STORAGE] 🔍 Database path: {getattr(self.connection, 'database_path', 'Unknown')}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_node(self, node_type: str, node_id: str) -> Optional[Dict[str, Any]]:
        """Get a node by type and ID."""
        try:
            query = f"MATCH (n:{node_type}) WHERE n.id = $node_id RETURN n.id, n"
            result = self._execute_query(query, {"node_id": node_id})
            
            if result and result.has_next():
                row = result.get_next()
                _vals = self._row_values(row)
                if len(_vals) < 2:
                    return None
                returned_id = _vals[0]
                node_obj = _vals[1]
                
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
    
    def _serialize_datetime_values(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Keep datetime objects as datetime objects for Kuzu storage."""
        serialized = {}
        for key, value in data.items():
            if isinstance(value, datetime):
                # Keep as datetime object - Kuzu expects datetime objects, not strings
                serialized[key] = value
            else:
                serialized[key] = value
        return serialized
    
    def update_node(self, node_type: str, node_id: str, updates: Dict[str, Any]) -> bool:
        """Update specific fields of a node."""
        try:
            print(f"🔧 [UPDATE_NODE] Called with node_type={node_type}, node_id={node_id}")
            print(f"🔧 [UPDATE_NODE] Raw updates: {updates}")
            
            # Serialize updates
            # Convert custom_metadata dict to JSON string so it persists
            if 'custom_metadata' in updates and isinstance(updates['custom_metadata'], dict):
                try:
                    updates['custom_metadata'] = json.dumps(updates['custom_metadata'])
                except Exception:
                    pass
            serialized_updates = self._serialize_datetime_values(updates)
            # KuzuDB requires datetime objects for TIMESTAMP fields, not strings
            serialized_updates['updated_at'] = datetime.now(timezone.utc)
            
            print(f"🔧 [UPDATE_NODE] Serialized updates: {serialized_updates}")
            
            # Build SET clause
            set_clauses = [f"n.{key} = ${key}" for key in serialized_updates.keys()]
            
            query = f"""
            MATCH (n:{node_type}) 
            WHERE n.id = $node_id 
            SET {', '.join(set_clauses)}
            """
            
            params = {"node_id": node_id, **serialized_updates}
            
            print(f"🔧 [UPDATE_NODE] Generated query: {query}")
            print(f"🔧 [UPDATE_NODE] Query parameters: {params}")
            
            try:
                print(f"🔧 [UPDATE_NODE] About to execute query...")
                result = self.kuzu_conn.execute(query, params)
                print(f"🔧 [UPDATE_NODE] Query executed successfully, result: {result}")
                print(f"🔧 [UPDATE_NODE] Result type: {type(result)}")
                return True
            except Exception as exec_e:
                print(f"🔧 [UPDATE_NODE] Query execution failed: {type(exec_e).__name__}: {str(exec_e)}")
                raise exec_e
            
        except Exception as e:
            logger.error(f"Failed to update {node_type} node {node_id}: {e}")
            print(f"🔧 [UPDATE_NODE] Exception details: {type(e).__name__}: {str(e)}")
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
            
            result = self._execute_query(query)
            nodes = []
            if result:
                while result.has_next():
                    row = result.get_next()
                    _vals = self._row_values(row)
                    if len(_vals) < 2:
                        continue
                    node_id = _vals[0]  # The ID
                    node_obj = _vals[1]  # The full node object
                    
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
    
    def create_relationship(self, from_type: str, from_id: str, rel_type: str, to_type: str, to_id: str, properties: Optional[Dict[str, Any]] = None) -> bool:
        """Create a relationship between two nodes."""
        try:
            properties = properties or {}
            
            print(f"[KUZU_STORAGE] 🔧 Relationship properties: {list(properties.keys()) if properties else 'None'}")
            
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
            
            # Ensure created_at is set as datetime object (not ISO string)
            if 'created_at' not in processed_props:
                processed_props['created_at'] = datetime.now(timezone.utc)
            elif isinstance(processed_props['created_at'], str):
                try:
                    if processed_props['created_at'].endswith('Z'):
                        processed_props['created_at'] = processed_props['created_at'][:-1] + '+00:00'
                    processed_props['created_at'] = datetime.fromisoformat(processed_props['created_at'])
                except (ValueError, TypeError):
                    processed_props['created_at'] = datetime.now(timezone.utc)
            
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
            
            print(f"[KUZU_STORAGE] 🚀 Executing CREATE RELATIONSHIP query")
            self.kuzu_conn.execute(query, params)
            
            # KuzuDB handles persistence automatically via WAL - no manual checkpoint needed
            
            print(f"[KUZU_STORAGE] ✅ Successfully created {rel_type} relationship")
            
            # Verify the relationship was created
            verify_query = f"""
            MATCH (from:{from_type})-[r:{rel_type}]->(to:{to_type})
            WHERE from.id = $from_id AND to.id = $to_id
            RETURN COUNT(r) as count
            """
            verify_result = self._execute_query(verify_query, {"from_id": from_id, "to_id": to_id})
            if verify_result and verify_result.has_next():
                _row = verify_result.get_next()
                _vals = self._row_values(_row)
                count = _vals[0] if _vals else 0
                if int(count) > 0:
                    print(f"[KUZU_STORAGE] ✅ Verification: {rel_type} relationship exists")
                else:
                    print(f"[KUZU_STORAGE] ❌ Verification failed: {rel_type} relationship not found after creation!")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to create relationship {from_id} -{rel_type}-> {to_id}: {e}")
            print(f"[KUZU_STORAGE] ❌ Failed to create relationship {from_id} -{rel_type}-> {to_id}: {e}")
            print(f"[KUZU_STORAGE] 🔍 Database path: {getattr(self.connection, 'database_path', 'Unknown')}")
            traceback.print_exc()
            return False
    
    def get_relationships(self, from_type: str, from_id: str, rel_type: Optional[str] = None) -> List[Dict[str, Any]]:
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
            
            result = self._execute_query(query, {"from_id": from_id})
            relationships = []
            if result:
                while result.has_next():
                    row = result.get_next()
                    vals = self._row_values(row)
                    rel_raw = vals[0] if len(vals) > 0 else {}
                    to_raw = vals[1] if len(vals) > 1 else {}
                    try:
                        rel_data = dict(rel_raw)
                    except Exception:
                        rel_data = {"value": rel_raw}
                    try:
                        to_data = dict(to_raw)
                    except Exception:
                        to_data = {"value": to_raw}
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
    
    def store_custom_metadata(self, user_id: str, book_id: str, field_name: str, field_value: str, field_type: Optional[str] = None) -> bool:
        """
        Store custom metadata for a user-book relationship.
        
        This method handles the dual storage system:
        1. Updates OWNS relationship custom_metadata JSON for display (if legacy OWNS enabled)
        2. Creates HAS_CUSTOM_FIELD relationships for usage tracking  
        3. Increments usage count on CustomField definitions
        
        Args:
            user_id: The user ID
            book_id: The book ID  
            field_name: The custom field name
            field_value: The field value
            field_type: Optional field type for new fields
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Step 1 (optional): Update the OWNS relationship custom_metadata when legacy OWNS is enabled
            owns_enabled = os.getenv('ENABLE_OWNS_SCHEMA', 'false').lower() in ('1', 'true', 'yes')
            if owns_enabled:
                try:
                    # First get the current OWNS relationship
                    query_get_owns = """
                    MATCH (u:User {id: $user_id})-[r:OWNS]->(b:Book {id: $book_id})
                    RETURN r.custom_metadata as current_metadata
                    """
                    result = self._execute_query(query_get_owns, {"user_id": user_id, "book_id": book_id})
                    current_metadata = {}
                    if result and result.has_next():
                        _row = result.get_next()
                        _vals = self._row_values(_row)
                        metadata_json = _vals[0] if _vals else None
                        if metadata_json:
                            try:
                                current_metadata = json.loads(metadata_json) if isinstance(metadata_json, str) else metadata_json
                            except (json.JSONDecodeError, TypeError):
                                current_metadata = {}
                    # Update the metadata
                    current_metadata[field_name] = field_value
                    metadata_json_str = json.dumps(current_metadata)
                    # Update OWNS rel
                    query_update_owns = """
                    MATCH (u:User {id: $user_id})-[r:OWNS]->(b:Book {id: $book_id})
                    SET r.custom_metadata = $metadata_json
                    """
                    self.kuzu_conn.execute(query_update_owns, {
                        "user_id": user_id,
                        "book_id": book_id,
                        "metadata_json": metadata_json_str
                    })
                except Exception:
                    # Skip silently if OWNS schema not present or any error occurs
                    pass
            
            
            # Step 2: Look for existing CustomField definition by name
            query_find_field = """
            MATCH (cf:CustomField)
            WHERE cf.name = $field_name
            RETURN cf.id as field_id
            LIMIT 1
            """
            
            result = self._execute_query(query_find_field, {"field_name": field_name})
            field_definition_id = None
            
            if result and result.has_next():
                _rowf = result.get_next()
                _valsf = self._row_values(_rowf)
                field_definition_id = _valsf[0] if _valsf else None
                
                # Step 3: Increment usage count on the field definition
                query_increment = """
                MATCH (cf:CustomField {id: $field_id})
                SET cf.usage_count = COALESCE(cf.usage_count, 0) + 1
                """
                
                self.kuzu_conn.execute(query_increment, {"field_id": field_definition_id})
                
                # Step 4: Create/Update HAS_CUSTOM_FIELD relationship
                # First check if relationship already exists
                query_check_rel = """
                MATCH (u:User {id: $user_id})-[r:HAS_CUSTOM_FIELD]->(cf:CustomField {id: $field_id})
                WHERE r.book_id = $book_id
                RETURN COUNT(r) as count
                """
                
                result = self._execute_query(query_check_rel, {
                    "user_id": user_id, 
                    "field_id": field_definition_id, 
                    "book_id": book_id
                })
                
                rel_exists = False
                if result and result.has_next():
                    _rowc = result.get_next()
                    _valsc = self._row_values(_rowc)
                    count = _valsc[0] if _valsc else 0
                    rel_exists = int(count) > 0
                
                if not rel_exists:
                    # Create new HAS_CUSTOM_FIELD relationship
                    rel_props = {
                        'book_id': book_id,
                        'field_name': field_name,
                        'field_value': field_value,
                        'created_at': datetime.now(timezone.utc).isoformat()
                    }
                    if field_definition_id is None:
                        logger.error("Cannot create HAS_CUSTOM_FIELD relationship: field_definition_id is None")
                    else:
                        success = self.create_relationship(
                            'User', user_id, 
                            'HAS_CUSTOM_FIELD', 
                            'CustomField', str(field_definition_id), 
                            rel_props
                        )
                        if success:
                            logger.info(f"Created custom field relationship for field {field_definition_id}")
                        else:
                            logger.error(f"Failed to create custom field relationship for field {field_definition_id}")
                else:
                    # Update existing relationship value
                    query_update_rel = """
                    MATCH (u:User {id: $user_id})-[r:HAS_CUSTOM_FIELD]->(cf:CustomField {id: $field_id})
                    WHERE r.book_id = $book_id
                    SET r.field_value = $field_value, r.updated_at = $updated_at
                    """
                    
                    self.kuzu_conn.execute(query_update_rel, {
                        "user_id": user_id,
                        "field_id": field_definition_id,
                        "book_id": book_id,
                        "field_value": field_value,
                        "updated_at": datetime.now(timezone.utc)
                    })
                    
            else:
                print(f"ℹ️ [STORE_CUSTOM_METADATA] No field definition found for '{field_name}' - stored in personal metadata only (OWNS skipped)")
            
            return True
            
        except Exception as e:
            traceback.print_exc()
            return False

    # Advanced Graph Queries
    
    def execute_cypher(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a custom Cypher query."""
        try:
            parameters = parameters or {}
            result = self._execute_query(query, parameters)
            
            rows = []
            if result and result.has_next():  # type: ignore
                while result.has_next():  # type: ignore
                    row = result.get_next()  # type: ignore
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
            result = self._execute_query(query)
            if result and result.has_next():  # type: ignore
                _row = result.get_next()  # type: ignore
                _vals = self._row_values(_row)
                return int(_vals[0]) if _vals else 0
            return 0
        except Exception as e:
            logger.error(f"Failed to count {node_type} nodes: {e}")
            return 0
    
    def count_relationships(self, rel_type: str) -> int:
        """Count relationships of a specific type."""
        try:
            query = f"MATCH ()-[r:{rel_type}]->() RETURN COUNT(r) as count"
            result = self._execute_query(query)
            if result and result.has_next():  # type: ignore
                _row = result.get_next()  # type: ignore
                _vals = self._row_values(_row)
                return int(_vals[0]) if _vals else 0
            return 0
        except Exception as e:
            logger.error(f"Failed to count {rel_type} relationships: {e}")
            return 0
    
    def _force_checkpoint(self):
        """Force a checkpoint to ensure all transactions are written to disk."""
        try:
            # Execute a checkpoint operation to flush WAL to main database files
            print(f"🔄 [KUZU_CHECKPOINT] Forcing checkpoint to flush WAL to disk...")
            self.kuzu_conn.execute("CHECKPOINT;")
        except Exception as e:
            # If CHECKPOINT command doesn't exist, try other approaches
            try:
                # Try a simple query to force a database sync
                self.kuzu_conn.execute("MATCH (n) RETURN COUNT(n) LIMIT 1;")
            except Exception as e2:
                logger.error(f"Failed to sync database: {e2}")
    
    def safe_checkpoint(self):
        """Safely checkpoint the database only when needed, with error handling."""
        try:
            print(f"💾 [KUZU_SAFE] Performing safe checkpoint...")
            self.kuzu_conn.execute("CHECKPOINT;")
            print(f"✅ [KUZU_SAFE] Safe checkpoint completed successfully")
            return True
        except Exception as e:
            print(f"⚠️ [KUZU_SAFE] Safe checkpoint failed (this is usually okay): {e}")
            return False
                

# Global database instance - SINGLE SOURCE OF TRUTH
_kuzu_database = None
_graph_storage = None


def get_kuzu_connection() -> 'KuzuGraphDB':
    """Get the global KuzuDB instance. DEPRECATED: Use get_kuzu_database() instead."""
    return get_kuzu_database()


def get_kuzu_database() -> 'KuzuGraphDB':
    """
    Get the single global KuzuGraphDB instance.
    
    ⚠️ WARNING: This function is NOT thread-safe and should be avoided in production!
    It will cause concurrency issues with multiple users. Use safe_execute_query() 
    or safe_get_connection() from app.utils.safe_kuzu_manager instead.
    
    This function is deprecated and will be removed in a future version.
    """
    global _kuzu_database
    if _kuzu_database is None:
        logger.warning("🚨 DANGEROUS: Using thread-unsafe global KuzuDB singleton! "
                      "This will cause concurrency issues. Use safe_execute_query() instead.")
        _kuzu_database = KuzuGraphDB()
        _kuzu_database.connect()
        logger.info(f"Single global KuzuDB instance established")
    # Suppress repeated reuse messages - only log once
    return _kuzu_database


def get_graph_storage() -> 'KuzuGraphStorage':
    """
    Get global KuzuGraphStorage instance using the single database.
    
    ⚠️ WARNING: This function is NOT thread-safe and should be avoided in production!
    It will cause concurrency issues with multiple users. Use safe_execute_query() 
    or safe_get_connection() from app.utils.safe_kuzu_manager instead.
    
    This function is deprecated and will be removed in a future version.
    """
    global _graph_storage
    if _graph_storage is None:
        logger.warning("🚨 DANGEROUS: Using thread-unsafe global KuzuDB storage! "
                      "This will cause concurrency issues. Use safe_get_connection() instead.")
        database = get_kuzu_database()  # Always use the same database instance
        _graph_storage = KuzuGraphStorage(database)
    return _graph_storage


# ==============================================================================
# SAFE THREAD-SAFE ALTERNATIVES - USE THESE INSTEAD!
# ==============================================================================

# Safe alternatives will be imported when needed to avoid circular imports
# Use safe_execute_kuzu_query() and safe_get_kuzu_connection() instead of the dangerous globals


# Convenience functions for migration
def safe_execute_kuzu_query(query: str, params: Optional[Dict[str, Any]] = None, 
                           user_id: Optional[str] = None, operation: str = "query"):
    """
    Execute a KuzuDB query safely with automatic connection management.
    
    This is the recommended way to execute KuzuDB queries. It provides
    thread-safe access and automatic connection lifecycle management.
    
    Args:
        query: Cypher query string
        params: Query parameters
        user_id: User identifier for tracking and isolation
        operation: Description of operation for debugging
        
    Returns:
        Query result
        
    Example:
        result = safe_execute_kuzu_query(
            "MATCH (b:Book) WHERE b.user_id = $user_id RETURN b",
            {"user_id": "user123"},
            user_id="user123",
            operation="get_user_books"
        )
    """
    try:
        # Import here to avoid circular imports
        from ..utils.safe_kuzu_manager import safe_execute_query
        return safe_execute_query(query, params, user_id, operation)
    except ImportError:
        logger.error("🚨 CRITICAL: safe_execute_query not available! Using dangerous fallback.")
        # Fallback to dangerous global (temporary during migration)
        db = get_kuzu_database()
        conn = db.connect()
        return conn.execute(query, params or {})


def safe_get_kuzu_connection(user_id: Optional[str] = None, operation: str = "connection"):
    """
    Get a thread-safe KuzuDB connection context manager.
    
    This is the recommended way to get KuzuDB connections for complex operations.
    
    Args:
        user_id: User identifier for tracking and isolation
        operation: Description of operation for debugging
        
    Returns:
        Context manager yielding a KuzuDB connection
        
    Example:
        with safe_get_kuzu_connection(user_id="user123", operation="book_import") as conn:
            conn.execute("CREATE (b:Book {title: $title})", {"title": "Test"})
            result = conn.execute("MATCH (b:Book) RETURN count(b)")
    """
    try:
        # Import here to avoid circular imports
        from ..utils.safe_kuzu_manager import safe_get_connection
        return safe_get_connection(user_id, operation)
    except ImportError:
        logger.error("🚨 CRITICAL: safe_get_connection not available! Using dangerous fallback.")
        # Fallback to dangerous global (temporary during migration)
        db = get_kuzu_database()
        return db.connect()
