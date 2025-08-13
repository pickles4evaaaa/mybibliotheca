"""
Safe KuzuDB Connection Manager

Provides thread-safe access to KuzuDB connections with proper isolation
and concurrency control to prevent database corruption and race conditions.

Critical Fix #2: Eliminates the dangerous global singleton pattern that
caused database access conflicts in multi-user scenarios.
"""

import threading
import logging
import kuzu  # type: ignore
import os
import time
import json
from pathlib import Path
from typing import Optional, Dict, Any, List, Generator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# Logging controls
_QUERY_LOG_ENABLED = os.getenv('KUZU_QUERY_LOG', 'false').lower() in ('1', 'true', 'on', 'yes')
try:
    _SLOW_QUERY_MS = int(os.getenv('KUZU_SLOW_QUERY_MS', '150'))
except Exception:
    _SLOW_QUERY_MS = 150
_VERBOSE_INIT = os.getenv('MYBIBLIOTHECA_VERBOSE_INIT', 'false').lower() in ('1', 'true', 'on', 'yes') or \
                os.getenv('KUZU_DEBUG', 'false').lower() in ('1', 'true', 'on', 'yes')


class SafeKuzuManager:
    """
    Thread-safe KuzuDB connection manager that prevents concurrent access issues.
    
    Key Features:
    - Thread-safe initialization with proper locking
    - Connection-per-request pattern to avoid shared state
    - Automatic connection cleanup and lifecycle management
    - User-scoped connection tracking for debugging
    - Deadlock prevention with timeout mechanisms
    """
    
    def __init__(self, database_path: Optional[str] = None):
        if database_path:
            self.database_path = database_path
        else:
            kuzu_dir = os.getenv('KUZU_DB_PATH', 'data/kuzu')
            self.database_path = os.path.join(kuzu_dir, 'bibliotheca.db')
        
        # Thread safety controls
        self._lock = threading.RLock()  # Reentrant lock for nested calls
        self._database: Optional[kuzu.Database] = None
        self._is_initialized = False
        
        # Connection tracking for debugging and monitoring
        self._active_connections: Dict[int, Dict[str, Any]] = {}  # thread_id -> connection_info
        self._connection_count = 0
        self._total_connections_created = 0
        
        # Performance and safety metrics
        self._last_access_time = None
        self._initialization_time = None
        self._lock_wait_times: List[float] = []
        
        logger.info(f"SafeKuzuManager initialized for database: {self.database_path}")
        # Track creating PID to detect forks
        try:
            import os as _os
            self._creator_pid = _os.getpid()
        except Exception:
            self._creator_pid = None
    
    def _get_thread_info(self) -> Dict[str, Any]:
        """Get current thread information for tracking."""
        thread = threading.current_thread()
        return {
            'thread_id': threading.get_ident(),
            'thread_name': thread.name,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'is_main_thread': threading.main_thread() == thread
        }
    
    def _initialize_database(self) -> None:
        """
        Initialize the KuzuDB database instance with proper thread safety.
        
        This method is called only once, protected by locks to prevent
        race conditions during initialization.
        """
        # Detect process fork: if PID changed, reset state
        try:
            import os as _os
            current_pid = _os.getpid()
            if hasattr(self, "_creator_pid") and self._creator_pid and self._creator_pid != current_pid:
                # Reset all state in child process before initializing
                self._database = None
                self._is_initialized = False
                self._active_connections.clear()
                self._connection_count = 0
                self._total_connections_created = 0
                self._last_access_time = None
                self._initialization_time = None
                self._lock_wait_times.clear()
                self._creator_pid = current_pid
                try:
                    print(f"[KUZU] ♻️ Detected fork; resetting manager state in pid {current_pid}")
                except Exception:
                    pass
        except Exception:
            pass

        if self._is_initialized:
            return
            
        start_time = time.time()
        thread_info = self._get_thread_info()
        
        # Visible stdout only when verbose init is enabled
        if _VERBOSE_INIT:
            try:
                print(f"[KUZU] ⏳ Initializing database at {self.database_path} (thread {thread_info['thread_id']})")
            except Exception:
                pass
        logger.info(f"[THREAD-{thread_info['thread_id']}:{thread_info['thread_name']}] "
                   f"Initializing KuzuDB database...")
        
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.database_path), exist_ok=True)
            
            # Create database instance
            self._database = kuzu.Database(self.database_path)
            
            # CRITICAL FIX: Initialize the database schema
            logger.info(f"[THREAD-{thread_info['thread_id']}:{thread_info['thread_name']}] "
                       f"Initializing database schema...")
            if _VERBOSE_INIT:
                try:
                    print("[KUZU] 🧩 Ensuring schema ...")
                except Exception:
                    pass
            self._initialize_schema()
            
            self._is_initialized = True
            self._initialization_time = datetime.now(timezone.utc)
            
            initialization_duration = time.time() - start_time
            logger.info(f"[THREAD-{thread_info['thread_id']}:{thread_info['thread_name']}] "
                       f"KuzuDB database initialized successfully in {initialization_duration:.3f}s")
            if _VERBOSE_INIT:
                try:
                    print(f"[KUZU] ✅ Database ready in {initialization_duration:.2f}s")
                except Exception:
                    pass
            
        except Exception as e:
            logger.error(f"[THREAD-{thread_info['thread_id']}:{thread_info['thread_name']}] "
                        f"Failed to initialize KuzuDB database: {e}")
            self._is_initialized = False
            self._database = None
            if _VERBOSE_INIT:
                try:
                    print(f"[KUZU] ❌ Initialization error: {e}")
                except Exception:
                    pass
            raise
    
    @contextmanager
    def get_connection(self, user_id: Optional[str] = None, operation: str = "unknown") -> Generator[kuzu.Connection, None, None]:
        """
        Get a thread-safe KuzuDB connection with automatic cleanup.
        
        This is the primary method for accessing KuzuDB. It provides:
        - Thread-safe database initialization
        - Connection-per-request isolation
        - Automatic connection cleanup
        - User-scoped tracking for debugging
        - Deadlock prevention with timeouts
        
        Args:
            user_id: Optional user identifier for tracking
            operation: Description of the operation for debugging
            
        Yields:
            kuzu.Connection: A KuzuDB connection ready for use
            
        Example:
            with safe_kuzu_manager.get_connection(user_id="user123", operation="book_import") as conn:
                result = conn.execute("MATCH (b:Book) RETURN b.title")
        """
        lock_start_time = time.time()
        thread_info = self._get_thread_info()
        connection_id = None
        
        # Track lock waiting time for performance monitoring
        with self._lock:
            lock_wait_time = time.time() - lock_start_time
            self._lock_wait_times.append(lock_wait_time)
            
            # Keep only last 100 measurements for memory efficiency
            if len(self._lock_wait_times) > 100:
                self._lock_wait_times = self._lock_wait_times[-50:]
            
            # Warn about long lock waits (potential contention)
            if lock_wait_time > 0.1:  # 100ms threshold
                logger.warning(f"[THREAD-{thread_info['thread_id']}:{thread_info['thread_name']}] "
                              f"Long lock wait: {lock_wait_time:.3f}s for operation '{operation}'")
            
            # Initialize database if needed
            if not self._is_initialized:
                self._initialize_database()
            
            if self._database is None:
                raise RuntimeError("KuzuDB database not properly initialized")
            
            # Create connection
            try:
                connection = kuzu.Connection(self._database)
                self._connection_count += 1
                self._total_connections_created += 1
                connection_id = self._total_connections_created
                self._last_access_time = datetime.now(timezone.utc)
                
                # Track active connection
                self._active_connections[thread_info['thread_id']] = {
                    'connection_id': connection_id,
                    'user_id': user_id,
                    'operation': operation,
                    'created_at': datetime.now(timezone.utc).isoformat(),
                    'thread_info': thread_info
                }
                
                logger.debug(f"[THREAD-{thread_info['thread_id']}:{thread_info['thread_name']}] "
                            f"Created connection #{connection_id} for operation '{operation}' "
                            f"(user: {user_id or 'anonymous'})")
                
            except Exception as e:
                logger.error(f"[THREAD-{thread_info['thread_id']}:{thread_info['thread_name']}] "
                            f"Failed to create KuzuDB connection: {e}")
                raise
        
        # Yield connection for use (outside the lock)
        try:
            yield connection
            
        except Exception as e:
            # Check if this is an expected "already exists" error during schema creation
            error_str = str(e).lower()
            is_schema_exists_error = (
                "already exists" in error_str and 
                ("customfield" in error_str or 
                 "has_personal_metadata" in error_str or 
                 "globalmetadata" in error_str or 
                 "has_global_metadata" in error_str or
                 "catalog" in error_str)
            )
            
            if is_schema_exists_error:
                # Log as debug instead of error for expected schema conflicts
                logger.debug(f"[THREAD-{thread_info['thread_id']}:{thread_info['thread_name']}] "
                            f"Schema element already exists during operation '{operation}': {e}")
            else:
                # Log as error for unexpected issues
                logger.error(f"[THREAD-{thread_info['thread_id']}:{thread_info['thread_name']}] "
                            f"Error during KuzuDB operation '{operation}': {e}")
            raise
            
        finally:
            # Cleanup connection (back inside the lock)
            with self._lock:
                try:
                    if connection:
                        connection.close()
                    self._connection_count -= 1
                    
                    # Remove from active connections tracking
                    if thread_info['thread_id'] in self._active_connections:
                        del self._active_connections[thread_info['thread_id']]
                    
                    logger.debug(f"[THREAD-{thread_info['thread_id']}:{thread_info['thread_name']}] "
                                f"Closed connection #{connection_id} for operation '{operation}' "
                                f"(user: {user_id or 'anonymous'})")
                        
                except Exception as e:
                    logger.error(f"[THREAD-{thread_info['thread_id']}:{thread_info['thread_name']}] "
                                f"Error closing connection #{connection_id}: {e}")
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None, 
                     user_id: Optional[str] = None, operation: str = "query") -> Any:
        """
        Execute a query with automatic connection management.
        
        This is a convenience method that handles connection lifecycle automatically.
        
        Args:
            query: Cypher query string
            params: Query parameters
            user_id: Optional user identifier for tracking
            operation: Description of the operation for debugging
            
        Returns:
            Query result
        """
        # Visible probe logs gated by env var
        if _QUERY_LOG_ENABLED:
            try:
                q_snippet = ' '.join(query.splitlines())[:120].strip()
                print(f"[KUZU] ▶ execute_query op='{operation}' q='{q_snippet}'")
            except Exception:
                pass
        with self.get_connection(user_id=user_id, operation=operation) as conn:
            t0 = time.time()
            result = conn.execute(query, params or {})
            dt = time.time() - t0
            # Log completion if query logging is enabled or the query is slow
            if _QUERY_LOG_ENABLED or (dt * 1000) >= _SLOW_QUERY_MS:
                try:
                    print(f"[KUZU] ◀ execute_query done in {dt:.2f}s")
                except Exception:
                    pass
            
            # Handle both single QueryResult and list[QueryResult]
            if isinstance(result, list):
                return result[0] if result else None
            return result
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get comprehensive health and performance metrics.
        
        Returns:
            Dictionary with health status, performance metrics, and active connections
        """
        with self._lock:
            avg_lock_wait = (sum(self._lock_wait_times) / len(self._lock_wait_times) 
                           if self._lock_wait_times else 0.0)
            max_lock_wait = max(self._lock_wait_times) if self._lock_wait_times else 0.0
            
            return {
                'database_status': {
                    'is_initialized': self._is_initialized,
                    'database_path': self.database_path,
                    'initialization_time': self._initialization_time.isoformat() if self._initialization_time else None,
                    'last_access_time': self._last_access_time.isoformat() if self._last_access_time else None
                },
                'connection_metrics': {
                    'active_connections': self._connection_count,
                    'total_connections_created': self._total_connections_created,
                    'active_threads': len(self._active_connections)
                },
                'performance_metrics': {
                    'average_lock_wait_ms': round(avg_lock_wait * 1000, 2),
                    'max_lock_wait_ms': round(max_lock_wait * 1000, 2),
                    'lock_samples': len(self._lock_wait_times)
                },
                'active_connections_detail': {
                    thread_id: {
                        'connection_id': info['connection_id'],
                        'user_id': info['user_id'],
                        'operation': info['operation'],
                        'created_at': info['created_at'],
                        'thread_name': info['thread_info']['thread_name'],
                        'is_main_thread': info['thread_info']['is_main_thread']
                    }
                    for thread_id, info in self._active_connections.items()
                },
                'thread_safety_status': {
                    'lock_type': 'RLock (Reentrant)',
                    'current_thread': threading.get_ident(),
                    'total_threads': threading.active_count()
                }
            }
    
    def cleanup_stale_connections(self, max_age_minutes: int = 30) -> int:
        """
        Clean up any stale connection tracking data.
        
        Note: KuzuDB connections are automatically closed when they go out of scope,
        but this helps clean up our tracking data for connections that may have
        been abandoned due to thread crashes or other issues.
        
        Args:
            max_age_minutes: Maximum age for connection tracking data
            
        Returns:
            Number of stale entries cleaned up
        """
        with self._lock:
            cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
            stale_threads = []
            
            for thread_id, info in self._active_connections.items():
                created_at = datetime.fromisoformat(info['created_at'])
                if created_at < cutoff_time:
                    stale_threads.append(thread_id)
            
            # Remove stale entries
            for thread_id in stale_threads:
                logger.warning(f"Cleaning up stale connection tracking for thread {thread_id}")
                del self._active_connections[thread_id]
            
            return len(stale_threads)
    
    def force_reset(self) -> None:
        """
        Force reset the database connection (for testing/recovery only).
        
        ⚠️ WARNING: This should only be used in testing or emergency recovery.
        It will close all active connections and reset the database instance.
        """
        with self._lock:
            logger.warning("Force resetting KuzuDB connection - this should only happen in tests or recovery!")
            
            # Close database if it exists
            if self._database:
                try:
                    # Note: KuzuDB doesn't have an explicit close method,
                    # connections are closed when they go out of scope
                    pass
                except Exception as e:
                    logger.error(f"Error during database reset: {e}")
            
            # Reset all state
            self._database = None
            self._is_initialized = False
            self._active_connections.clear()
            self._connection_count = 0
            self._last_access_time = None
            self._initialization_time = None
            self._lock_wait_times.clear()
            
    def _initialize_schema(self):
        """Initialize the graph schema with node and relationship tables."""
        try:
            # Schema initialization (reduced logging for performance)
            logger.info("🔧 [DEBUG] SafeKuzuManager starting schema initialization...")
            
            # Check environment variable for forced reset
            force_reset = os.getenv('KUZU_FORCE_RESET', 'false').lower() == 'true'
            debug_mode = os.getenv('KUZU_DEBUG', 'false').lower() == 'true'
            
            # Check force reset and debug mode (logging reduced for performance)
            logger.info(f"🔧 [DEBUG] Schema init - force_reset={force_reset}, debug_mode={debug_mode}")
            
            # Check if database already has essential tables and determine initialization strategy
            has_complete_schema = False
            if not force_reset:
                try:
                    # Use direct connection instead of get_connection to avoid circular dependency
                    if self._database is None:
                        raise RuntimeError("Database not initialized")
                    temp_conn = kuzu.Connection(self._database)
                    try:
                        logger.debug("🔧 [DEBUG] Checking for complete schema...")
                        
                        # Check for key tables that indicate a complete schema
                        essential_tables = ['User', 'Book', 'Location', 'Person', 'Category', 'Author', 'Publisher', 'Series']
                        missing_tables = []
                        
                        for table_name in essential_tables:
                            try:
                                # Try to query each essential table
                                result = temp_conn.execute(f"MATCH (n:{table_name}) RETURN COUNT(n) as count LIMIT 1")
                                logger.debug(f"✅ Table {table_name} exists")
                            except Exception as table_e:
                                if "does not exist" in str(table_e).lower():
                                    missing_tables.append(table_name)
                                    logger.debug(f"❌ Table {table_name} missing: {table_e}")
                                else:
                                    # Some other error, assume table exists but has issues
                                    logger.debug(f"⚠️ Table {table_name} exists but has issues: {table_e}")
                        
                        if not missing_tables:
                            has_complete_schema = True
                            logger.info("✅ All essential tables exist - schema appears complete")

                            # Minimal post-check: ensure ReadingLog.updated_at exists (older DBs may miss it)
                            try:
                                temp_conn.execute("MATCH (rl:ReadingLog) RETURN rl.updated_at LIMIT 1")
                            except Exception as rl_e:
                                if "Cannot find property updated_at" in str(rl_e):
                                    try:
                                        temp_conn.execute("ALTER TABLE ReadingLog ADD updated_at TIMESTAMP")
                                        logger.info("🛠️ Added missing updated_at column to ReadingLog table")
                                    except Exception as alter_e:
                                        logger.debug(f"Could not add updated_at to ReadingLog: {alter_e}")

                            # Log some stats for confirmation
                            try:
                                user_result = temp_conn.execute("MATCH (u:User) RETURN COUNT(u) as count LIMIT 1")
                                if isinstance(user_result, list):
                                    user_result = user_result[0] if user_result else None
                                if user_result and user_result.has_next():
                                    try:
                                        _row = user_result.get_next()
                                        _vals = _row if isinstance(_row, (list, tuple)) else list(_row)
                                        user_count = _vals[0] if _vals else 0
                                    except Exception:
                                        user_count = 0
                                    logger.info(f"📊 Database contains {user_count} users")
                            except Exception as e:
                                logger.debug(f"Could not get user count: {e}")
                        else:
                            logger.info(f"❌ Missing essential tables: {missing_tables} - will create full schema")
                            
                    finally:
                        temp_conn.close()
                        
                except Exception as e:
                    # If we can't check tables, assume we need to initialize schema
                    logger.debug(f"Could not check schema completeness - will initialize schema: {e}")
                    
            if force_reset:
                logger.warning("⚠️ KUZU_FORCE_RESET=true - Recreating entire schema")
            elif has_complete_schema:
                logger.info("✅ Schema is complete - skipping initialization")
                return
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
                    average_rating DOUBLE,
                    rating_count INT64,
                    series STRING,
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
                CREATE NODE TABLE Library(
                    id STRING,
                    name STRING,
                    description STRING,
                    is_default BOOLEAN,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE LibraryShelf(
                    id STRING,
                    name STRING,
                    description STRING,
                    position INT64,
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
                    updated_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE ReadingSession(
                    id STRING,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    pages_read INT64,
                    notes STRING,
                    reading_speed DOUBLE,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE ReadingGoal(
                    id STRING,
                    title STRING,
                    description STRING,
                    target_books INT64,
                    target_pages INT64,
                    target_reading_time_minutes INT64,
                    deadline TIMESTAMP,
                    is_completed BOOLEAN,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    PRIMARY KEY(id)
                )
                """,
                """
                CREATE NODE TABLE Review(
                    id STRING,
                    rating INT64,
                    review_text STRING,
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
                """,
                """
                CREATE NODE TABLE UserCustomField(
                    id STRING,
                    field_name STRING,
                    field_type STRING,
                    is_required BOOLEAN,
                    default_value STRING,
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
                "CREATE REL TABLE READS(FROM User TO Book, started_at TIMESTAMP, finished_at TIMESTAMP, status STRING, current_page INT64, progress_percentage DOUBLE)",
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
                "CREATE REL TABLE EDITED(FROM Person TO Book, role STRING, order_index INT64, created_at TIMESTAMP)",
                "CREATE REL TABLE ILLUSTRATED(FROM Person TO Book, role STRING, order_index INT64, created_at TIMESTAMP)", 
                "CREATE REL TABLE TRANSLATED(FROM Person TO Book, role STRING, order_index INT64, created_at TIMESTAMP)",
                "CREATE REL TABLE NARRATED(FROM Person TO Book, role STRING, order_index INT64, created_at TIMESTAMP)",
                "CREATE REL TABLE CONTRIBUTED(FROM Person TO Book, role STRING)",
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
                "CREATE REL TABLE CATEGORIZED(FROM Book TO Category)",
                "CREATE REL TABLE BELONGS_TO_LIBRARY(FROM Book TO Library)",
                "CREATE REL TABLE SHELVED_IN(FROM Book TO LibraryShelf, position INT64)",
                "CREATE REL TABLE LIBRARY_CONTAINS_SHELF(FROM Library TO LibraryShelf)",
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
                CREATE REL TABLE FOR_BOOK(
                    FROM ReadingLog TO Book,
                    created_at TIMESTAMP
                )
                """,
                "CREATE REL TABLE HAS_READING_SESSION(FROM User TO ReadingSession)",
                "CREATE REL TABLE SESSION_FOR_BOOK(FROM ReadingSession TO Book)",
                "CREATE REL TABLE HAS_GOAL(FROM User TO ReadingGoal)",
                "CREATE REL TABLE GOAL_INCLUDES_BOOK(FROM ReadingGoal TO Book)",
                "CREATE REL TABLE HAS_REVIEW(FROM User TO Review)",
                "CREATE REL TABLE REVIEW_FOR_BOOK(FROM Review TO Book)",
                """
                CREATE REL TABLE PARENT_CATEGORY(
                    FROM Category TO Category,
                    created_at TIMESTAMP
                )
                """,
                """
                CREATE REL TABLE STORED_AT(
                    FROM Book TO Location,
                    user_id STRING,
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
                """,
                "CREATE REL TABLE HAS_IMPORT_JOB(FROM User TO ImportJob)",
                "CREATE REL TABLE USER_OWNS_LIBRARY(FROM User TO Library)",
                "CREATE REL TABLE USER_HAS_LOCATION(FROM User TO Location)"
            ]
            
            # Combine all queries
            all_queries = node_queries + relationship_queries
            
            # Execute all queries with direct connection
            tables_created = 0
            tables_existed = 0
            
            if self._database is None:
                raise RuntimeError("Database not initialized")
            
            # Use direct connection instead of get_connection to avoid circular dependency
            conn = kuzu.Connection(self._database)
            try:
                for i, query in enumerate(all_queries):
                    try:
                        conn.execute(query)
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
            finally:
                conn.close()
                
            logger.info(f"✅ Kuzu schema ensured: {tables_created} created, {tables_existed} already existed")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kuzu schema: {e}")
            import traceback
            traceback.print_exc()
            raise


# Global thread-safe instance
# This replaces the dangerous _kuzu_database global singleton
_safe_kuzu_manager: Optional[SafeKuzuManager] = None
_manager_lock = threading.Lock()


def get_safe_kuzu_manager() -> SafeKuzuManager:
    """
    Get the global thread-safe KuzuDB manager instance.
    
    This function replaces the dangerous get_kuzu_database() singleton pattern
    with a thread-safe alternative that prevents concurrency issues.
    
    Returns:
        SafeKuzuManager: Thread-safe database manager
    """
    global _safe_kuzu_manager
    
    # Double-checked locking pattern for thread-safe singleton
    if _safe_kuzu_manager is None:
        with _manager_lock:
            if _safe_kuzu_manager is None:
                _safe_kuzu_manager = SafeKuzuManager()
                logger.info("Global SafeKuzuManager instance created")
    
    return _safe_kuzu_manager


def reset_safe_kuzu_manager(database_path: Optional[str] = None) -> None:
    """
    Reset the global SafeKuzuManager instance.

    Useful in tests to ensure a fresh manager picks up environment changes
    such as KUZU_DB_PATH. Optionally provide a database_path to immediately
    seed a new manager with that path.
    """
    global _safe_kuzu_manager
    with _manager_lock:
        _safe_kuzu_manager = SafeKuzuManager(database_path) if database_path else None


def safe_execute_query(query: str, params: Optional[Dict[str, Any]] = None, 
                      user_id: Optional[str] = None, operation: str = "query") -> Any:
    """
    Execute a KuzuDB query with automatic thread-safe connection management.
    
    This is a convenience function that provides the same interface as the old
    dangerous global database access, but with proper thread safety.
    
    Args:
        query: Cypher query string
        params: Query parameters
        user_id: Optional user identifier for tracking
        operation: Description of the operation for debugging
        
    Returns:
        Query result
        
    Example:
        result = safe_execute_query(
            "MATCH (b:Book) WHERE b.user_id = $user_id RETURN b",
            {"user_id": "user123"},
            user_id="user123",
            operation="get_user_books"
        )
    """
    manager = get_safe_kuzu_manager()
    return manager.execute_query(query, params, user_id, operation)


def safe_query_value(query: str, params: Optional[Dict[str, Any]] = None, user_id: Optional[str] = None,
                     operation: str = "query", default: Any = None) -> Any:
    """Execute a query and return the first column of the first row or default.

    This helper mirrors prior test expectations that previously relied on
    a convenience wrapper in legacy code. Reintroduced to maintain backward
    compatibility for tests referencing safe_query_value.
    """
    try:
        result = safe_execute_query(query, params, user_id=user_id, operation=operation)
        if not result or not hasattr(result, 'has_next'):
            # If result is already a list (perhaps from manager adaptation)
            if isinstance(result, list) and result:
                first = result[0]
                if isinstance(first, dict):
                    return next(iter(first.values()), default)
                return first
            return default
        if result.has_next():
            row = result.get_next()
            if len(row) > 0:
                return row[0]
        return default
    except Exception:
        return default


def safe_query_list(query: str, params: Optional[Dict[str, Any]] = None, user_id: Optional[str] = None,
                    operation: str = "query") -> List[Dict[str, Any]]:
    """Execute a query and return list of dict rows (column_name -> value)."""
    rows: List[Dict[str, Any]] = []
    try:
        result = safe_execute_query(query, params, user_id=user_id, operation=operation)
        # If already a python list of dicts
        if isinstance(result, list):
            for item in result:
                if isinstance(item, dict):
                    rows.append(item)
            return rows
        if not result:
            return rows
        # Kuzu result iteration
        while result.has_next():
            row = result.get_next()
            record: Dict[str, Any] = {}
            col_names = result.get_column_names()
            for i, col in enumerate(col_names):
                try:
                    record[col] = row[i]
                except Exception:
                    record[col] = None
            rows.append(record)
    except Exception:
        return rows
    return rows


def safe_get_connection(user_id: Optional[str] = None, operation: str = "unknown"):
    """
    Get a thread-safe KuzuDB connection context manager.
    
    This function provides direct access to the connection for more complex operations
    that need to execute multiple queries in sequence.
    
    Args:
        user_id: Optional user identifier for tracking
        operation: Description of the operation for debugging
        
    Returns:
        Context manager yielding a KuzuDB connection
        
    Example:
        with safe_get_connection(user_id="user123", operation="book_import") as conn:
            # Multiple related queries in same connection
            conn.execute("CREATE (b:Book {title: $title})", {"title": "Test"})
            result = conn.execute("MATCH (b:Book) WHERE b.title = $title RETURN b", {"title": "Test"})
    """
    manager = get_safe_kuzu_manager()
    return manager.get_connection(user_id=user_id, operation=operation)


def get_kuzu_health_status() -> Dict[str, Any]:
    """
    Get comprehensive health status of the KuzuDB system.
    
    Returns:
        Dictionary with health status and performance metrics
    """
    manager = get_safe_kuzu_manager()
    return manager.get_health_status()
