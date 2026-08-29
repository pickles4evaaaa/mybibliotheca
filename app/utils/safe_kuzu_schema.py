"""Schema initialization implementation for SafeKuzuManager.

This module is deliberately a code-organization boundary.  The SQL/Cypher
statements and execution order are copied unchanged from the manager; no
schema migration or upgrade behavior is introduced here.
"""

from __future__ import annotations

import logging
import os

import kuzu  # type: ignore


logger = logging.getLogger("app.utils.safe_kuzu_manager")


def initialize_schema(manager) -> None:
    """Initialize the graph schema with node and relationship tables."""
    self = manager
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
                opds_source_id STRING,
                opds_source_updated_at STRING,
                opds_source_entry_hash STRING,
                media_type STRING,
                average_rating DOUBLE,
                rating_count INT64,
                series STRING,
                series_volume STRING,
                series_order INT64,
                quantity INT64,
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
        relationship_queries = []
        import os as _os_dep_owns_mgr
        if _os_dep_owns_mgr.getenv('ENABLE_OWNS_SCHEMA', 'false').lower() in ('1', 'true', 'yes'):  # pragma: no cover
            relationship_queries.append(
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
                """
            )
        
        relationship_queries += [
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

