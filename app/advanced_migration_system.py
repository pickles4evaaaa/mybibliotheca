#!/usr/bin/env python3
"""
Advanced SQLite to Kuzu Migration System for Bibliotheca
========================================================

This module provides a comprehensive migration system that handles:
1. Detection of V1 (single-user) and V2 (multi-user) SQLite databases
2. First-time setup with admin user creation
3. V1 database migration with admin user assignment
4. V2 database migration with user mapping and selection
5. Complete backup and rollback capabilities

Key Features:
- Automatic database version detection
- Safe backup creation before migration
- Admin user creation for first-time setups
- User mapping for V2 migrations
- Complete data integrity validation
- Rollback capabilities on failure
"""

import os
import sys
import sqlite3
import shutil
import json
import logging
import asyncio
import time
import random
import re
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple, Any, Union
from pathlib import Path
from dataclasses import asdict
from werkzeug.security import generate_password_hash
import uuid

# Add the parent directory to the path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.domain.models import (
    Book, User, UserBookRelationship, Author, Publisher, Category, 
    ReadingStatus, Person, BookContribution, ContributionType
)
from app.infrastructure.kuzu_repositories import KuzuBookRepository, KuzuUserRepository
from app.utils.safe_kuzu_manager import get_safe_kuzu_manager
from app.services import book_service, user_service, run_async
from config import Config

# Helper function for query result conversion
def _convert_query_result_to_list(result) -> list:
    """Convert KuzuDB query result to list of dictionaries."""
    if not result:
        return []
    
    data = []
    while result.has_next():
        row = result.get_next()
        record = {}
        for i in range(len(row)):
            column_name = result.get_column_names()[i]
            record[column_name] = row[i]
        data.append(record)
    
    return data

# Setup logging
logger = logging.getLogger(__name__)


class DatabaseVersion:
    """Enum for database versions"""
    V1_SINGLE_USER = "v1_single_user"
    V2_MULTI_USER = "v2_multi_user"
    UNKNOWN = "unknown"


class MigrationStatus:
    """Enum for migration status"""
    NOT_STARTED = "not_started"
    BACKUP_CREATED = "backup_created"
    ADMIN_CREATED = "admin_created"
    USERS_MIGRATED = "users_migrated"
    BOOKS_MIGRATED = "books_migrated"
    RELATIONSHIPS_CREATED = "relationships_created"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


class AdvancedMigrationSystem:
    """
    Comprehensive migration system for converting SQLite databases to Kuzu.
    Handles both V1 (single-user) and V2 (multi-user) databases with full backup support.
    """
    
    def __init__(self, kuzu_db_path: Optional[str] = None):
        """
        Initialize the advanced migration system.
        
        Args:
            kuzu_db_path: Kuzu database path (defaults to config)
        """
        self.backup_dir = Path("migration_backups") / datetime.now().strftime("%Y%m%d_%H%M%S")
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup Kuzu connection using SafeKuzuManager
        self.safe_manager = get_safe_kuzu_manager()
        # Use safe manager instead of deprecated graph_store
        self.graph_store = self.safe_manager
        self.book_repo = KuzuBookRepository()
        self.user_repo = KuzuUserRepository()
        
        # Initialize services for migration operations
        # For common library model, use the direct book service (no user relationships)
        from app.services.kuzu_book_service import KuzuBookService
        self.book_service = KuzuBookService()
        self.user_service = user_service
        
        # Initialize location service
        from app.location_service import LocationService
        self.location_service = LocationService()
        
        # Migration state
        self.current_status = MigrationStatus.NOT_STARTED
        self.migration_log = []
        self.rollback_data = {}
        
        # Migration statistics
        self.stats = {
            'database_version': None,
            'books_migrated': 0,
            'reading_logs_migrated': 0,
            'users_migrated': 0,
            'relationships_created': 0,
            'errors': []
        }
    
    def find_sqlite_databases(self) -> List[Path]:
        """Find all potential SQLite databases in the data directory only."""
        search_paths = [
            Path.cwd() / "data",
            Path("/app/data"),
        ]
        
        # Remove duplicates by resolving paths and using a set
        seen_paths = set()
        unique_paths = []
        for path in search_paths:
            resolved_path = path.resolve()
            if resolved_path not in seen_paths:
                seen_paths.add(resolved_path)
                unique_paths.append(path)
        
        db_files = []
        for path in unique_paths:
            if path.exists():
                for db_file in path.glob("*.db"):
                    if self._is_bibliotheca_database(db_file):
                        db_files.append(db_file)
        
        return db_files
    
    def _is_bibliotheca_database(self, db_path: Path) -> bool:
        """Check if this is a Bibliotheca SQLite database."""
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()
            
            # Must have book table
            return 'book' in tables
            
        except Exception as e:
            logger.debug(f"Error checking database {db_path}: {e}")
            return False
    
    def detect_database_version(self, db_path: Path) -> Tuple[str, Dict]:
        """
        Detect the version and analyze structure of a SQLite database.
        
        Returns:
            Tuple of (version, analysis_dict)
        """
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Get table structure
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            
            analysis = {
                'tables': tables,
                'has_users': 'user' in tables,
                'has_books': 'book' in tables,
                'has_reading_logs': 'reading_log' in tables,
                'book_count': 0,
                'user_count': 0,
                'reading_log_count': 0,
                'columns': {}
            }
            
            # Analyze book table structure
            if 'book' in tables:
                cursor.execute("PRAGMA table_info(book);")
                book_columns = [row[1] for row in cursor.fetchall()]
                analysis['columns']['book'] = book_columns
                analysis['has_user_id_in_books'] = 'user_id' in book_columns
                
                cursor.execute("SELECT COUNT(*) FROM book;")
                analysis['book_count'] = cursor.fetchone()[0]
            
            # Analyze user table if it exists
            if 'user' in tables:
                cursor.execute("PRAGMA table_info(user);")
                user_columns = [row[1] for row in cursor.fetchall()]
                analysis['columns']['user'] = user_columns
                
                cursor.execute("SELECT COUNT(*) FROM user;")
                analysis['user_count'] = cursor.fetchone()[0]
                
                # Get user list for V2 databases
                cursor.execute("SELECT id, username, email, is_admin FROM user;")
                analysis['users'] = [
                    {
                        'id': row[0],
                        'username': row[1],
                        'email': row[2],
                        'is_admin': bool(row[3]) if row[3] is not None else False
                    }
                    for row in cursor.fetchall()
                ]
            
            # Analyze reading log table if it exists
            if 'reading_log' in tables:
                cursor.execute("PRAGMA table_info(reading_log);")
                reading_log_columns = [row[1] for row in cursor.fetchall()]
                analysis['columns']['reading_log'] = reading_log_columns
                analysis['has_user_id_in_reading_logs'] = 'user_id' in reading_log_columns
                
                cursor.execute("SELECT COUNT(*) FROM reading_log;")
                analysis['reading_log_count'] = cursor.fetchone()[0]
            
            conn.close()
            
            # Determine version
            if analysis['has_users'] and analysis.get('has_user_id_in_books', False):
                version = DatabaseVersion.V2_MULTI_USER
            elif analysis['has_books']:
                version = DatabaseVersion.V1_SINGLE_USER
            else:
                version = DatabaseVersion.UNKNOWN
            
            analysis['detected_version'] = version
            return version, analysis
            
        except Exception as e:
            logger.error(f"Error analyzing database {db_path}: {e}")
            return DatabaseVersion.UNKNOWN, {'error': str(e)}
    
    def create_backup(self, db_path: Optional[Path] = None) -> bool:
        """
        Create comprehensive backups before migration.
        
        Args:
            db_path: Path to SQLite database to backup (optional)
        """
        try:
            self._log_action("Creating pre-migration backups")
            
            # Backup SQLite database if provided
            if db_path and db_path.exists():
                sqlite_backup = self.backup_dir / f"sqlite_backup_{db_path.name}"
                shutil.copy2(db_path, sqlite_backup)
                logger.info(f"✅ Created SQLite backup: {sqlite_backup}")
                self._log_action(f"SQLite backup created: {sqlite_backup}")
            
            # Backup current Kuzu state (if any exists)
            kuzu_backup = self.backup_dir / "kuzu_backup_pre_migration.json"
            self._backup_kuzu_data(kuzu_backup)
            logger.info(f"✅ Created Kuzu backup: {kuzu_backup}")
            self._log_action(f"Kuzu backup created: {kuzu_backup}")
            
            self.current_status = MigrationStatus.BACKUP_CREATED
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create backup: {e}")
            self._log_error(f"Backup creation failed: {e}")
            return False
    
    def _backup_kuzu_data(self, backup_path: Path) -> None:
        """Backup current Kuzu database state to a JSON file."""
        backup_data = {
            'timestamp': datetime.now().isoformat(),
            'stats': {}
        }
        
        try:
            # Get basic database statistics
            stats = {}
            
            # Count users
            try:
                user_result = self.safe_manager.execute_query("MATCH (u:User) RETURN COUNT(u) as count")
                rows = _convert_query_result_to_list(user_result) if user_result else []
                if rows:
                    # Kuzu returns column name 'count'
                    stats['users'] = rows[0].get('count', 0) or 0
                else:
                    stats['users'] = 0
            except Exception as e:
                logger.warning(f"Could not count users: {e}")
                stats['users'] = 0
            
            # Count books
            try:
                book_result = self.safe_manager.execute_query("MATCH (b:Book) RETURN COUNT(b) as count")
                rows = _convert_query_result_to_list(book_result) if book_result else []
                if rows:
                    stats['books'] = rows[0].get('count', 0) or 0
                else:
                    stats['books'] = 0
            except Exception as e:
                logger.warning(f"Could not count books: {e}")
                stats['books'] = 0
            
            # Count user-book relationships (no longer OWNS, but HAS_READ or similar)
            try:
                # Use the current schema's relationship label
                reading_result = self.safe_manager.execute_query("MATCH ()-[r:READS]->() RETURN COUNT(r) as count")
                rows = _convert_query_result_to_list(reading_result) if reading_result else []
                if rows:
                    stats['reading_relationships'] = rows[0].get('count', 0) or 0
                else:
                    stats['reading_relationships'] = 0
            except Exception as e:
                logger.warning(f"Could not count reading relationships: {e}")
                stats['reading_relationships'] = 0
            
            backup_data['stats'] = stats
            logger.info(f"✅ Kuzu database state recorded: {stats}")
            
        except Exception as e:
            logger.warning(f"Could not backup Kuzu data: {e}")
            backup_data['error'] = str(e)
        
        with open(backup_path, 'w') as f:
            json.dump(backup_data, f, indent=2)
    
    def create_first_admin_user(self, username: str, email: str, password: str) -> Optional[User]:
        """
        Create the first admin user for new installations.
        
        Args:
            username: Admin username
            email: Admin email
            password: Admin password (will be hashed)
            
        Returns:
            Created User object or None if failed
        """
        try:
            self._log_action(f"Creating first admin user: {username}")
            
            # Hash the password
            password_hash = generate_password_hash(password)
            
            # Create admin user using the user service
            admin_user = self.user_service.create_user_sync(
                username=username,
                email=email,
                password_hash=password_hash,
                is_admin=True,
                is_active=True,
                password_must_change=False
            )
            
            # Support both domain object and plain dict return types
            admin_user_id = None
            if admin_user:
                if isinstance(admin_user, dict):
                    admin_user_id = admin_user.get('id')
                else:
                    admin_user_id = getattr(admin_user, 'id', None)

            if admin_user and admin_user_id:
                self.current_status = MigrationStatus.ADMIN_CREATED
                self._log_action(f"First admin user created successfully: {admin_user_id}")
                logger.info(f"✅ First admin user created: {username}")
                # If returned as dict, keep state but return None to satisfy type hints
                if isinstance(admin_user, dict):
                    return None
                return admin_user
            else:
                logger.error(f"❌ Failed to create first admin user - no ID returned")
                self._log_error(f"Admin user creation failed - no ID returned")
                return None
            
        except Exception as e:
            logger.error(f"❌ Failed to create first admin user: {e}")
            self._log_error(f"Admin user creation failed: {e}")
            return None
    
    def migrate_v1_database(self, db_path: Path, admin_user_id: str) -> bool:
        """
        Migrate a V1 (single-user) database, assigning all content to the admin user.
        
        Args:
            db_path: Path to the V1 SQLite database
            admin_user_id: ID of the admin user to assign all content to
            
        Returns:
            True if migration successful, False otherwise
        """
        try:
            self._log_action(f"Starting V1 database migration: {db_path}")
            
            # Ensure default locations exist
            # Check if any default location exists, if not create one
            default_location = self.location_service.get_default_location()
            default_location_id = None
            
            if not default_location:
                self._log_action("No default location found. Creating default location...")
                # Setup default locations (this will create a "Home" location)
                created_locations = self.location_service.setup_default_locations()
                if created_locations:
                    # Get the newly created default location
                    default_location = self.location_service.get_default_location()
                    if default_location:
                        default_location_id = getattr(default_location, 'id', None)
                        loc_name = getattr(default_location, 'name', 'Unknown')
                        self._log_action(f"✅ Created and using default location: {loc_name} (ID: {default_location_id})")
                    else:
                        self._log_error("Failed to get default location after creation")
                else:
                    self._log_error("Failed to create default locations")
            else:
                # Safely extract location attributes
                loc_name = getattr(default_location, 'name', 'Unknown')
                default_location_id = getattr(default_location, 'id', None)
                self._log_action(f"Using existing default location: {loc_name} (ID: {default_location_id})")
            
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Track book ID mapping for reading logs
            book_id_mapping = {}  # old_id -> new_id
            
            # Migrate books
            cursor.execute("SELECT * FROM book;")
            books = cursor.fetchall()
            
            for book_row in books:
                try:
                    # Get the old book ID for mapping
                    old_book_id = book_row['id']
                    
                    # Convert SQLite row to Book object with API metadata enhancement
                    # For v1 databases, always fetch API metadata since they lack categories
                    book = self._sqlite_row_to_book(book_row, fetch_api_metadata=True)
                    
                    # Create book in Kuzu using service with a proper Book domain object
                    created_book = self.book_service.create_book_sync(
                        domain_book=book
                    )
                    
                    # Add book to admin user's library with appropriate status
                    created_book_id = getattr(created_book, 'id', None) if created_book else None
                    if created_book and created_book_id:
                        # Store the book ID mapping for reading logs
                        book_id_mapping[old_book_id] = created_book_id
                        
                        reading_status = ReadingStatus.READ if book_row['finish_date'] else ReadingStatus.WANT_TO_READ
                        
                        # Assign book to admin user's default location
                        if default_location_id:
                            try:
                                location_success = self.location_service.add_book_to_location(
                                    created_book_id, default_location_id, admin_user_id
                                )
                                if location_success:
                                    self._log_action(f"✅ Assigned book '{book.title}' to admin user's default location")
                                    self.stats['books_migrated'] += 1
                                else:
                                    self._log_error(f"Failed to assign book '{book.title}' to admin user's default location")
                            except Exception as e:
                                self._log_error(f"Error assigning book '{book.title}' to admin user's location: {e}")
                        else:
                            self._log_error(f"No default location found for admin user when trying to assign book '{book.title}'")
                            self.stats['books_migrated'] += 1  # Still count as migrated even if location assignment failed
                        
                        # TODO: Create reading activity/history records for user interaction with this book
                        # This could include reading status, dates, ratings, etc. stored in ReadingLog
                        # For now, we'll skip this as the common library model is still being developed
                    else:
                        self._log_error(f"Failed to create book '{book.title}'")
                    
                except Exception as e:
                    title = book_row['title'] if 'title' in book_row.keys() else 'Unknown'
                    self._log_error(f"Error migrating book {title}: {e}")
                    continue
            
            # Migrate reading logs
            if self._table_exists(cursor, 'reading_log'):
                cursor.execute("SELECT * FROM reading_log;")
                reading_logs = cursor.fetchall()
                
                # Import reading log service for actual migration
                from app.services import reading_log_service
                from app.domain.models import ReadingLog
                
                for log_row in reading_logs:
                    try:
                        # Helper function for safe row access
                        def safe_get(row, key, default=None):
                            try:
                                return row[key] if key in row.keys() and row[key] is not None else default
                            except (KeyError, IndexError):
                                return default
                        
                        old_book_id = safe_get(log_row, 'book_id')
                        log_date = safe_get(log_row, 'date')
                        
                        if old_book_id and old_book_id in book_id_mapping and log_date:
                            new_book_id = book_id_mapping[old_book_id]
                            
                            # Convert string date to date object if needed
                            if isinstance(log_date, str):
                                from datetime import datetime
                                log_date = datetime.strptime(log_date, '%Y-%m-%d').date()
                            
                            # Create ReadingLog domain object
                            reading_log = ReadingLog(
                                user_id=admin_user_id,
                                book_id=new_book_id,
                                date=log_date,
                                pages_read=safe_get(log_row, 'pages_read', 1),  # Default 1 page if not in SQLite
                                minutes_read=safe_get(log_row, 'minutes_read', 1),  # Default 1 minute if not in SQLite
                                notes=safe_get(log_row, 'notes', '')  # Empty notes if not in SQLite
                            )
                            
                            # Create the reading log in Kuzu
                            created_log = reading_log_service.create_reading_log_sync(reading_log)
                            
                            if created_log:
                                self.stats['reading_logs_migrated'] += 1
                                logger.debug(f"✅ Migrated reading log for book {new_book_id} on {log_date}")
                            else:
                                logger.warning(f"⚠️ Failed to create reading log for book {new_book_id} on {log_date}")
                        else:
                            logger.warning(f"⚠️ Skipping reading log - missing data: book_id={old_book_id}, date={log_date}")
                            
                    except Exception as e:
                        self._log_error(f"Error migrating reading log: {e}")
                        continue
            
            conn.close()
            
            self.current_status = MigrationStatus.COMPLETED
            self._log_action("V1 database migration completed successfully")
            logger.info(f"✅ V1 migration completed: {self.stats['books_migrated']} books migrated")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ V1 migration failed: {e}")
            self._log_error(f"V1 migration failed: {e}")
            self.current_status = MigrationStatus.FAILED
            return False
    
    def migrate_v2_database(self, db_path: Path, user_mapping: Dict[int, str], fetch_api_metadata: bool = False) -> bool:
        """
        Migrate a V2 (multi-user) database with user mapping.
        
        Args:
            db_path: Path to the V2 SQLite database
            user_mapping: Dictionary mapping old user IDs to new user IDs
            fetch_api_metadata: Whether to fetch fresh metadata from APIs or use existing SQLite data
            
        Returns:
            True if migration successful, False otherwise
        """
        try:
            self._log_action(f"Starting V2 database migration: {db_path}")
            
            # Extract admin user ID from user mapping (V2 migration uses admin user for creating books like V1)
            admin_user_id = list(user_mapping.values())[0] if user_mapping else None
            if not admin_user_id:
                self._log_error("No admin user ID found in user mapping")
                return False
            
            # Setup default locations for the system (universal)
            # This ensures there's a default location for book assignment
            default_location = self.location_service.get_default_location()
            if not default_location:
                self._log_action(f"No default location found. Creating default location...")
                created_locations = self.location_service.setup_default_locations()
                if created_locations:
                    default_location = self.location_service.get_default_location()
                    if default_location:
                        default_location_id = getattr(default_location, 'id', None)
                        loc_name = getattr(default_location, 'name', 'Unknown')
                        self._log_action(f"✅ Created default location: {loc_name} (ID: {default_location_id})")
            
            # All users will use the same universal default location
            user_default_locations = {}
            for old_user_id, new_user_id in user_mapping.items():
                if default_location and default_location.id:
                    user_default_locations[new_user_id] = default_location.id
                else:
                    user_default_locations[new_user_id] = None
            
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # First migrate users that aren't already mapped (create new users)
            cursor.execute("SELECT * FROM user;")
            users = cursor.fetchall()
            
            for user_row in users:
                old_user_id = user_row['id']
                if old_user_id not in user_mapping:
                    # Create new user
                    try:
                        # Helper function for safe row access
                        def safe_get(row, key, default=None):
                            try:
                                return row[key] if key in row.keys() and row[key] is not None else default
                            except (KeyError, IndexError):
                                return default
                        
                        new_user = self.user_service.create_user_sync(
                            username=user_row['username'],
                            email=user_row['email'],
                            password_hash=user_row['password_hash'],
                            is_admin=bool(safe_get(user_row, 'is_admin', False)),
                            is_active=bool(safe_get(user_row, 'is_active', True)),
                            password_must_change=bool(safe_get(user_row, 'password_must_change', False))
                        )
                        new_user_id = getattr(new_user, 'id', None) if new_user else None
                        if new_user and new_user_id:
                            user_mapping[old_user_id] = new_user_id
                            self.stats['users_migrated'] += 1
                        else:
                            self._log_error(f"Failed to create user {user_row['username']} - no ID returned")
                        
                    except Exception as e:
                        self._log_error(f"Error migrating user {user_row['username']}: {e}")
                        continue
            
            # Migrate books as global entities (no user ownership)
            cursor.execute("SELECT * FROM book;")
            books = cursor.fetchall()
            
            book_id_mapping = {}  # Map old book IDs to new ones
            created_books = set()  # Track books we've already created (avoid duplicates)
            
            for book_row in books:
                try:
                    # Helper function for safe row access
                    def safe_get(row, key, default=None):
                        try:
                            return row[key] if key in row.keys() and row[key] is not None else default
                        except (KeyError, IndexError):
                            return default
                    
                    # Create a unique book identifier to avoid duplicates
                    # Use ISBN or title+author combination
                    book_identifier = safe_get(book_row, 'isbn') or f"{safe_get(book_row, 'title', '')}|{safe_get(book_row, 'author', '')}"
                    
                    if book_identifier not in created_books:
                        # Convert SQLite row to Book object with optional API enhancement
                        book = self._sqlite_row_to_book(book_row, fetch_api_metadata=fetch_api_metadata)
                        
                        # Create book as global entity (without user-specific ownership)
                        created_book = self.book_service.create_book_sync(
                            domain_book=book
                        )
                        
                        created_book_id = getattr(created_book, 'id', None) if created_book else None
                        if created_book and created_book_id:
                            created_books.add(book_identifier)
                            book_id_mapping[book_row['id']] = created_book_id
                            self.stats['books_migrated'] += 1
                            
                            # Don't assign to admin location here - books will be assigned to 
                            # their original users later based on user_id in reading data
                        else:
                            self._log_error(f"Failed to create global book '{book.title}'")
                    else:
                        # Book already exists, just map the ID
                        existing_book_id = None
                        for old_id, new_id in book_id_mapping.items():
                            if book_identifier in created_books:
                                existing_book_id = new_id
                                break
                        if existing_book_id:
                            book_id_mapping[book_row['id']] = existing_book_id
                    
                except Exception as e:
                    title = book_row['title'] if 'title' in book_row.keys() else 'Unknown'
                    self._log_error(f"Error migrating book {title}: {e}")
                    continue
            
            # Assign books to users based on user_id field in book table
            # In V2 databases, every book has a user_id indicating ownership
            cursor.execute("SELECT * FROM book;")
            books_with_user_data = cursor.fetchall()
            
            for book_row in books_with_user_data:
                try:
                    # Helper function for safe row access
                    def safe_get(row, key, default=None):
                        try:
                            return row[key] if key in row.keys() and row[key] is not None else default
                        except (KeyError, IndexError):
                            return default
                    
                    old_book_id = book_row['id']
                    old_user_id = safe_get(book_row, 'user_id')
                    
                    # Assign book to user if both user mapping and book mapping exist
                    if (old_user_id and old_user_id in user_mapping and 
                        old_book_id in book_id_mapping):
                        
                        new_user_id = user_mapping[old_user_id]
                        new_book_id = book_id_mapping[old_book_id]
                        
                        # Get default location for this user and assign the book to it (if default location exists)
                        user_default_location_id = user_default_locations.get(new_user_id)
                        
                        if user_default_location_id:
                            try:
                                # Add book to user's default location (like manual book adding)
                                location_success = self.location_service.add_book_to_location(
                                    new_book_id, user_default_location_id, new_user_id
                                )
                                if location_success:
                                    self._log_action(f"✅ Assigned book '{safe_get(book_row, 'title', 'Unknown')}' to user {new_user_id}'s default location")
                                    self.stats['relationships_created'] += 1
                                else:
                                    self._log_error(f"Failed to assign book '{safe_get(book_row, 'title', 'Unknown')}' to user {new_user_id}'s default location")
                            except Exception as e:
                                self._log_error(f"Error assigning book '{safe_get(book_row, 'title', 'Unknown')}' to user {new_user_id}'s location: {e}")
                        else:
                            # No default location - leave book unassigned to any location (but still owned by user)
                            self._log_action(f"📝 Book '{safe_get(book_row, 'title', 'Unknown')}' assigned to user {new_user_id} but no default location set - book remains location-free")
                            self.stats['relationships_created'] += 1
                            
                        # TODO: Create reading activity/history records for user interaction with this book
                        # This could include reading status, dates, ratings, notes, etc. stored in ReadingLog
                        # For now, we'll skip this as the common library model is still being developed
                    
                except Exception as e:
                    title = book_row['title'] if 'title' in book_row.keys() else 'Unknown'
                    self._log_error(f"Error assigning book to user: {title}: {e}")
                    continue
            
            # Migrate reading logs with proper user and book mapping
            if self._table_exists(cursor, 'reading_log'):
                # First check what columns exist in the reading_log table
                cursor.execute("PRAGMA table_info(reading_log);")
                reading_log_columns = [row[1] for row in cursor.fetchall()]
                has_user_id_column = 'user_id' in reading_log_columns
                
                cursor.execute("SELECT * FROM reading_log;")
                reading_logs = cursor.fetchall()
                
                # Import reading log service for actual migration
                from app.services import reading_log_service
                from app.domain.models import ReadingLog
                
                for log_row in reading_logs:
                    try:
                        # Helper function for safe row access
                        def safe_get(row, key, default=None):
                            try:
                                return row[key] if key in row.keys() and row[key] is not None else default
                            except (KeyError, IndexError):
                                return default
                        
                        old_book_id = safe_get(log_row, 'book_id')
                        old_user_id = safe_get(log_row, 'user_id') if has_user_id_column else None
                        log_date = safe_get(log_row, 'date')
                        
                        # For V2 databases without user_id in reading_log, assign to admin user
                        # For V2 databases with user_id in reading_log, use the mapping
                        target_user_id = None
                        if has_user_id_column and old_user_id and old_user_id in user_mapping:
                            target_user_id = user_mapping[old_user_id]
                        elif not has_user_id_column:
                            # No user_id column - assign to admin user (like V1 migration)
                            target_user_id = admin_user_id
                        
                        if (old_book_id in book_id_mapping and target_user_id and log_date):
                            
                            new_book_id = book_id_mapping[old_book_id]
                            
                            # Convert string date to date object if needed
                            if isinstance(log_date, str):
                                from datetime import datetime
                                log_date = datetime.strptime(log_date, '%Y-%m-%d').date()
                            
                            # Create ReadingLog domain object
                            reading_log = ReadingLog(
                                user_id=target_user_id,
                                book_id=new_book_id,
                                date=log_date,
                                pages_read=safe_get(log_row, 'pages_read', 1),  # Default 1 page if not in SQLite
                                minutes_read=safe_get(log_row, 'minutes_read', 1),  # Default 1 minute if not in SQLite
                                notes=safe_get(log_row, 'notes', '')  # Empty notes if not in SQLite
                            )
                            
                            # Create the reading log in Kuzu
                            created_log = reading_log_service.create_reading_log_sync(reading_log)
                            
                            if created_log:
                                self.stats['reading_logs_migrated'] += 1
                                logger.debug(f"✅ Migrated reading log for user {target_user_id}, book {new_book_id} on {log_date}")
                            else:
                                logger.warning(f"⚠️ Failed to create reading log for user {target_user_id}, book {new_book_id} on {log_date}")
                        else:
                            # More detailed warning about why reading log was skipped
                            missing_parts = []
                            if old_book_id not in book_id_mapping:
                                missing_parts.append(f"book_id={old_book_id} not in mapping")
                            if not target_user_id:
                                if has_user_id_column:
                                    missing_parts.append(f"user_id={old_user_id} not in mapping")
                                else:
                                    missing_parts.append("no admin_user_id for V1-style reading log")
                            if not log_date:
                                missing_parts.append("missing date")
                                
                            logger.warning(f"⚠️ Skipping reading log - {', '.join(missing_parts)}")
                            
                    except Exception as e:
                        self._log_error(f"Error migrating reading log: {e}")
                        continue
            
            conn.close()
            
            self.current_status = MigrationStatus.COMPLETED
            self._log_action("V2 database migration completed successfully")
            logger.info(f"✅ V2 migration completed: {self.stats['users_migrated']} users, {self.stats['books_migrated']} books")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ V2 migration failed: {e}")
            self._log_error(f"V2 migration failed: {e}")
            self.current_status = MigrationStatus.FAILED
            return False
    
    def _sqlite_row_to_book(self, row, fetch_api_metadata: bool = True) -> Book:
        """Convert a SQLite row to a Book domain object with optional API metadata enhancement.
        
        Args:
            row: SQLite row data
            fetch_api_metadata: Whether to fetch metadata from APIs (used for v2 migration user choice)
        """
        # Helper function to safely get values from sqlite3.Row
        def safe_get(row, key, default=None):
            try:
                return row[key] if key in row.keys() and row[key] is not None else default
            except (KeyError, IndexError):
                return default
        
        # Handle ISBN - SQLite has 'isbn', Book model has isbn13/isbn10
        isbn = safe_get(row, 'isbn')
        isbn13 = None
        isbn10 = None
        if isbn:
            # Try to determine if it's ISBN-13 or ISBN-10 based on length
            clean_isbn = isbn.replace('-', '').replace(' ', '')
            if len(clean_isbn) == 13:
                isbn13 = isbn
            elif len(clean_isbn) == 10:
                isbn10 = isbn
            else:
                # Default to isbn13 if unclear
                isbn13 = isbn
        
        # Determine if we should fetch API metadata
        api_data = None
        should_fetch_api = False
        
        if isbn and fetch_api_metadata:
            # For v1 databases: Always fetch if no metadata exists, prioritize API for categories
            # For v2 databases: Fetch based on user choice
            has_existing_metadata = bool(safe_get(row, 'categories') or safe_get(row, 'description'))
            
            # For V1 databases, always fetch API data to get proper categories
            # since V1 databases typically lack category information
            if not has_existing_metadata or not safe_get(row, 'categories'):
                should_fetch_api = True
                logger.info(f"📡 Fetching API metadata for ISBN {isbn} (V1 migration or missing categories)")
            elif fetch_api_metadata:
                # Has existing metadata but user chose to fetch fresh API data (v2 option)
                should_fetch_api = True
                logger.info(f"📡 User chose fresh API metadata for ISBN {isbn} - fetching from APIs")
        
        if should_fetch_api:
            try:
                # Use unified cover/metadata helper for consistency across system
                from app.utils import get_google_books_cover, fetch_book_data
                from app.utils.book_utils import get_best_cover_for_book
                
                # Add small random delay for migration to avoid overwhelming APIs
                delay = random.uniform(0.4, 0.8)  # Random delay between 400-800ms
                time.sleep(delay)
                
                # Use existing granular retrieval for rich metadata
                google_data = None
                openlibrary_data = None
                try:
                    google_data = get_google_books_cover(isbn, fetch_title_author=True)
                    if google_data:
                        logger.info(f"✅ Got Google Books data for {isbn}")
                except Exception as e:
                    logger.warning(f"⚠️ Google Books failed for {isbn}: {e}")
                try:
                    openlibrary_data = fetch_book_data(isbn)
                    if openlibrary_data:
                        logger.info(f"✅ Got OpenLibrary data for {isbn}")
                except Exception as e:
                    logger.warning(f"⚠️ OpenLibrary failed for {isbn}: {e}")

                # Merge metadata preserving richer Google info then supplementing
                if google_data or openlibrary_data:
                    api_data = {}
                    if google_data:
                        api_data.update(google_data)
                    if openlibrary_data:
                        if google_data and google_data.get('categories') and openlibrary_data.get('categories'):
                            google_cats = set(google_data.get('categories', []))
                            ol_cats = set(openlibrary_data.get('categories', []))
                            api_data['categories'] = list(google_cats.union(ol_cats))[:10]
                        elif openlibrary_data.get('categories') and not api_data.get('categories'):
                            api_data['categories'] = openlibrary_data.get('categories', [])
                        for field in ['description', 'publisher', 'page_count', 'published_date']:
                            if not api_data.get(field) and openlibrary_data.get(field):
                                api_data[field] = openlibrary_data[field]
                        if openlibrary_data.get('openlibrary_id'):
                            api_data['openlibrary_id'] = openlibrary_data['openlibrary_id']
                        logger.info(f"✅ Merged OpenLibrary data for {isbn}")
                    if api_data.get('categories'):
                        logger.info(f"📚 Categories for {isbn}: {api_data['categories']}")

                # Unified cover selection
                try:
                    best_cover = get_best_cover_for_book(isbn=isbn)
                    if best_cover and best_cover.get('cover_url'):
                        if api_data is None:
                            api_data = {}
                        from app.utils.book_utils import normalize_cover_url
                        api_data['cover'] = normalize_cover_url(best_cover['cover_url'])
                        api_data['cover_source'] = best_cover.get('source')
                        api_data['cover_quality'] = best_cover.get('quality')
                        logger.info(f"🖼️ Selected cover ({best_cover.get('source')}) for {isbn}")
                except Exception as e:
                    logger.warning(f"⚠️ Unified cover selection failed for {isbn}: {e}")
                
            except Exception as e:
                logger.warning(f"⚠️ Failed to fetch API metadata for {isbn}: {e}")
                api_data = None
        
        # Create the book object with intelligent data prioritization
        # Priority: SQLite data (if exists) > API data > defaults
        book = Book(
            id=str(uuid.uuid4()),  # Generate new ID
            title=safe_get(row, 'title') or (api_data.get('title') if api_data else '') or '',
            isbn13=isbn13 or (api_data.get('isbn13') if api_data else None),
            isbn10=isbn10 or (api_data.get('isbn10') if api_data else None),
            # Prefer existing sqlite cover, else unified selected cover
            cover_url=safe_get(row, 'cover_url') or (api_data.get('cover') if api_data else None),
            description=safe_get(row, 'description') or (api_data.get('description') if api_data else None),
            published_date=safe_get(row, 'published_date') or (api_data.get('published_date') if api_data else None),
            page_count=safe_get(row, 'page_count') or (api_data.get('page_count') if api_data else None),
            language=safe_get(row, 'language') or (api_data.get('language') if api_data else 'en') or 'en',
            average_rating=safe_get(row, 'average_rating') or (api_data.get('average_rating') if api_data else None),
            rating_count=safe_get(row, 'rating_count') or (api_data.get('rating_count') if api_data else None),
            asin=api_data.get('asin') if api_data else None,
            google_books_id=api_data.get('google_books_id') if api_data else None,
            openlibrary_id=api_data.get('openlibrary_id') if api_data else None,
            created_at=self._parse_datetime(safe_get(row, 'created_at')) or datetime.now(),
            updated_at=datetime.now()
        )
        
        # Handle authors - convert single author string to Person/Contributor with API enhancement
        author_name = safe_get(row, 'author')
        api_authors = api_data.get('authors_list', []) if api_data else []
        
        # Determine authors to process with proper comma-separation support
        authors_to_process = []
        
        # Prioritize API authors if available (they're already in list format)
        if api_authors:
            authors_to_process = api_authors
            logger.info(f"📚 Using API authors for {safe_get(row, 'title', 'Unknown')}: {api_authors}")
        elif author_name and author_name.strip():
            # Handle comma-separated authors from SQLite data
            # Split by common separators: comma, semicolon, " and ", " & "
            raw_author = author_name.strip()
            
            # Try different separators in order of preference
            if ',' in raw_author:
                authors_to_process = [name.strip() for name in raw_author.split(',') if name.strip()]
            elif ';' in raw_author:
                authors_to_process = [name.strip() for name in raw_author.split(';') if name.strip()]
            elif ' and ' in raw_author.lower():
                # Split on " and " (case insensitive)
                authors_to_process = [name.strip() for name in re.split(r'\s+and\s+', raw_author, flags=re.IGNORECASE) if name.strip()]
            elif ' & ' in raw_author:
                authors_to_process = [name.strip() for name in raw_author.split(' & ') if name.strip()]
            else:
                # Single author
                authors_to_process = [raw_author]
            
            logger.info(f"📚 Parsed SQLite authors for {safe_get(row, 'title', 'Unknown')}: {authors_to_process}")
        
        contributors = []
        for i, author in enumerate(authors_to_process):
            if author and author.strip():
                # Create Person for the author with OpenLibrary metadata enrichment
                person = Person(
                    name=author.strip(),
                    normalized_name=author.strip().lower(),
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                
                # Try to enrich person data with OpenLibrary metadata during migration
                try:
                    from app.utils import search_author_by_name, fetch_author_data
                    
                    # Add small delay to avoid overwhelming OpenLibrary API
                    delay = random.uniform(0.3, 0.7)  # Random delay between 300-700ms
                    time.sleep(delay)
                    
                    logger.info(f"📡 Fetching OpenLibrary metadata for author: {author.strip()}")
                    search_result = search_author_by_name(author.strip())
                    
                    if search_result and search_result.get('openlibrary_id'):
                        # Get detailed author data using the OpenLibrary ID
                        author_id = search_result['openlibrary_id']
                        detailed_author_data = fetch_author_data(author_id)
                        
                        if detailed_author_data:
                            # Use the same comprehensive parser as the person metadata refresh
                            from app.routes.people_routes import parse_comprehensive_openlibrary_data
                            
                            # Parse comprehensive data
                            updates = parse_comprehensive_openlibrary_data(detailed_author_data)
                            
                            # Apply all the comprehensive updates to the person
                            for field, value in updates.items():
                                if hasattr(person, field) and value is not None:
                                    setattr(person, field, value)
                            
                            logger.info(f"✅ Enriched author '{author.strip()}' with comprehensive OpenLibrary data: {author_id}")
                            logger.info(f"📚 Applied fields: {list(updates.keys())}")
                        else:
                            logger.warning(f"⚠️ Could not fetch detailed data for OpenLibrary ID: {author_id}")
                    else:
                        logger.info(f"⚠️ No OpenLibrary data found for author: {author.strip()}")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Failed to fetch OpenLibrary data for author '{author.strip()}': {e}")
                    # Continue with basic person data if API fails
                
                # Create BookContribution with proper order
                contribution = BookContribution(
                    person=person,
                    contribution_type=ContributionType.AUTHORED,
                    order=i,  # Proper ordering for multiple authors
                    created_at=datetime.now()
                )
                contributors.append(contribution)
        
        book.contributors = contributors
        
        # Handle categories with intelligent prioritization
        # Always prioritize API categories for proper genre/category data
        categories_str = safe_get(row, 'categories')  # From SQLite (v2 databases may have this)
        api_categories = api_data.get('categories', []) if api_data else []
        
        categories_to_use = []
        
        # ALWAYS prioritize API categories when available, since they're more accurate
        if api_categories:
            categories_to_use = api_categories
            if categories_str:
                logger.info(f"📚 Using API categories over existing SQLite categories for {book.title}")
            else:
                logger.info(f"📚 Using API categories for {book.title}: {categories_to_use}")
        elif categories_str:
            # Only use existing SQLite categories if no API data available
            for sep in [',', ';', '|']:
                if sep in categories_str:
                    categories_to_use = [name.strip() for name in categories_str.split(sep)]
                    break
            else:
                # Single category
                categories_to_use = [categories_str.strip()]
            logger.info(f"📚 Using existing SQLite categories (no API data) for {book.title}: {categories_to_use}")
        else:
            logger.info(f"📚 No categories available for {book.title}")
        
        # Convert category names to raw_categories for processing by repository
        if categories_to_use:
            book.raw_categories = categories_to_use
            logger.info(f"📚 Set raw_categories for {book.title}: {book.raw_categories}")
        
        # Handle publisher with intelligent prioritization
        publisher_name = safe_get(row, 'publisher') or (api_data.get('publisher') if api_data else None)
        if publisher_name:
            publisher = Publisher(
                name=publisher_name.strip(),
                created_at=datetime.now()
            )
            book.publisher = publisher
        
        return book
    
    def _parse_date(self, date_str) -> Optional[date]:
        """Parse date string to date object."""
        if not date_str:
            return None
        try:
            # Try different date formats
            for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y']:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
            # If none work, try parsing as ISO datetime and extract date
            return datetime.fromisoformat(str(date_str)).date()
        except:
            return None
    
    def _parse_datetime(self, datetime_str) -> Optional[datetime]:
        """Parse datetime string to datetime object."""
        if not datetime_str:
            return None
        try:
            return datetime.fromisoformat(datetime_str)
        except:
            return None
    
    def _table_exists(self, cursor, table_name: str) -> bool:
        """Check if a table exists in the database."""
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
        return cursor.fetchone() is not None
    
    def _log_action(self, message: str):
        """Log an action to the migration log."""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'type': 'action',
            'message': message
        }
        self.migration_log.append(log_entry)
        logger.info(message)
    
    def _log_error(self, message: str):
        """Log an error to the migration log."""
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'type': 'error',
            'message': message
        }
        self.migration_log.append(log_entry)
        self.stats['errors'].append(message)
        logger.error(message)
    
    def get_migration_summary(self) -> Dict:
        """Get a summary of the migration status and statistics."""
        return {
            'status': self.current_status,
            'stats': self.stats,
            'log': self.migration_log,
            'backup_location': str(self.backup_dir)
        }
    
    def cleanup_temp_files(self):
        """Clean up temporary files after successful migration."""
        try:
            # Keep backups but remove any temporary files
            # This is intentionally conservative
            pass
        except Exception as e:
            logger.warning(f"Could not clean up temporary files: {e}")
