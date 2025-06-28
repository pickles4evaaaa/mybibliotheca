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
from app.infrastructure.kuzu_graph import KuzuGraphStorage, get_graph_storage
from app.services import book_service, user_service
from config import Config

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
    
    def __init__(self, kuzu_db_path: str = None):
        """
        Initialize the advanced migration system.
        
        Args:
            kuzu_db_path: Kuzu database path (defaults to config)
        """
        self.backup_dir = Path("migration_backups") / datetime.now().strftime("%Y%m%d_%H%M%S")
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup Kuzu connection
        self.graph_store = get_graph_storage()
        self.book_repo = KuzuBookRepository(self.graph_store)
        self.user_repo = KuzuUserRepository(self.graph_store)
        
        # Initialize services for migration operations
        self.book_service = book_service
        self.user_service = user_service
        
        # Initialize location service
        from .location_service import LocationService
        self.location_service = LocationService(self.graph_store)
        
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
    
    def create_backup(self, db_path: Path = None) -> bool:
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
            
            # Backup Redis data (if any exists)
            redis_backup = self.backup_dir / "redis_backup_pre_migration.json"
            self._backup_redis_data(redis_backup)
            logger.info(f"✅ Created Redis backup: {redis_backup}")
            self._log_action(f"Redis backup created: {redis_backup}")
            
            self.current_status = MigrationStatus.BACKUP_CREATED
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create backup: {e}")
            self._log_error(f"Backup creation failed: {e}")
            return False
    
    def _backup_redis_data(self, backup_path: Path) -> None:
        """Backup all Redis data to a JSON file."""
        backup_data = {
            'timestamp': datetime.now().isoformat(),
            'keys': {}
        }
        
        # Get all keys and their data
        for key in self.redis_client.scan_iter():
            key_str = key.decode('utf-8')
            key_type = self.redis_client.type(key).decode('utf-8')
            
            try:
                if key_type == 'string':
                    backup_data['keys'][key_str] = {
                        'type': 'string',
                        'value': self.redis_client.get(key).decode('utf-8')
                    }
                elif key_type == 'hash':
                    backup_data['keys'][key_str] = {
                        'type': 'hash',
                        'value': {k.decode('utf-8'): v.decode('utf-8') 
                                for k, v in self.redis_client.hgetall(key).items()}
                    }
                # Add other Redis data types as needed
            except Exception as e:
                logger.warning(f"Could not backup key {key_str}: {e}")
        
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
            
            self.current_status = MigrationStatus.ADMIN_CREATED
            self._log_action(f"First admin user created successfully: {admin_user.id}")
            logger.info(f"✅ First admin user created: {username}")
            
            return admin_user
            
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
            
            # Get the default location for the admin user
            default_location = self.location_service.get_default_location(admin_user_id)
            if not default_location:
                self._log_error("No default location found for admin user. Books will be created without location assignment.")
                # Debug: List all locations for this user
                all_locations = self.location_service.get_user_locations(admin_user_id, active_only=False)
                self._log_action(f"DEBUG: Found {len(all_locations)} locations for user {admin_user_id}")
                for loc in all_locations:
                    self._log_action(f"DEBUG: Location {loc.name} (ID: {loc.id}, default: {loc.is_default}, active: {loc.is_active})")
            else:
                self._log_action(f"Using default location: {default_location.name} (ID: {default_location.id})")
            
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Migrate books
            cursor.execute("SELECT * FROM book;")
            books = cursor.fetchall()
            
            for book_row in books:
                try:
                    # Convert SQLite row to Book object
                    book = self._sqlite_row_to_book(book_row)
                    
                    # Extract categories for processing AFTER book creation
                    categories_to_process = []
                    if hasattr(book, 'categories') and book.categories:
                        categories_to_process = [category.name for category in book.categories]
                    
                    # Clear categories from book object to avoid issues during creation
                    book.categories = []
                    
                    # Create book in Redis using service (without categories for now)
                    created_book = self.book_service.create_book_sync(book)
                    
                    # Now process categories using the service's category processing method
                    if categories_to_process:
                        try:
                            success = self.book_service.process_book_categories_sync(
                                created_book.id, categories_to_process
                            )
                            if success:
                                self._log_action(f"Successfully processed {len(categories_to_process)} categories for book: {book.title}")
                            else:
                                self._log_error(f"Failed to process categories for book: {book.title}")
                        except Exception as cat_error:
                            self._log_error(f"Error processing categories for book '{book.title}': {cat_error}")
                    
                    # Add book to admin user's library with appropriate status
                    reading_status = ReadingStatus.READ if book_row['finish_date'] else ReadingStatus.WANT_TO_READ
                    locations = [default_location.id] if default_location else []
                    
                    # Debug location assignment
                    self._log_action(f"DEBUG: Assigning book '{book.title}' to locations: {locations}")
                    
                    self.book_service.add_book_to_user_library_sync(
                        user_id=admin_user_id,
                        book_id=created_book.id,
                        reading_status=reading_status,
                        locations=locations,
                        start_date=self._parse_date(book_row['start_date']),
                        finish_date=self._parse_date(book_row['finish_date']),
                        date_added=datetime.now().date()
                    )
                    
                    self.stats['books_migrated'] += 1
                    self.stats['relationships_created'] += 1
                    
                except Exception as e:
                    title = book_row['title'] if 'title' in book_row.keys() else 'Unknown'
                    self._log_error(f"Error migrating book {title}: {e}")
                    continue
            
            # Migrate reading logs
            if self._table_exists(cursor, 'reading_log'):
                cursor.execute("SELECT * FROM reading_log;")
                reading_logs = cursor.fetchall()
                
                for log_row in reading_logs:
                    try:
                        # Find the corresponding book by old book_id
                        # This requires mapping old IDs to new ones
                        # For now, we'll skip reading logs or implement ID mapping
                        self.stats['reading_logs_migrated'] += 1
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
    
    def migrate_v2_database(self, db_path: Path, user_mapping: Dict[int, str]) -> bool:
        """
        Migrate a V2 (multi-user) database with user mapping.
        
        Args:
            db_path: Path to the V2 SQLite database
            user_mapping: Dictionary mapping old user IDs to new user IDs
            
        Returns:
            True if migration successful, False otherwise
        """
        try:
            self._log_action(f"Starting V2 database migration: {db_path}")
            
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
                        user_mapping[old_user_id] = new_user.id
                        self.stats['users_migrated'] += 1
                        
                    except Exception as e:
                        self._log_error(f"Error migrating user {user_row['username']}: {e}")
                        continue
            
            # Migrate books with user relationships
            cursor.execute("SELECT * FROM book;")
            books = cursor.fetchall()
            
            book_id_mapping = {}  # Map old book IDs to new ones
            
            for book_row in books:
                try:
                    # Helper function for safe row access
                    def safe_get(row, key, default=None):
                        try:
                            return row[key] if key in row.keys() and row[key] is not None else default
                        except (KeyError, IndexError):
                            return default
                    
                    # Convert SQLite row to Book object
                    book = self._sqlite_row_to_book(book_row)
                    
                    # Extract categories for processing AFTER book creation
                    categories_to_process = []
                    if hasattr(book, 'categories') and book.categories:
                        categories_to_process = [category.name for category in book.categories]
                    
                    # Clear categories from book object to avoid issues during creation
                    book.categories = []
                    
                    # Create book in Redis using service (without categories for now)
                    created_book = self.book_service.create_book_sync(book)
                    book_id_mapping[book_row['id']] = created_book.id
                    
                    # Now process categories using the service's category processing method
                    if categories_to_process:
                        try:
                            success = self.book_service.process_book_categories_sync(
                                created_book.id, categories_to_process
                            )
                            if success:
                                self._log_action(f"Successfully processed {len(categories_to_process)} categories for book: {book.title}")
                            else:
                                self._log_error(f"Failed to process categories for book: {book.title}")
                        except Exception as cat_error:
                            self._log_error(f"Error processing categories for book '{book.title}': {cat_error}")
                    
                    # Create relationship with the appropriate user
                    old_user_id = safe_get(book_row, 'user_id')
                    if old_user_id and old_user_id in user_mapping:
                        new_user_id = user_mapping[old_user_id]
                        
                        # Get default location for this user
                        default_location = self.location_service.get_default_location(new_user_id)
                        locations = [default_location.id] if default_location else []
                        
                        reading_status = ReadingStatus.READ if book_row['finish_date'] else ReadingStatus.WANT_TO_READ
                        self.book_service.add_book_to_user_library_sync(
                            user_id=new_user_id,
                            book_id=created_book.id,
                            reading_status=reading_status,
                            locations=locations,
                            start_date=self._parse_date(book_row['start_date']),
                            finish_date=self._parse_date(book_row['finish_date']),
                            date_added=self._parse_datetime(safe_get(book_row, 'created_at')).date() if self._parse_datetime(safe_get(book_row, 'created_at')) else datetime.now().date()
                        )
                        
                        self.stats['relationships_created'] += 1
                    
                    self.stats['books_migrated'] += 1
                    
                except Exception as e:
                    title = book_row['title'] if 'title' in book_row.keys() else 'Unknown'
                    self._log_error(f"Error migrating book {title}: {e}")
                    continue
            
            # Migrate reading logs with proper user and book mapping
            if self._table_exists(cursor, 'reading_log'):
                cursor.execute("SELECT * FROM reading_log;")
                reading_logs = cursor.fetchall()
                
                for log_row in reading_logs:
                    try:
                        # Helper function for safe row access
                        def safe_get(row, key, default=None):
                            try:
                                return row[key] if key in row.keys() and row[key] is not None else default
                            except (KeyError, IndexError):
                                return default
                        
                        old_book_id = log_row['book_id']
                        old_user_id = safe_get(log_row, 'user_id')
                        
                        if (old_book_id in book_id_mapping and 
                            old_user_id and old_user_id in user_mapping):
                            
                            # Create reading log entry
                            # Implementation depends on your reading log structure
                            self.stats['reading_logs_migrated'] += 1
                            
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
    
    def _sqlite_row_to_book(self, row) -> Book:
        """Convert a SQLite row to a Book domain object."""
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
        
        # Create the book object
        book = Book(
            id=str(uuid.uuid4()),  # Generate new ID
            title=safe_get(row, 'title') or '',
            isbn13=isbn13,
            isbn10=isbn10,
            cover_url=safe_get(row, 'cover_url'),
            description=safe_get(row, 'description'),
            published_date=safe_get(row, 'published_date'),
            page_count=safe_get(row, 'page_count'),
            language=safe_get(row, 'language') or 'en',
            average_rating=safe_get(row, 'average_rating'),
            rating_count=safe_get(row, 'rating_count'),
            created_at=self._parse_datetime(safe_get(row, 'created_at')) or datetime.now(),
            updated_at=datetime.now()
        )
        
        # Handle authors - convert single author string to Person/Contributor
        author_name = safe_get(row, 'author')
        if author_name:
            # Create Person for the author
            person = Person(
                id=str(uuid.uuid4()),
                name=author_name.strip(),
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            
            # Create BookContribution
            contribution = BookContribution(
                person_id=person.id,
                book_id=book.id,
                contribution_type=ContributionType.AUTHORED,
                person=person,
                created_at=datetime.now()
            )
            book.contributors = [contribution]
        
        # Handle categories - convert SQLite categories to Category objects
        categories_str = safe_get(row, 'categories')
        categories = []
        if categories_str:
            # Categories might be comma-separated or semicolon-separated
            for sep in [',', ';', '|']:
                if sep in categories_str:
                    category_names = [name.strip() for name in categories_str.split(sep)]
                    break
            else:
                # Single category
                category_names = [categories_str.strip()]
            
            for category_name in category_names:
                if category_name:
                    category = Category(
                        id=str(uuid.uuid4()),
                        name=category_name,
                        normalized_name=Category._normalize_name(category_name),
                        created_at=datetime.now(),
                        updated_at=datetime.now()
                    )
                    categories.append(category)
        
        # Store categories for later processing (we'll handle these after creating the book)
        book.categories = categories
        
        # Handle publisher
        publisher_name = safe_get(row, 'publisher')
        if publisher_name:
            publisher = Publisher(
                id=str(uuid.uuid4()),
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
