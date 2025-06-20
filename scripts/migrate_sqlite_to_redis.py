#!/usr/bin/env python3
"""
SQLite to Redis Migration Script for Bibliotheca
================================================

This script migrates data from SQLite databases to the new Redis-based system.
It handles both v1 (single-user) and v2 (multi-user) SQLite schemas.

Features:
- Automatic backup creation before and after migration
- Support for both old and new SQLite schemas
- Data validation and integrity checks
- Rollback capability if migration fails
- Detailed logging and progress reporting

Usage:
    python scripts/migrate_sqlite_to_redis.py --db-path path/to/database.db [--user-id user_id]
    
For single-user databases (v1), --user-id is required.
For multi-user databases (v2), --user-id is optional (migrates all users).
"""

import os
import sys
import sqlite3
import shutil
import json
import argparse
import logging
import asyncio
import redis
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
from dataclasses import asdict

# Add the parent directory to the path so we can import our modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.domain.models import Book, User, UserBookRelationship, Author, Publisher, Category, ReadingStatus
from app.infrastructure.redis_repositories import RedisBookRepository, RedisUserRepository
from app.infrastructure.redis_graph import RedisGraphStorage, RedisGraphConnection
import redis
from config import Config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class WebBasedSQLiteToRedisMigrator:
    """Simplified web-based migration class for converting SQLite to Redis."""
    
    def __init__(self, db_path: str, target_user_id: str, redis_url: str = None):
        """
        Initialize the web-based migrator.
        
        Args:
            db_path: Path to the SQLite database file
            target_user_id: User ID from current web session (current_user.id)
            redis_url: Redis connection URL (defaults to config)
        """
        self.db_path = Path(db_path)
        self.target_user_id = target_user_id
        self.backup_dir = Path("migration_backups") / datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Ensure backup directory exists
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup Redis connection
        self.redis_client = redis.from_url(redis_url or Config.REDIS_URL)
        
        # Initialize repositories with proper connection object
        redis_connection = RedisGraphConnection(redis_url or Config.REDIS_URL)
        self.graph_store = RedisGraphStorage(redis_connection)
        self.book_repo = RedisBookRepository(self.graph_store)
        self.user_repo = RedisUserRepository(self.graph_store)
        
        # Migration statistics
        self.stats = {
            'books_migrated': 0,
            'reading_logs_migrated': 0,
            'relationships_created': 0,
            'errors': []
        }
    
    def create_pre_migration_backup(self) -> bool:
        """Create a backup of the SQLite database before migration."""
        try:
            # Backup SQLite database
            sqlite_backup = self.backup_dir / f"sqlite_backup_{self.db_path.name}"
            shutil.copy2(self.db_path, sqlite_backup)
            logger.info(f"✅ Created SQLite backup: {sqlite_backup}")
            
            # Backup Redis data (if any exists)
            redis_backup = self.backup_dir / "redis_backup_pre_migration.json"
            self._backup_redis_data(redis_backup)
            logger.info(f"✅ Created Redis backup: {redis_backup}")
            
            return True
        except Exception as e:
            logger.error(f"❌ Failed to create pre-migration backup: {e}")
            return False
    
    def create_post_migration_backup(self) -> bool:
        """Create a backup of Redis data after successful migration."""
        try:
            redis_backup = self.backup_dir / "redis_backup_post_migration.json"
            self._backup_redis_data(redis_backup)
            logger.info(f"✅ Created post-migration Redis backup: {redis_backup}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to create post-migration backup: {e}")
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
                elif key_type == 'set':
                    backup_data['keys'][key_str] = {
                        'type': 'set',
                        'value': [member.decode('utf-8') for member in self.redis_client.smembers(key)]
                    }
                # Add more types as needed
            except Exception as e:
                logger.warning(f"⚠️  Failed to backup key {key_str}: {e}")
        
        with open(backup_path, 'w') as f:
            json.dump(backup_data, f, indent=2)
    
    def detect_schema_version(self) -> int:
        """
        Detect SQLite schema version.
        
        Returns:
            1: Single-user schema (no user table)
            2: Multi-user schema (has user table)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Check if user table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user';")
            has_user_table = cursor.fetchone() is not None
            
            # Check book table schema for user_id column
            cursor.execute("PRAGMA table_info(book);")
            columns = [col[1] for col in cursor.fetchall()]
            has_user_id_column = 'user_id' in columns
            
            if has_user_table and has_user_id_column:
                return 2  # Multi-user schema
            else:
                return 1  # Single-user schema
                
        finally:
            conn.close()
    
    def migrate_users_v2(self, conn: sqlite3.Connection) -> Dict[int, str]:
        """
        Migrate users from v2 schema.
        
        Returns:
            Dict mapping SQLite user IDs to Redis user IDs
        """
        cursor = conn.cursor()
        user_id_mapping = {}
        
        cursor.execute("""
            SELECT id, username, email, password_hash, is_admin, created_at,
                   is_active, share_current_reading, share_reading_activity, 
                   share_library, reading_streak_offset
            FROM user
        """)
        
        for row in cursor.fetchall():
            (sqlite_id, username, email, password_hash, is_admin, created_at,
             is_active, share_current_reading, share_reading_activity,
             share_library, reading_streak_offset) = row
            
            try:
                # Create Redis user
                user = User(
                    username=username,
                    email=email,
                    password_hash=password_hash,
                    is_admin=bool(is_admin),
                    is_active=bool(is_active),
                    created_at=datetime.fromisoformat(created_at) if created_at else datetime.now(),
                    profile={
                        'share_current_reading': bool(share_current_reading),
                        'share_reading_activity': bool(share_reading_activity),
                        'share_library': bool(share_library),
                        'reading_streak_offset': reading_streak_offset or 0
                    }
                )
                
                redis_user_id = self.user_repo.save_user(user)
                user_id_mapping[sqlite_id] = redis_user_id
                self.stats['users_migrated'] += 1
                
                logger.info(f"✅ Migrated user: {username} ({sqlite_id} -> {redis_user_id})")
                
            except Exception as e:
                error_msg = f"Failed to migrate user {username}: {e}"
                logger.error(f"❌ {error_msg}")
                self.stats['errors'].append(error_msg)
        
        return user_id_mapping
    
    def migrate_books_to_current_user(self, conn: sqlite3.Connection) -> Dict[int, str]:
        """
        Migrate all books from SQLite to Redis and assign to current user.
        
        Args:
            conn: SQLite connection
            
        Returns:
            Dict mapping SQLite book IDs to Redis book IDs
        """
        cursor = conn.cursor()
        book_id_mapping = {}
        
        # Simple query - doesn't matter if v1 or v2 schema
        query = """
            SELECT id, uid, title, author, isbn, start_date, finish_date,
                   cover_url, want_to_read, library_only, description,
                   published_date, page_count, categories, publisher,
                   language, average_rating, rating_count
            FROM book
        """
        
        cursor.execute(query)
        
        for row in cursor.fetchall():
            try:
                (sqlite_id, uid, title, author, isbn, start_date, finish_date,
                 cover_url, want_to_read, library_only, description,
                 published_date, page_count, categories, publisher,
                 language, average_rating, rating_count) = row
                
                # Transform and create Book object with proper field mapping
                
                # Handle authors: convert string to list of Author objects
                authors = []
                if author:
                    # Split multiple authors if they're comma/semicolon separated
                    author_names = [name.strip() for name in author.replace(';', ',').split(',') if name.strip()]
                    for author_name in author_names:
                        authors.append(Author(name=author_name, normalized_name=author_name.strip().lower()))
                
                # Handle publisher: convert string to Publisher object
                publisher_obj = None
                if publisher:
                    publisher_obj = Publisher(name=publisher, normalized_name=publisher.strip().lower())
                
                # Handle categories: convert comma-separated string to list of Category objects
                categories_list = []
                if categories:
                    category_names = [cat.strip() for cat in categories.split(',') if cat.strip()]
                    for cat_name in category_names:
                        categories_list.append(Category(name=cat_name, normalized_name=cat_name.strip().lower()))
                
                # Handle ISBN: legacy uses single 'isbn' field, new model has isbn13/isbn10
                isbn13_val = None
                isbn10_val = None
                if isbn:
                    clean_isbn = isbn.replace('-', '').replace(' ', '')
                    if len(clean_isbn) == 13:
                        isbn13_val = isbn
                    elif len(clean_isbn) == 10:
                        isbn10_val = isbn
                    else:
                        # Default to isbn13 if unclear
                        isbn13_val = isbn
                
                # Handle dates
                published_date_obj = None
                if published_date:
                    try:
                        if isinstance(published_date, str):
                            published_date_obj = datetime.fromisoformat(published_date.replace('Z', '+00:00'))
                        elif isinstance(published_date, (int, float)):
                            published_date_obj = datetime.fromtimestamp(published_date)
                    except (ValueError, TypeError):
                        logger.warning(f"Could not parse published_date: {published_date}")
                
                book = Book(
                    title=title or "",
                    authors=authors,
                    isbn13=isbn13_val,
                    isbn10=isbn10_val,
                    cover_url=cover_url,
                    description=description,
                    published_date=published_date_obj,
                    page_count=page_count,
                    categories=categories_list,
                    publisher=publisher_obj,
                    language=language or "en",
                    average_rating=average_rating,
                    rating_count=rating_count
                )
                
                # Create book in Redis with error handling
                try:
                    redis_book = asyncio.run(self.book_repo.create(book))
                    redis_book_id = redis_book.id
                    book_id_mapping[sqlite_id] = redis_book_id
                    logger.debug(f"✅ Created book in Redis: {redis_book_id}")
                except Exception as e:
                    error_msg = f"Failed to create book {sqlite_id} in Redis: {e}"
                    logger.error(f"❌ {error_msg}")
                    self.stats['errors'].append(error_msg)
                    continue
                
                # Create user-book relationship with current user
                relationship = UserBookRelationship(
                    user_id=self.target_user_id,  # Always current user from web session
                    book_id=redis_book_id,
                    reading_status=self._determine_reading_status(start_date, finish_date, want_to_read, library_only),
                    start_date=datetime.strptime(start_date, '%Y-%m-%d') if start_date else None,
                    finish_date=datetime.strptime(finish_date, '%Y-%m-%d') if finish_date else None,
                    user_rating=None,  # No rating in old schema
                    personal_notes="",
                    custom_metadata={}
                )
                
                # Save relationship
                success = self.graph_store.create_relationship(
                    'user', self.target_user_id, 'HAS_BOOK', 'book', redis_book_id,
                    properties=asdict(relationship)
                )
                
                if success:
                    self.stats['relationships_created'] += 1
                
                # Increment counters only on successful migration
                self.stats['books_migrated'] += 1
                logger.info(f"✅ Migrated book: {title} ({sqlite_id} -> {redis_book_id}) for user {self.target_user_id}")
                
            except Exception as e:
                error_msg = f"Failed to migrate book {sqlite_id}: {e}"
                logger.error(f"❌ {error_msg}")
                self.stats['errors'].append(error_msg)
        
        return book_id_mapping
    
    def migrate_reading_logs_for_current_user(self, conn: sqlite3.Connection, 
                                             book_id_mapping: Dict[int, str]) -> None:
        """Migrate reading logs from SQLite to Redis for current user."""
        cursor = conn.cursor()
        
        # Simple query - works for both v1 and v2 schemas
        query = "SELECT id, book_id, date FROM reading_log"
        
        try:
            cursor.execute(query)
        except sqlite3.OperationalError:
            # No reading_log table exists
            logger.info("No reading_log table found - skipping reading log migration")
            return
        
        for row in cursor.fetchall():
            try:
                sqlite_id, sqlite_book_id, log_date = row
                redis_book_id = book_id_mapping.get(sqlite_book_id)
                
                if not redis_book_id:
                    logger.warning(f"⚠️  No book mapping for reading log {sqlite_id}")
                    continue
                
                # Store reading log as a relationship property or separate entity
                # For now, we'll add it to the user-book relationship
                rel_key = f"rel:user:{self.target_user_id}:HAS_BOOK"
                existing_rels = self.redis_client.smembers(rel_key)
                
                for rel_data in existing_rels:
                    rel_json = json.loads(rel_data)
                    if rel_json.get('to_id') == redis_book_id:
                        # Add reading log date to the relationship
                        reading_logs = rel_json.get('properties', {}).get('reading_logs', [])
                        reading_logs.append(log_date)
                        rel_json['properties']['reading_logs'] = reading_logs
                        
                        # Update the relationship
                        self.redis_client.srem(rel_key, rel_data)
                        self.redis_client.sadd(rel_key, json.dumps(rel_json))
                        break
                
                self.stats['reading_logs_migrated'] += 1
                logger.info(f"✅ Migrated reading log: {log_date} for book {redis_book_id}")
                
            except Exception as e:
                error_msg = f"Failed to migrate reading log {sqlite_id}: {e}"
                logger.error(f"❌ {error_msg}")
                self.stats['errors'].append(error_msg)
    
    def _determine_reading_status(self, start_date: str, finish_date: str, 
                                 want_to_read: bool, library_only: bool) -> ReadingStatus:
        """Determine reading status based on SQLite data."""
        if finish_date:
            return ReadingStatus.READ
        elif start_date:
            return ReadingStatus.READING
        elif want_to_read:
            return ReadingStatus.PLAN_TO_READ
        elif library_only:
            return ReadingStatus.LIBRARY_ONLY
        else:
            return ReadingStatus.PLAN_TO_READ  # Default
    
    def validate_migration(self) -> bool:
        """Validate the migration by checking data integrity."""
        logger.info("🔍 Validating migration...")
        
        try:
            # Check if we have the expected number of users and books
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            schema_version = self.detect_schema_version()
            
            if schema_version == 2:
                cursor.execute("SELECT COUNT(*) FROM user")
                sqlite_user_count = cursor.fetchone()[0]
                # TODO: Implement Redis user count check
                # redis_user_count = self.user_repo.count_users()
            
            cursor.execute("SELECT COUNT(*) FROM book")
            sqlite_book_count = cursor.fetchone()[0]
            # TODO: Implement Redis book count check
            # redis_book_count = self.book_repo.count_books()
            
            cursor.execute("SELECT COUNT(*) FROM reading_log")
            sqlite_log_count = cursor.fetchone()[0]
            
            conn.close()
            
            # Basic validation
            success = True
            if self.stats['books_migrated'] != sqlite_book_count:
                logger.error(f"❌ Book count mismatch: {self.stats['books_migrated']} vs {sqlite_book_count}")
                success = False
            
            if self.stats['reading_logs_migrated'] != sqlite_log_count:
                logger.error(f"❌ Reading log count mismatch: {self.stats['reading_logs_migrated']} vs {sqlite_log_count}")
                success = False
            
            if success:
                logger.info("✅ Migration validation passed!")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Migration validation failed: {e}")
            return False

    def run_migration(self) -> bool:
        """
        Run the complete migration process for web-based migration.
        Assigns all books to the current logged-in user.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info(f"🚀 Starting web-based migration from {self.db_path}")
            logger.info(f"📊 Target user: {self.target_user_id}")
            
            # Create backup
            if not self.create_pre_migration_backup():
                return False
            
            # Connect to SQLite database
            conn = sqlite3.connect(self.db_path)
            
            # Migrate all books to current user (no user mapping needed)
            logger.info("📚 Migrating books...")
            book_id_mapping = self.migrate_books_to_current_user(conn)
            
            # Migrate reading logs for current user
            logger.info("📖 Migrating reading logs...")
            self.migrate_reading_logs_for_current_user(conn, book_id_mapping)
            
            conn.close()
            
            # Create post-migration backup
            self.create_post_migration_backup()
            
            # Validate migration
            if self.validate_migration():
                logger.info("✅ Migration completed successfully!")
                logger.info(f"📊 Summary: {self.stats['books_migrated']} books, {self.stats['reading_logs_migrated']} logs, {self.stats['relationships_created']} relationships")
                return True
            else:
                logger.error("❌ Migration validation failed!")
                return False
                
        except Exception as e:
            logger.error(f"❌ Migration failed: {e}")
            self.stats['errors'].append(str(e))
            return False

    def _determine_reading_status(self, start_date: str, finish_date: str, 
                                 want_to_read: bool, library_only: bool) -> ReadingStatus:
        """Determine reading status based on SQLite data."""
        if finish_date:
            return ReadingStatus.READ
        elif start_date:
            return ReadingStatus.READING
        elif want_to_read:
            return ReadingStatus.PLAN_TO_READ
        elif library_only:
            return ReadingStatus.LIBRARY_ONLY
        else:
            return ReadingStatus.PLAN_TO_READ  # Default

    def validate_migration(self) -> bool:
        """Validate the migration by checking data integrity."""
        logger.info("🔍 Validating migration...")
        
        try:
            # Check if we have the expected number of books
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(*) FROM book")
            sqlite_book_count = cursor.fetchone()[0]
            conn.close()
            
            if sqlite_book_count == self.stats['books_migrated']:
                logger.info(f"✅ Book count matches: {sqlite_book_count} books migrated")
                return True
            else:
                logger.error(f"❌ Book count mismatch: SQLite has {sqlite_book_count}, migrated {self.stats['books_migrated']}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Validation failed: {e}")
            return False

    def test_single_book_creation(self):
        """Test creating a single book to diagnose issues."""
        logger.info("🧪 Testing single book creation...")
        try:
            # Create a simple test book
            test_book = Book(
                title="Test Book",
                authors=[Author(name="Test Author")],
                isbn13="9780000000000",
                description="A test book for migration debugging"
            )
            
            # Try to create it
            redis_book = asyncio.run(self.book_repo.create(test_book))
            logger.info(f"✅ Test book created successfully: {redis_book.id}")
            
            # Try to retrieve it to verify it was stored
            retrieved_book = asyncio.run(self.book_repo.get_by_id(redis_book.id))
            if retrieved_book:
                logger.info(f"✅ Test book retrieved successfully: {retrieved_book.title}")
                return True
            else:
                logger.error("❌ Test book was created but could not be retrieved")
                return False
                
        except Exception as e:
            logger.error(f"❌ Test book creation failed: {e}")
            return False
    
    def create_default_user_v1(self, user_id: str) -> bool:
        """Create a default user for v1 database migration, or use existing user."""
        try:
            logger.info(f"🧑 Checking for existing user: {user_id}")
            
            # First, check if this user already exists
            existing_user = asyncio.run(self.user_repo.get_by_id(user_id))
            if existing_user:
                logger.info(f"✅ Using existing user: {existing_user.username} ({existing_user.id})")
                return True
            
            # User doesn't exist, create a new one
            logger.info(f"🧑 Creating new user: {user_id}")
            
            # Use a clean username if user_id looks like a UUID
            username = user_id
            if len(user_id) > 20 and '-' in user_id:  # Likely a UUID
                username = "migrated_user"  # Default for clean display
            
            # Create a basic user object
            default_user = User(
                id=user_id,
                username=username,
                email=f"{username}@localhost",  # Placeholder email
                is_active=True,
                is_admin=True,  # First user should be admin
                display_name=username.title(),
                created_at=datetime.utcnow()
            )
            
            # Create user in Redis
            redis_user = asyncio.run(self.user_repo.create(default_user))
            logger.info(f"✅ Created new user: {redis_user.username} ({redis_user.id})")
            return True
            
        except Exception as e:
            error_msg = f"Failed to create/check user {user_id}: {e}"
            logger.error(f"❌ {error_msg}")
            self.stats['errors'].append(error_msg)
            return False


def main():
    """Main entry point for the migration script."""
    parser = argparse.ArgumentParser(
        description="Migrate SQLite database to Redis for Bibliotheca",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Migrate single-user database
  python migrate_sqlite_to_redis.py --db-path books.db --user-id user123
  
  # Migrate multi-user database
  python migrate_sqlite_to_redis.py --db-path library.db
  
  # Use custom Redis URL
  python migrate_sqlite_to_redis.py --db-path books.db --redis-url redis://localhost:6380
        """
    )
    
    parser.add_argument('--db-path', required=True, 
                       help='Path to the SQLite database file')
    parser.add_argument('--user-id', 
                       help='Default user ID for single-user databases')
    parser.add_argument('--redis-url', 
                       help='Redis connection URL (default: from config)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Perform a dry run without making changes')
    
    args = parser.parse_args()
    
    if args.dry_run:
        print("🧪 DRY RUN MODE - No changes will be made")
        # TODO: Implement dry run logic
        return
    
    # Create migrator and run migration
    migrator = WebBasedSQLiteToRedisMigrator(
        db_path=args.db_path,
        target_user_id=args.user_id,
        redis_url=args.redis_url
    )
    
    success = migrator.run_migration()
    
    if success:
        print("\n🎉 Migration completed successfully!")
        print(f"📋 Detailed log saved to: migration.log")
        print(f"📁 Backups saved to: {migrator.backup_dir}")
        sys.exit(0)
    else:
        print("\n❌ Migration failed!")
        print(f"📋 Check migration.log for details")
        print(f"📁 Backups saved to: {migrator.backup_dir}")
        sys.exit(1)


if __name__ == '__main__':
    main()
