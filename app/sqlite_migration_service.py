"""
SQLite Migration Service

This module handles migration from legacy SQLite databases (v1 and v1.5) to the new KuzuDB system.
Uses the existing batch import infrastructure for optimal performance.
"""

import sqlite3
import tempfile
import os
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple, Any
from app.simplified_book_service import SimplifiedBook
from app.utils.safe_kuzu_manager import SafeKuzuManager, get_safe_kuzu_manager

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


class SQLiteMigrationService:
    """Service for migrating legacy SQLite databases to the new system."""
    
    def __init__(self):
        self.supported_versions = ['v1', 'v1.5']
    
    def detect_database_version(self, sqlite_file_path: str) -> str:
        """
        Detect the version of the SQLite database by examining table structure.
        
        Returns:
            'v1' for single-user databases
            'v1.5' for multi-user databases
            'unknown' for unrecognized formats
        """
        try:
            conn = sqlite3.connect(sqlite_file_path)
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {row[0] for row in cursor.fetchall()}
            
            conn.close()
            
            # Determine version based on table presence
            if 'user' in tables and 'task' in tables:
                return 'v1.5'
            elif 'book' in tables and 'reading_log' in tables:
                return 'v1'
            else:
                return 'unknown'
                
        except Exception as e:
            print(f"Error detecting database version: {e}")
            return 'unknown'
    
    def migrate_sqlite_database(self, sqlite_file_path: str, target_user_id: str, 
                               create_default_user: bool = False) -> Dict[str, Any]:
        """
        Migrate books from an SQLite database using batch infrastructure.
        
        Args:
            sqlite_file_path: Path to the SQLite database file
            target_user_id: ID of the user to assign books to (for v1.5, ignored for v1)
            create_default_user: Whether to use the first admin user for v1.5 databases
            
        Returns:
            Migration results dictionary
        """
        print(f"🔄 [SQLITE_MIGRATION] Starting SQLite migration")
        
        # Detect database version
        db_version = self.detect_database_version(sqlite_file_path)
        print(f"📋 [SQLITE_MIGRATION] Detected database version: {db_version}")
        
        if db_version == 'unknown':
            raise ValueError("Unrecognized SQLite database format")
        
        # Extract data based on version
        if db_version == 'v1':
            return self._migrate_v1_database(sqlite_file_path, target_user_id)
        elif db_version == 'v1.5':
            return self._migrate_v1_5_database(sqlite_file_path, target_user_id, create_default_user)
        else:
            raise ValueError(f"Unsupported database version: {db_version}")
    
    def _migrate_v1_database(self, sqlite_file_path: str, target_user_id: str) -> Dict[str, Any]:
        """Migrate v1 (single-user) SQLite database."""
        print(f"📋 [V1_MIGRATION] Starting v1 database migration for user {target_user_id}")
        
        # ===== PHASE 1: Extract all data from SQLite =====
        conn = sqlite3.connect(sqlite_file_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            # Extract books
            cursor.execute("SELECT * FROM book")
            book_rows = cursor.fetchall()
            print(f"📋 [V1_MIGRATION] Found {len(book_rows)} books")
            
            # Extract reading logs
            cursor.execute("SELECT * FROM reading_log")
            reading_log_rows = cursor.fetchall()
            print(f"📋 [V1_MIGRATION] Found {len(reading_log_rows)} reading log entries")
            
            # Build reading log lookup
            reading_logs_by_book = {}
            for log in reading_log_rows:
                book_id = log['book_id']
                if book_id not in reading_logs_by_book:
                    reading_logs_by_book[book_id] = []
                reading_logs_by_book[book_id].append(log['date'])
            
            # Convert to SimplifiedBook objects
            all_books_data = []
            all_isbns = set()
            all_authors = set()
            
            for row in book_rows:
                simplified_book = self._convert_v1_row_to_simplified_book(dict(row), reading_logs_by_book)
                
                if simplified_book:
                    all_books_data.append(simplified_book)
                    
                    # Collect ISBNs and authors for batch processing
                    if simplified_book.isbn13:
                        all_isbns.add(simplified_book.isbn13)
                    if simplified_book.isbn10:
                        all_isbns.add(simplified_book.isbn10)
                    if simplified_book.author:
                        all_authors.add(simplified_book.author)
            
        finally:
            conn.close()
        
        
        # Use batch processing pipeline
        return self._process_books_with_batch_pipeline(all_books_data, all_isbns, all_authors, target_user_id)
    
    def _migrate_v1_5_database(self, sqlite_file_path: str, target_user_id: str, 
                              create_default_user: bool = False) -> Dict[str, Any]:
        """Migrate v1.5 (multi-user) SQLite database."""
        print(f"📋 [V1.5_MIGRATION] Starting v1.5 database migration")
        
        # ===== PHASE 1: Extract all data from SQLite =====
        conn = sqlite3.connect(sqlite_file_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            # First, handle user logic for v1.5
            cursor.execute("SELECT * FROM user WHERE is_admin = 1 ORDER BY created_at ASC LIMIT 1")
            admin_user = cursor.fetchone()
            
            if admin_user and create_default_user:
                print(f"📋 [V1.5_MIGRATION] Found admin user: {admin_user['username']} (ID: {admin_user['id']})")
                sqlite_user_id = admin_user['id']
                migration_note = f"Migrated from admin user '{admin_user['username']}'"
            else:
                # Use provided target_user_id and migrate all books regardless of original user_id
                sqlite_user_id = None  # Will migrate all books
                migration_note = f"Migrated all books to user {target_user_id}"
            
            # Extract books (filter by user if we found an admin)
            if sqlite_user_id:
                cursor.execute("SELECT * FROM book WHERE user_id = ?", (sqlite_user_id,))
            else:
                cursor.execute("SELECT * FROM book")
            
            book_rows = cursor.fetchall()
            print(f"📋 [V1.5_MIGRATION] Found {len(book_rows)} books")
            
            # Extract reading logs (filter by user if applicable)
            if sqlite_user_id:
                cursor.execute("""
                    SELECT rl.* FROM reading_log rl 
                    JOIN book b ON rl.book_id = b.id 
                    WHERE b.user_id = ?
                """, (sqlite_user_id,))
            else:
                cursor.execute("SELECT * FROM reading_log")
            
            reading_log_rows = cursor.fetchall()
            print(f"📋 [V1.5_MIGRATION] Found {len(reading_log_rows)} reading log entries")
            
            # Build reading log lookup
            reading_logs_by_book = {}
            for log in reading_log_rows:
                book_id = log['book_id']
                if book_id not in reading_logs_by_book:
                    reading_logs_by_book[book_id] = []
                reading_logs_by_book[book_id].append(log['date'])
            
            # Convert to SimplifiedBook objects
            all_books_data = []
            all_isbns = set()
            all_authors = set()
            
            for row in book_rows:
                simplified_book = self._convert_v1_5_row_to_simplified_book(dict(row), reading_logs_by_book)
                
                if simplified_book:
                    all_books_data.append(simplified_book)
                    
                    # Collect ISBNs and authors for batch processing
                    if simplified_book.isbn13:
                        all_isbns.add(simplified_book.isbn13)
                    if simplified_book.isbn10:
                        all_isbns.add(simplified_book.isbn10)
                    if simplified_book.author:
                        all_authors.add(simplified_book.author)
            
        finally:
            conn.close()
        
        print(f"📝 [V1.5_MIGRATION] {migration_note}")
        
        # Use batch processing pipeline
        result = self._process_books_with_batch_pipeline(all_books_data, all_isbns, all_authors, target_user_id)
        result['migration_note'] = migration_note
        return result
    
    def _process_books_with_batch_pipeline(self, all_books_data: List[SimplifiedBook], 
                                         all_isbns: set, all_authors: set, 
                                         target_user_id: str) -> Dict[str, Any]:
        """Process books using the existing batch import infrastructure."""
        
        # ===== PHASE 2-3: Use existing batch API functions =====
        from app.routes.import_routes import batch_fetch_book_metadata
        
        print(f"📚 [BATCH_MIGRATION] Batch fetching book metadata for {len(all_isbns)} ISBNs...")
        book_api_data = batch_fetch_book_metadata(list(all_isbns))
        
        # For migration, we'll use the author data from SQLite without API enhancement
        print(f"👥 [BATCH_MIGRATION] Using {len(all_authors)} authors from SQLite data...")  
        author_api_data = {}  # Empty dict since we don't have batch author metadata
        
        # ===== PHASE 4-5: Use existing book creation pipeline =====
        from app.services import book_service
        from app.infrastructure.kuzu_repositories import KuzuUserBookRepository
        from app.domain.models import ReadingStatus
        
        user_book_repo = KuzuUserBookRepository()
        
        
        success_count = 0
        error_count = 0
        migration_details = []
        
        for book_data in all_books_data:
            try:
                # Convert to domain object using the enhanced API data
                domain_book = self._convert_simplified_book_to_domain(book_data, book_api_data, author_api_data)
                
                # Find or create book logic
                existing_book = None
                
                # Try to find existing book by ISBN
                if book_data.isbn13:
                    existing_book = book_service.get_book_by_isbn_for_user_sync(book_data.isbn13, target_user_id)
                if not existing_book and book_data.isbn10:
                    existing_book = book_service.get_book_by_isbn_for_user_sync(book_data.isbn10, target_user_id)
                
                if existing_book:
                    # Book already exists for this user
                    created_book = existing_book
                else:
                    # Create new book (this automatically creates the user-book relationship)
                    created_book = book_service.create_book_sync(domain_book, target_user_id)
                
                if created_book:
                    # Get book ID (handle both Book objects and book IDs)
                    book_id = getattr(created_book, 'id', None) if hasattr(created_book, 'id') else str(created_book)
                    
                    if book_id:
                        # Update with personal information from SQLite
                        self._update_personal_information(user_book_repo, target_user_id, book_id, book_data)
                        
                        # Determine reading status from SQLite data
                        reading_status = self._determine_reading_status(book_data)
                        
                        success_count += 1
                        migration_details.append({
                            'title': book_data.title,
                            'status': 'success',
                            'reading_status': reading_status.value
                        })
                    else:
                        error_count += 1
                        migration_details.append({
                            'title': book_data.title,
                            'status': 'failed_creation',
                            'error': 'Book created but no ID available'
                        })
                else:
                    error_count += 1
                    migration_details.append({
                        'title': book_data.title,
                        'status': 'failed_creation',
                        'error': 'Failed to create book'
                    })
                    
            except Exception as e:
                error_count += 1
                migration_details.append({
                    'title': book_data.title,
                    'status': 'error',
                    'error': str(e)
                })
        
        
        return {
            'total_books': len(all_books_data),
            'success_count': success_count,
            'error_count': error_count,
            'api_calls_made': len(all_isbns) + len(all_authors),
            'migration_details': migration_details
        }
    
    def _convert_v1_row_to_simplified_book(self, row: dict, reading_logs: dict) -> Optional[SimplifiedBook]:
        """Convert a v1 SQLite row to SimplifiedBook format."""
        if not row.get('title'):
            return None
        
        # Determine reading status from dates and flags
        reading_status = 'plan_to_read'  # default
        if row.get('finish_date'):
            reading_status = 'read'
        elif row.get('start_date'):
            reading_status = 'reading'
        elif row.get('want_to_read'):
            reading_status = 'plan_to_read'
        
        return SimplifiedBook(
            title=row['title'],
            author=row.get('author', 'Unknown Author'),
            isbn13=self._extract_isbn13(row.get('isbn')),
            isbn10=self._extract_isbn10(row.get('isbn')),
            cover_url=row.get('cover_url'),
            reading_status=reading_status,
            date_read=self._parse_date(row.get('finish_date')),
            date_started=self._parse_date(row.get('start_date')),
            reading_logs=reading_logs.get(row['id'], [])
        )
    
    def _convert_v1_5_row_to_simplified_book(self, row: dict, reading_logs: dict) -> Optional[SimplifiedBook]:
        """Convert a v1.5 SQLite row to SimplifiedBook format."""
        if not row.get('title'):
            return None
        
        # Determine reading status from dates and flags
        reading_status = 'plan_to_read'  # default
        if row.get('finish_date'):
            reading_status = 'read'
        elif row.get('start_date'):
            reading_status = 'reading'
        elif row.get('want_to_read'):
            reading_status = 'plan_to_read'
        
        return SimplifiedBook(
            title=row['title'],
            author=row.get('author', 'Unknown Author'),
            isbn13=self._extract_isbn13(row.get('isbn')),
            isbn10=self._extract_isbn10(row.get('isbn')),
            description=row.get('description'),
            publisher=row.get('publisher'),
            published_date=row.get('published_date'),
            page_count=self._safe_int(row.get('page_count')),
            language=row.get('language', 'en'),
            cover_url=row.get('cover_url'),
            categories=self._parse_categories(row.get('categories')),
            average_rating=self._safe_float(row.get('average_rating')),
            rating_count=self._safe_int(row.get('rating_count')),
            reading_status=reading_status,
            date_read=self._parse_date(row.get('finish_date')),
            date_started=self._parse_date(row.get('start_date')),
            date_added=self._parse_date(row.get('created_at')),
            reading_logs=reading_logs.get(row['id'], [])
        )
    
    def _convert_simplified_book_to_domain(self, book_data: SimplifiedBook, 
                                         book_api_data: dict, author_api_data: dict):
        """Convert SimplifiedBook to domain object with API enhancements."""
        from app.domain.models import Book as DomainBook, Person, BookContribution, ContributionType, Publisher
        
        # Start with SQLite data, enhance with API data if available
        title = book_data.title
        description = book_data.description
        publisher_name = book_data.publisher
        page_count = book_data.page_count
        language = book_data.language or 'en'
        cover_url = book_data.cover_url
        categories = book_data.categories
        
        # Enhance with API data if available
        isbn_for_lookup = book_data.isbn13 or book_data.isbn10
        if isbn_for_lookup and isbn_for_lookup in book_api_data:
            api_data = book_api_data[isbn_for_lookup]
            title = api_data.get('title', title)
            description = description or api_data.get('description')
            publisher_name = publisher_name or api_data.get('publisher')
            page_count = page_count or api_data.get('page_count')
            language = language or api_data.get('language', 'en')
            if not cover_url and api_data.get('cover_url'):
                from app.utils.book_utils import normalize_cover_url
                cover_url = normalize_cover_url(api_data.get('cover_url'))
            if not categories and api_data.get('categories'):
                categories = api_data.get('categories')
        
        # Handle contributors
        contributors = []
        if book_data.author:
            person = Person(name=book_data.author)
            contribution = BookContribution(
                person=person,
                contribution_type=ContributionType.AUTHORED,
                order=0
            )
            contributors.append(contribution)
        
        # Create domain book
        return DomainBook(
            title=title,
            contributors=contributors,
            isbn13=book_data.isbn13,
            isbn10=book_data.isbn10,
            description=description,
            publisher=Publisher(name=publisher_name) if publisher_name else None,
            page_count=page_count,
            language=language,
            cover_url=cover_url,
            raw_categories=categories,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
    
    def _determine_reading_status(self, book_data: SimplifiedBook):
        """Determine ReadingStatus enum from SimplifiedBook data."""
        from app.domain.models import ReadingStatus
        
        status_str = book_data.reading_status
        if status_str == 'read':
            return ReadingStatus.READ
        elif status_str == 'reading':
            return ReadingStatus.READING
        elif status_str == 'plan_to_read':
            return ReadingStatus.PLAN_TO_READ
        else:
            return ReadingStatus.PLAN_TO_READ  # default
    
    def _update_personal_information(self, user_book_repo, user_id: str, book_id: str, book_data: SimplifiedBook):
        """Update personal information from migrated data."""
        try:
            # Build migration notes
            migration_notes = []
            if hasattr(book_data, 'reading_logs') and book_data.reading_logs:
                log_dates = [str(log_date) for log_date in book_data.reading_logs[:5]]  # First 5 dates
                if len(book_data.reading_logs) > 5:
                    log_dates.append(f"... and {len(book_data.reading_logs) - 5} more")
                migration_notes.append(f"Reading logs: {', '.join(log_dates)}")
            
            migration_notes.append("Migrated from SQLite database")
            personal_notes = "; ".join(migration_notes)
            
            # Update reading status using the repository method
            reading_status = self._determine_reading_status(book_data)
            from app.services.kuzu_async_helper import run_async
            run_async(user_book_repo.update_reading_status(user_id, book_id, reading_status.value))
            
            # Update personal metadata instead of OWNS
            try:
                from app.services.personal_metadata_service import personal_metadata_service
                custom_updates = {}
                if book_data.date_read:
                    custom_updates['finish_date'] = book_data.date_read
                if book_data.date_started:
                    custom_updates['start_date'] = book_data.date_started
                if book_data.date_added:
                    custom_updates['date_added'] = book_data.date_added
                personal_metadata_service.update_personal_metadata(
                    user_id, book_id, personal_notes=personal_notes, custom_updates=custom_updates or None, merge=True
                )
            except Exception as _pm_err:  # pragma: no cover
                print(f"⚠️ [MIGRATION] Personal metadata update fallback failed: {_pm_err}")
                
        except Exception as e:
            print(f"❌ [MIGRATION] Error updating personal information: {e}")
            # Continue migration even if personal info update fails
    
    # Helper methods
    def _extract_isbn13(self, isbn_str):
        """Extract ISBN13 from mixed ISBN string."""
        if not isbn_str:
            return None
        clean = ''.join(filter(str.isdigit, str(isbn_str)))
        return clean if len(clean) == 13 else None
    
    def _extract_isbn10(self, isbn_str):
        """Extract ISBN10 from mixed ISBN string."""
        if not isbn_str:
            return None
        clean = ''.join(c for c in str(isbn_str) if c.isdigit() or c.upper() == 'X')
        return clean if len(clean) == 10 else None
    
    def _safe_int(self, value):
        """Safely convert value to int."""
        if not value:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_float(self, value):
        """Safely convert value to float."""
        if not value:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _parse_date(self, date_str):
        """Parse date string to ISO format date string."""
        if not date_str:
            return None
        
        # Try common date formats
        date_formats = [
            '%Y-%m-%d',
            '%Y/%m/%d', 
            '%m/%d/%Y',
            '%d/%m/%Y',
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f'
        ]
        
        for fmt in date_formats:
            try:
                parsed_date = datetime.strptime(str(date_str), fmt)
                return parsed_date.date().isoformat()  # Return ISO format string
            except ValueError:
                continue
        
        return None
    
    def _parse_categories(self, categories_str):
        """Parse categories string to list of strings."""
        if not categories_str:
            return []
        
        # If it's already a list, return it
        if isinstance(categories_str, list):
            return categories_str
        
        # If it's a string, split by common delimiters
        if isinstance(categories_str, str):
            # Try different delimiters
            for delimiter in [';', ',', '|', '\n']:
                if delimiter in categories_str:
                    categories = [cat.strip() for cat in categories_str.split(delimiter)]
                    return [cat for cat in categories if cat]  # Remove empty strings
            
            # If no delimiter found, return as single item
            return [categories_str.strip()] if categories_str.strip() else []
        
        return []
