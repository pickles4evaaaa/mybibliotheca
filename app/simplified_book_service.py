"""
Simplified Book Service - Decoupled Architecture
Separates book creation from user relationships for better persistence.
"""

import os as _os_for_import_verbosity
_IMPORT_VERBOSE = (
    (_os_for_import_verbosity.getenv('VERBOSE') or 'false').lower() == 'true'
    or (_os_for_import_verbosity.getenv('IMPORT_VERBOSE') or 'false').lower() == 'true'
)
def _dprint(*args, **kwargs):
    if _IMPORT_VERBOSE:
        __builtins__.print(*args, **kwargs)

# Redirect module print to conditional debug print
print = _dprint

import uuid
import json
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field

from .domain.models import Book, Person, Publisher, Series, Category, BookContribution, ContributionType, MediaType
from .infrastructure.kuzu_graph import safe_execute_kuzu_query
from .services.kuzu_custom_field_service import KuzuCustomFieldService
from app.utils.user_settings import get_default_book_format


def _convert_query_result_to_list(result):
    """Convert SafeKuzuManager query result to legacy list format"""
    if hasattr(result, 'get_as_df'):
        return result.get_as_df().to_dict('records')
    elif hasattr(result, 'get_next'):
        rows = []
        while result.has_next():
            rows.append(result.get_next())
        return rows
    else:
        return list(result) if result else []


class BookAlreadyExistsError(Exception):
    """Exception raised when attempting to add a duplicate book."""
    def __init__(self, book_id: str, message: str = "Book already exists in library"):
        self.book_id = book_id
        self.message = message
        super().__init__(self.message)


def normalize_goodreads_value(value, field_type='text'):
    """
    Normalize values from Goodreads CSV exports that use Excel text formatting.
    Goodreads exports often have values like ="123456789" or ="" to force text formatting.
    """
    if not value or not isinstance(value, str):
        return value.strip() if value else ''
    
    # Remove Excel text formatting: ="value" -> value
    if value.startswith('="') and value.endswith('"'):
        value = value[2:-1]  # Remove =" prefix and " suffix
    elif value.startswith('=') and value.endswith('"'):
        value = value[1:-1]  # Remove = prefix and " suffix  
    elif value == '=""':
        value = ''  # Empty quoted value
    
    # Additional cleaning for ISBN fields
    if field_type == 'isbn':
        # Remove any remaining quotes, equals, or whitespace
        value = value.replace('"', '').replace('=', '').strip()
        # Validate that it looks like an ISBN (digits, X, hyphens only)
        if value and not all(c.isdigit() or c in 'X-' for c in value):
            # If it doesn't look like an ISBN, it might be corrupted
            pass  # Continue with potentially corrupted ISBN
    
    return value.strip()


def _normalize_publisher_name(value: Optional[str]) -> Optional[str]:
    """Normalize publisher strings from CSV/API:
    - Handle Goodreads-style ="..." wrapping
    - Strip leading/trailing straight or curly quotes
    - Trim whitespace
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return s
    # Excel text wrapper ="value"
    if s.startswith('="') and s.endswith('"') and len(s) >= 3:
        s = s[2:-1].strip()
    # Strip paired quotes repeatedly (straight or curly)
    quotes = ('"', '“', '”', "'")
    changed = True
    while changed and len(s) >= 2:
        changed = False
        for ql, qr in (("\"", "\""), ('“', '”'), ("'", "'")):
            if s.startswith(ql) and s.endswith(qr):
                s = s[1:-1].strip()
                changed = True
    return s


@dataclass
class SimplifiedBook:
    """Simplified book model focused on core bibliographic data only."""
    title: str
    author: str  # Primary author as string for simplicity
    isbn13: Optional[str] = None
    isbn10: Optional[str] = None
    subtitle: Optional[str] = None
    description: Optional[str] = None
    publisher: Optional[str] = None
    published_date: Optional[str] = None
    page_count: Optional[int] = None
    language: str = "en"
    cover_url: Optional[str] = None
    series: Optional[str] = None
    series_volume: Optional[str] = None
    series_order: Optional[int] = None
    categories: List[str] = field(default_factory=list)
    # Raw hierarchical category path strings (e.g., "Computers / Programming / General")
    # When present, use these to build PARENT_CATEGORY graph and link the leaf to the book
    raw_categories: Optional[List[str]] = None
    google_books_id: Optional[str] = None
    openlibrary_id: Optional[str] = None
    asin: Optional[str] = None
    average_rating: Optional[float] = None
    rating_count: Optional[int] = None
    media_type: Optional[str] = None  # physical, ebook, audiobook, kindle
    quantity: int = 1  # Number of copies owned (default 1)
    
    # Additional person type fields (like additional authors)
    additional_authors: Optional[str] = None  # Comma-separated string for simplicity
    narrator: Optional[str] = None           # Comma-separated string
    editor: Optional[str] = None             # Comma-separated string  
    translator: Optional[str] = None         # Comma-separated string
    illustrator: Optional[str] = None        # Comma-separated string
    
    # Custom metadata fields for batch import
    global_custom_metadata: Dict[str, Any] = field(default_factory=dict)
    personal_custom_metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Migration-specific fields
    reading_status: Optional[str] = None
    date_read: Optional[str] = None  # Can be date object or string
    date_started: Optional[str] = None
    date_added: Optional[str] = None
    user_rating: Optional[float] = None
    personal_notes: Optional[str] = None
    reading_logs: Optional[List] = field(default_factory=list)  # For tracking reading session dates


@dataclass
class UserBookAnnotation:
    """User's personal annotations and reading data for a book in universal library."""
    user_id: str
    book_id: str
    reading_status: str = ""  # User's reading progress (empty = not set)
    date_added: Optional[datetime] = None  # When user started tracking this book
    user_rating: Optional[float] = None   # User's personal rating
    personal_notes: Optional[str] = None  # User's notes about the book
    custom_metadata: Optional[Dict[str, Any]] = None  # User's custom fields
    
    # Removed: ownership_status, media_type, location_id
    # Books are universal and stored at locations, not owned by users
class SimplifiedBookService:
    """
    Simplified book service with clean separation:
    1. Create books as standalone global entities
    2. Create user relationships separately
    """
    
    def __init__(self):
        # Use global safe connection management instead of separate instance
        self.custom_field_service = KuzuCustomFieldService()
        # Using global safe_execute_kuzu_query for thread-safe database access
    
    def _convert_to_date(self, date_value):
        """Convert various date formats to a format suitable for KuzuDB DATE type."""
        from datetime import date, datetime
        import re
        
        if not date_value:
            return None
            
        # If it's already a date object, convert to string
        if isinstance(date_value, date):
            return date_value
        
        # If it's a datetime object, extract the date
        if isinstance(date_value, datetime):
            return date_value.date()
            
        # If it's a string, parse it
        if isinstance(date_value, str):
            # Handle common date formats
            date_str = date_value.strip()
            
            # Already in YYYY-MM-DD format (perfect for KuzuDB)
            if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                return datetime.strptime(date_str, '%Y-%m-%d').date()
            
            # Just a year (like '2006')
            if re.match(r'^\d{4}$', date_str):
                return datetime.strptime(f'{date_str}-01-01', '%Y-%m-%d').date()
                
            # Try other common formats
            for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y']:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    return parsed_date.date()
                except ValueError:
                    continue
                    
        return None
    
    def _convert_query_result_to_list(self, query_result):
        """Convert QueryResult to list format for backward compatibility."""
        if not query_result or not hasattr(query_result, '__iter__'):
            return []
        
        try:
            # Convert QueryResult to list of dictionaries
            result_list = []
            for row in query_result:
                # Handle both tuple-like and dict-like row formats
                if hasattr(row, '_asdict'):
                    # NamedTuple-like result
                    row_dict = row._asdict()
                elif hasattr(row, 'keys'):
                    # Dict-like result
                    row_dict = dict(row)
                else:
                    # Tuple-like result - create numbered columns
                    row_dict = {f'col_{i}': val for i, val in enumerate(row)}
                
                result_list.append(row_dict)
            
            return result_list
            
        except Exception as e:
            print(f"⚠️ [QUERY_CONVERT] Error converting QueryResult: {e}")
            return []
    
    async def create_standalone_book(self, book_data: SimplifiedBook) -> Optional[str]:
        """
        Create a book as a standalone global entity.
        Returns book_id if successful, None if failed.
        """
        try:
            book_id = str(uuid.uuid4())
            
            # Create standalone book with contributors            # Prepare core book node data
            book_node_data = {
                'id': book_id,
                'title': book_data.title or '',
                'subtitle': getattr(book_data, 'subtitle', '') or '',
                'normalized_title': (book_data.title or '').lower(),
                'description': book_data.description or '',
                'published_date': self._convert_to_date(book_data.published_date),
                'page_count': book_data.page_count or 0,
                'language': book_data.language or 'en',
                # Only store cover_url (the schema field that exists)
                'cover_url': book_data.cover_url or '',
                # Store both ISBN formats 
                'isbn13': book_data.isbn13 or '',
                'isbn10': book_data.isbn10 or '',
                'asin': book_data.asin or '',
                'google_books_id': getattr(book_data, 'google_books_id', '') or '',
                'openlibrary_id': getattr(book_data, 'openlibrary_id', '') or '',
                'average_rating': book_data.average_rating or 0.0,
                'rating_count': book_data.rating_count or 0,
                'series': book_data.series or '',
                'series_volume': book_data.series_volume,
                'series_order': book_data.series_order,
                'media_type': getattr(book_data, 'media_type', '') or '',
                'quantity': getattr(book_data, 'quantity', 1) or 1,
                'created_at_str': datetime.now(timezone.utc).isoformat(),
                'updated_at_str': datetime.now(timezone.utc).isoformat()
            }
            
            # Remove only the fields that can be None (series_volume and series_order)
            if book_node_data['series_volume'] is None:
                del book_node_data['series_volume']
            if book_node_data['series_order'] is None:
                del book_node_data['series_order']
            
            # Enhanced debugging for ISBN fields
            description = book_node_data.get('description')
            # Only print basic info to reduce log noise
            if description:
                print(f"📖 Creating: {book_node_data.get('title')} by {book_data.author}")
            
            # 1. Create book node (SINGLE TRANSACTION) - Use explicit property syntax for KuzuDB
            # Handle optional fields that might be None
            query_params = book_node_data.copy()
            
            # Ensure series_volume and series_order have default values if missing
            if 'series_volume' not in query_params:
                query_params['series_volume'] = None
            if 'series_order' not in query_params:
                query_params['series_order'] = None
            
            book_result = safe_execute_kuzu_query(
                """
                CREATE (b:Book {
                    id: $id,
                    title: $title,
                    subtitle: $subtitle,
                    normalized_title: $normalized_title,
                    isbn13: $isbn13,
                    isbn10: $isbn10,
                    asin: $asin,
                    description: $description,
                    published_date: $published_date,
                    page_count: $page_count,
                    language: $language,
                    cover_url: $cover_url,
                    google_books_id: $google_books_id,
                    openlibrary_id: $openlibrary_id,
                    average_rating: $average_rating,
                    rating_count: $rating_count,
                    series: $series,
                    series_volume: $series_volume,
                    series_order: $series_order,
                    media_type: $media_type,
                    quantity: $quantity,
                    created_at: CASE WHEN $created_at_str IS NULL OR $created_at_str = '' THEN NULL ELSE timestamp($created_at_str) END,
                    updated_at: CASE WHEN $updated_at_str IS NULL OR $updated_at_str = '' THEN NULL ELSE timestamp($updated_at_str) END
                })
                RETURN b.id
                """,
                query_params
            )
            if not book_result:
                return None
            
            # 1.5. Download and cache cover image if cover_url is provided
            final_cover_url = book_data.cover_url or ''
            if book_data.cover_url and book_data.cover_url.startswith('http'):
                try:
                    print(f"🖼️ [COVER_DOWNLOAD] Downloading cover for '{book_data.title}': {book_data.cover_url}")
                    
                    # Use persistent covers directory in data folder (same logic as book_routes.py)
                    from pathlib import Path
                    import requests  # type: ignore
                    
                    covers_dir = Path('/app/data/covers')
                    
                    # Fallback to local development path if Docker path doesn't exist
                    if not covers_dir.exists():
                        # Check for data directory from app config
                        try:
                            from flask import current_app
                            data_dir = getattr(current_app.config, 'DATA_DIR', None)
                            if data_dir:
                                covers_dir = Path(data_dir) / 'covers'
                            else:
                                # Last resort - use relative path from app root
                                base_dir = Path(__file__).parent.parent.parent
                                covers_dir = base_dir / 'data' / 'covers'
                        except:
                            # If no Flask context available, use fallback
                            covers_dir = Path('./data/covers')
                    
                    covers_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Generate new filename with UUID
                    book_temp_id = str(uuid.uuid4())
                    file_extension = '.jpg'
                    if book_data.cover_url.lower().endswith('.png'):
                        file_extension = '.png'
                    elif book_data.cover_url.lower().endswith('.gif'):
                        file_extension = '.gif'
                    elif book_data.cover_url.lower().endswith('.webp'):
                        file_extension = '.webp'
                    
                    filename = f"{book_temp_id}{file_extension}"
                    filepath = covers_dir / filename
                    
                    # Download the image
                    response = requests.get(book_data.cover_url, timeout=10, stream=True, 
                                          headers={'User-Agent': 'Mozilla/5.0 (compatible; BookLibrary/1.0)'})
                    response.raise_for_status()
                    
                    with open(filepath, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    
                    # Update the book node with the local cover URL
                    final_cover_url = f"/covers/{filename}"
                    
                    # Update book record with local cover URL
                    update_cover_result = safe_execute_kuzu_query(
                        """
                        MATCH (b:Book {id: $book_id})
                        SET b.cover_url = $cover_url,
                            b.updated_at = CASE WHEN $updated_at_str IS NULL OR $updated_at_str = '' THEN b.updated_at ELSE timestamp($updated_at_str) END
                        RETURN b.id
                        """,
                        {
                            "book_id": book_id,
                            "cover_url": final_cover_url,
                            "updated_at_str": datetime.now(timezone.utc).isoformat()
                        }
                    )
                    
                    if update_cover_result:
                        print(f"✅ [COVER_DOWNLOAD] Successfully downloaded and cached cover: {final_cover_url}")
                    else:
                        print(f"❌ [COVER_DOWNLOAD] Failed to update book record with local cover URL")
                        
                except Exception as cover_error:
                    print(f"⚠️ [COVER_DOWNLOAD] Failed to download cover for '{book_data.title}': {cover_error}")
                    # Continue with original cover URL if download fails
                    final_cover_url = book_data.cover_url
            
            # Update the book_data with the final cover URL for logging
            book_data.cover_url = final_cover_url
            
            
            # Initialize book repository for relationship creation
            from .infrastructure.kuzu_repositories import KuzuBookRepository
            book_repo = KuzuBookRepository()
            
            # Helper function to create person data objects with API enhancement
            def create_person_data(name, enhance_with_api=True):
                class PersonData:
                    def __init__(self, name):
                        self.id = str(uuid.uuid4())
                        self.name = name
                        self.birth_year: Optional[int] = None
                        self.death_year: Optional[int] = None
                        self.bio: str = ""
                        self.openlibrary_id: Optional[str] = None
                        self.image_url: Optional[str] = None
                        self.birth_place: Optional[str] = None
                        self.website: Optional[str] = None
                        self.created_at = datetime.now(timezone.utc)
                
                person_data = PersonData(name)
                
                # Enhance with API data if requested and name is available
                if enhance_with_api and name and name.strip():
                    try:
                        from app.utils.book_utils import search_author_by_name, fetch_author_data
                        
                        print(f"🔍 [PERSON_API] Searching for author metadata: {name}")
                        
                        # Search for author on OpenLibrary
                        author_search_result = search_author_by_name(name)
                        if author_search_result and author_search_result.get('openlibrary_id'):
                            author_id = author_search_result['openlibrary_id']
                            print(f"✅ [PERSON_API] Found OpenLibrary ID for {name}: {author_id}")
                            
                            # Fetch detailed author data
                            author_data = fetch_author_data(author_id)
                            if author_data:
                                print(f"✅ [PERSON_API] Retrieved detailed metadata for {name}")
                                
                                # Update person data with API metadata
                                person_data.bio = author_data.get('bio', '') or ''
                                person_data.openlibrary_id = author_data.get('openlibrary_id', '') or ''
                                person_data.image_url = author_data.get('photo_url', '') or ''
                                person_data.website = author_data.get('wikipedia_url', '') or ''
                                
                                # Parse birth/death dates if available
                                birth_date = author_data.get('birth_date', '')
                                if birth_date:
                                    try:
                                        # Extract year from various date formats
                                        import re
                                        year_match = re.search(r'\b(1[0-9]{3}|20[0-9]{2})\b', str(birth_date))
                                        if year_match:
                                            person_data.birth_year = int(year_match.group(1))
                                            print(f"📅 [PERSON_API] Set birth year for {name}: {person_data.birth_year}")
                                    except (ValueError, TypeError):
                                        pass
                                
                                death_date = author_data.get('death_date', '')
                                if death_date:
                                    try:
                                        # Extract year from various date formats
                                        import re
                                        year_match = re.search(r'\b(1[0-9]{3}|20[0-9]{2})\b', str(death_date))
                                        if year_match:
                                            person_data.death_year = int(year_match.group(1))
                                            print(f"📅 [PERSON_API] Set death year for {name}: {person_data.death_year}")
                                    except (ValueError, TypeError):
                                        pass
                                
                                print(f"🎉 [PERSON_API] Enhanced {name} with: bio={bool(person_data.bio)}, image={bool(person_data.image_url)}, birth_year={person_data.birth_year}")
                            else:
                                print(f"❌ [PERSON_API] No detailed data found for OpenLibrary ID: {author_id}")
                        else:
                            print(f"❌ [PERSON_API] No OpenLibrary ID found for author: {name}")
                    
                    except Exception as e:
                        print(f"⚠️ [PERSON_API] Error fetching metadata for {name}: {e}")
                        # Continue with basic person data if API fetch fails
                
                return person_data
            
            # 2. Create author relationship using clean repository (with auto-fetch)
            if book_data.author and str(book_data.author).strip().lower() != 'unknown':
                try:
                    
                    person_data = create_person_data(book_data.author)
                    
                    # Use the book repository's _ensure_person_exists method
                    author_id = await book_repo._ensure_person_exists(person_data)
                    if author_id:
                        
                        # Create AUTHORED relationship - Use explicit property syntax for KuzuDB
                        authored_result = safe_execute_kuzu_query(
                            """
                            MATCH (p:Person {id: $author_id}), (b:Book {id: $book_id})
                            CREATE (p)-[r:AUTHORED {
                                role: $role,
                                order_index: $order_index,
                                created_at: CASE WHEN $created_at_str IS NULL OR $created_at_str = '' THEN NULL ELSE timestamp($created_at_str) END
                            }]->(b)
                            RETURN r
                            """,
                            {
                                "author_id": author_id,
                                "book_id": book_id,
                                "role": 'authored',
                                "order_index": 0,
                                "created_at_str": datetime.now(timezone.utc).isoformat()
                            }
                        )
                        if not authored_result:
                            print(f"❌ [SIMPLIFIED] Failed to create AUTHORED relationship for main author: {book_data.author}")
                    else:
                        print(f"❌ [SIMPLIFIED] Failed to create main author person: {book_data.author}")
                except Exception as e:
                    import traceback
                    traceback.print_exc()
            
            # 2.5. Handle additional authors if present
            if book_data.additional_authors:
                try:
                    additional_authors_list = [name.strip() for name in book_data.additional_authors.split(',') if name.strip()]
                    for index, author_name in enumerate(additional_authors_list):
                        if not author_name or author_name.strip().lower() == 'unknown':
                            continue
                        
                        person_data = create_person_data(author_name)
                        author_id = await book_repo._ensure_person_exists(person_data)
                        
                        if author_id:
                            authored_result = safe_execute_kuzu_query(
                                """
                                MATCH (p:Person {id: $author_id}), (b:Book {id: $book_id})
                                CREATE (p)-[r:AUTHORED {
                                    role: $role,
                                    order_index: $order_index,
                                    created_at: CASE WHEN $created_at_str IS NULL OR $created_at_str = '' THEN NULL ELSE timestamp($created_at_str) END
                                }]->(b)
                                RETURN r
                                """,
                                {
                                    "author_id": author_id,
                                    "book_id": book_id,
                                    "role": 'authored',
                                    "order_index": index + 1,
                                    "created_at_str": datetime.now(timezone.utc).isoformat()
                                }
                            )
                            if authored_result:
                                print(f"✅ [SIMPLIFIED] Created AUTHORED relationship for additional author: {author_name}")
                            else:
                                print(f"❌ [SIMPLIFIED] Failed to create AUTHORED relationship for additional author: {author_name}")
                        else:
                            print(f"❌ [SIMPLIFIED] Failed to create additional author person: {author_name}")
                except Exception as e:
                    print(f"❌ [SIMPLIFIED] Error creating additional author relationship: {e}")
            
            # 2.6. Handle narrator if present
            if book_data.narrator:
                try:
                    narrator_list = [name.strip() for name in book_data.narrator.split(',') if name.strip()]
                    for index, narrator_name in enumerate(narrator_list):
                        if not narrator_name or narrator_name.strip().lower() == 'unknown':
                            continue
                        
                        person_data = create_person_data(narrator_name)
                        narrator_id = await book_repo._ensure_person_exists(person_data)
                        
                        if narrator_id:
                            narrated_result = safe_execute_kuzu_query(
                                """
                                MATCH (p:Person {id: $narrator_id}), (b:Book {id: $book_id})
                                CREATE (p)-[r:AUTHORED {
                                    role: $role,
                                    order_index: $order_index,
                                    created_at: CASE WHEN $created_at_str IS NULL OR $created_at_str = '' THEN NULL ELSE timestamp($created_at_str) END
                                }]->(b)
                                RETURN r
                                """,
                                {
                                    "narrator_id": narrator_id,
                                    "book_id": book_id,
                                    "role": 'narrated',
                                    "order_index": index,
                                    "created_at_str": datetime.now(timezone.utc).isoformat()
                                }
                            )
                            if narrated_result:
                                print(f"✅ [SIMPLIFIED] Created AUTHORED (narrated) relationship for {narrator_name}")
                            else:
                                print(f"❌ [SIMPLIFIED] Failed to create AUTHORED (narrated) relationship for {narrator_name}")
                        else:
                            print(f"❌ [SIMPLIFIED] Failed to create narrator person: {narrator_name}")
                except Exception as e:
                    print(f"❌ [SIMPLIFIED] Error creating narrator relationship: {e}")
            
            # 2.7. Handle editor if present
            if book_data.editor:
                try:
                    editor_list = [name.strip() for name in book_data.editor.split(',') if name.strip()]
                    for index, editor_name in enumerate(editor_list):
                        
                        person_data = create_person_data(editor_name)
                        editor_id = await book_repo._ensure_person_exists(person_data)
                        
                        if editor_id:
                            edited_result = safe_execute_kuzu_query(
                                """
                                MATCH (p:Person {id: $editor_id}), (b:Book {id: $book_id})
                                CREATE (p)-[r:AUTHORED {
                                    role: $role,
                                    order_index: $order_index,
                                    created_at: CASE WHEN $created_at_str IS NULL OR $created_at_str = '' THEN NULL ELSE timestamp($created_at_str) END
                                }]->(b)
                                RETURN r
                                """,
                                {
                                    "editor_id": editor_id,
                                    "book_id": book_id,
                                    "role": 'edited',
                                    "order_index": index,
                                    "created_at_str": datetime.now(timezone.utc).isoformat()
                                }
                            )
                            if edited_result:
                                print(f"✅ [SIMPLIFIED] Created AUTHORED (edited) relationship for {editor_name}")
                            else:
                                print(f"❌ [SIMPLIFIED] Failed to create AUTHORED (edited) relationship for {editor_name}")
                        else:
                            print(f"❌ [SIMPLIFIED] Failed to create editor person: {editor_name}")
                except Exception as e:
                    print(f"❌ [SIMPLIFIED] Error creating editor relationship: {e}")
            
            # 2.8. Handle translator if present
            if book_data.translator:
                try:
                    translator_list = [name.strip() for name in book_data.translator.split(',') if name.strip()]
                    for index, translator_name in enumerate(translator_list):
                        
                        person_data = create_person_data(translator_name)
                        translator_id = await book_repo._ensure_person_exists(person_data)
                        
                        if translator_id:
                            translated_result = safe_execute_kuzu_query(
                                """
                                MATCH (p:Person {id: $translator_id}), (b:Book {id: $book_id})
                                CREATE (p)-[r:AUTHORED {
                                    role: $role,
                                    order_index: $order_index,
                                    created_at: CASE WHEN $created_at_str IS NULL OR $created_at_str = '' THEN NULL ELSE timestamp($created_at_str) END
                                }]->(b)
                                RETURN r
                                """,
                                {
                                    "translator_id": translator_id,
                                    "book_id": book_id,
                                    "role": 'translated',
                                    "order_index": index,
                                    "created_at_str": datetime.now(timezone.utc).isoformat()
                                }
                            )
                            if translated_result:
                                print(f"✅ [SIMPLIFIED] Created AUTHORED (translated) relationship for {translator_name}")
                            else:
                                print(f"❌ [SIMPLIFIED] Failed to create AUTHORED (translated) relationship for {translator_name}")
                        else:
                            print(f"❌ [SIMPLIFIED] Failed to create translator person: {translator_name}")
                except Exception as e:
                    print(f"❌ [SIMPLIFIED] Error creating translator relationship: {e}")
            
            # 2.9. Handle illustrator if present
            if book_data.illustrator:
                try:
                    illustrator_list = [name.strip() for name in book_data.illustrator.split(',') if name.strip()]
                    for index, illustrator_name in enumerate(illustrator_list):
                        
                        person_data = create_person_data(illustrator_name)
                        illustrator_id = await book_repo._ensure_person_exists(person_data)
                        
                        if illustrator_id:
                            illustrated_result = safe_execute_kuzu_query(
                                """
                                MATCH (p:Person {id: $illustrator_id}), (b:Book {id: $book_id})
                                CREATE (p)-[r:AUTHORED {
                                    role: $role,
                                    order_index: $order_index,
                                    created_at: CASE WHEN $created_at_str IS NULL OR $created_at_str = '' THEN NULL ELSE timestamp($created_at_str) END
                                }]->(b)
                                RETURN r
                                """,
                                {
                                    "illustrator_id": illustrator_id,
                                    "book_id": book_id,
                                    "role": 'illustrated',
                                    "order_index": index,
                                    "created_at_str": datetime.now(timezone.utc).isoformat()
                                }
                            )
                            if illustrated_result:
                                print(f"✅ [SIMPLIFIED] Created AUTHORED (illustrated) relationship for {illustrator_name}")
                            else:
                                print(f"❌ [SIMPLIFIED] Failed to create AUTHORED (illustrated) relationship for {illustrator_name}")
                        else:
                            print(f"❌ [SIMPLIFIED] Failed to create illustrator person: {illustrator_name}")
                except Exception as e:
                    print(f"❌ [SIMPLIFIED] Error creating illustrator relationship: {e}")
            
            # 3. Create publisher relationship using clean repository
            if book_data.publisher:
                try:
                    publisher_id = await book_repo._ensure_publisher_exists(book_data.publisher)
                    if publisher_id:
                        # Create PUBLISHED_BY relationship
                        # Use the helper method for date conversion
                        pub_date = self._convert_to_date(book_data.published_date)
                        
                        published_result = safe_execute_kuzu_query(
                            """
                            MATCH (b:Book {id: $book_id}), (pub:Publisher {id: $publisher_id})
                            CREATE (b)-[r:PUBLISHED_BY {
                                created_at: CASE WHEN $created_at_str IS NULL OR $created_at_str = '' THEN NULL ELSE timestamp($created_at_str) END
                            }]->(pub)
                            RETURN r
                            """,
                            {
                                "book_id": book_id,
                                "publisher_id": publisher_id,
                                "created_at_str": datetime.now(timezone.utc).isoformat()
                            }
                        )
                        if published_result:
                            print(f"✅ [SIMPLIFIED] Created PUBLISHED_BY relationship for {book_data.publisher}")
                        else:
                            print(f"❌ [SIMPLIFIED] Failed to create PUBLISHED_BY relationship for {book_data.publisher}")
                    else:
                        print(f"❌ [SIMPLIFIED] Failed to create publisher: {book_data.publisher}")
                except Exception as e:
                    print(f"❌ [SIMPLIFIED] Error creating publisher relationship: {e}")
            
            # 4. Create category relationships using clean repository
            try:
                # Prefer hierarchical raw_categories if provided (from unified metadata)
                if getattr(book_data, 'raw_categories', None):
                    await book_repo._create_category_relationships_from_raw(book_id, book_data.raw_categories)
                elif book_data.categories:
                    # Fallback to flat categories
                    for category_name in book_data.categories:
                        if not category_name.strip():
                            continue
                        category_id = await book_repo._ensure_category_exists(category_name.strip())
                        if category_id:
                            # Create CATEGORIZED_AS relationship
                            categorized_result = safe_execute_kuzu_query(
                                """
                                MATCH (b:Book {id: $book_id}), (c:Category {id: $category_id})
                                CREATE (b)-[r:CATEGORIZED_AS {
                                    created_at: CASE WHEN $created_at_str IS NULL OR $created_at_str = '' THEN NULL ELSE timestamp($created_at_str) END
                                }]->(c)
                                RETURN r
                                """,
                                {
                                    "book_id": book_id,
                                    "category_id": category_id,
                                    "created_at_str": datetime.now(timezone.utc).isoformat()
                                }
                            )
                            if not categorized_result:
                                print(f"❌ [SIMPLIFIED] Failed to create CATEGORIZED_AS relationship for {category_name}")
                        else:
                            print(f"❌ [SIMPLIFIED] Failed to create category: {category_name}")
            except Exception as e:
                print(f"❌ [SIMPLIFIED] Error creating category relationship(s): {e}")
            
            # 5. Handle global custom metadata (if any)
            if book_data.global_custom_metadata:
                try:
                    print(f"📝 [SIMPLIFIED] Processing {len(book_data.global_custom_metadata)} global custom fields")
                    
                    # Note: For global custom fields, we use a system user ID or the first user
                    # This is a design decision - global fields need an owner for the field definition
                    system_user_id = "system"  # You might want to use a real user ID
                    
                    # Ensure field definitions exist
                    fields_ensured = self.custom_field_service.ensure_custom_fields_exist(
                        system_user_id, book_data.global_custom_metadata, {}
                    )
                    
                    if fields_ensured:
                        # Save global custom metadata to the book
                        global_saved = self.custom_field_service.save_custom_metadata_sync(
                            book_id, system_user_id, book_data.global_custom_metadata
                        )
                        
                        if global_saved:
                            pass  # Custom metadata saved successfully
                        else:
                            pass  # Custom metadata save failed
                    else:
                        pass  # System user not found
                        
                except Exception as e:
                    pass  # Error saving custom metadata
            
            print(f"🎉 [SIMPLIFIED] Book creation completed: {book_id}")
            
            # Use safe checkpoint to ensure data is visible after container restarts
            # This is done AFTER all operations complete to avoid corruption
            # SafeKuzuManager handles persistence automatically
            
            return book_id
            
        except Exception as e:
            return None
    
    def create_user_annotation(self, annotation: UserBookAnnotation) -> bool:
        """
        Create user annotation relationship - completely separate from book creation.
        In universal library: Books are shared, but users can have personal annotations.
        Returns True if successful, False if failed.
        """
        try:
            # Ensure date_added is set
            if annotation.date_added is None:
                annotation.date_added = datetime.now(timezone.utc)
            
            # In universal library mode, create a USER_ANNOTATES relationship instead of OWNS
            # This preserves user's personal data without implying ownership
            date_added_str = annotation.date_added.isoformat()
            
            result = safe_execute_kuzu_query(
                """
                MATCH (u:User {id: $user_id}), (b:Book {id: $book_id})
                CREATE (u)-[r:USER_ANNOTATES {
                    reading_status: $reading_status,
                    date_added: CASE WHEN $date_added_str IS NULL OR $date_added_str = '' THEN NULL ELSE timestamp($date_added_str) END,
                    user_rating: $user_rating,
                    personal_notes: $personal_notes,
                    custom_metadata: $custom_metadata
                }]->(b)
                RETURN r
                """,
                {
                    "user_id": annotation.user_id,
                    "book_id": annotation.book_id,
                    "reading_status": annotation.reading_status,
                    "date_added_str": date_added_str,
                    "user_rating": annotation.user_rating,
                    "personal_notes": annotation.personal_notes,
                    "custom_metadata": json.dumps(annotation.custom_metadata) if annotation.custom_metadata else None
                }
            )
            
            if result:
                # Handle personal custom metadata through custom field service
                if annotation.custom_metadata:
                    try:
                        print(f"📝 [ANNOTATION] Processing {len(annotation.custom_metadata)} personal custom fields")
                        
                        # Ensure field definitions exist
                        fields_ensured = self.custom_field_service.ensure_custom_fields_exist(
                            annotation.user_id, {}, annotation.custom_metadata
                        )
                        
                        if fields_ensured:
                            # Save personal custom metadata
                            personal_saved = self.custom_field_service.save_custom_metadata_sync(
                                annotation.book_id, annotation.user_id, annotation.custom_metadata
                            )
                            
                            if not personal_saved:
                                print(f"⚠️ [ANNOTATION] Failed to save personal custom metadata")
                        else:
                            print(f"⚠️ [ANNOTATION] Failed to ensure custom field definitions exist")
                            
                    except Exception as e:
                        print(f"❌ [ANNOTATION] Error saving personal custom metadata: {e}")
                
                return True
            else:
                return False
                
        except Exception as e:
            print(f"Error creating user annotation: {e}")
            return False
    
    def find_book_by_isbn(self, isbn: str) -> Optional[str]:
        """Find existing book by ISBN. Returns book_id if found."""
        try:
            # Normalize ISBN: keep digits and the letter X (uppercase)
            normalized_isbn = ''.join(c for c in str(isbn).strip().upper() if c.isdigit() or c == 'X')
            
            # Search by ISBN13
            if len(normalized_isbn) == 13:
                query = "MATCH (b:Book {isbn13: $isbn}) RETURN b.id"
                result = safe_execute_kuzu_query(query, {"isbn": normalized_isbn})
                result_list = self._convert_query_result_to_list(result)
                if result_list:
                    return result_list[0]['col_0']
            
            # Search by ISBN10  
            elif len(normalized_isbn) == 10:
                query = "MATCH (b:Book {isbn10: $isbn}) RETURN b.id"
                result = safe_execute_kuzu_query(query, {"isbn": normalized_isbn})
                result_list = self._convert_query_result_to_list(result)
                if result_list:
                    return result_list[0]['col_0']
            
            return None
            
        except Exception as e:
            return None

    def find_book_by_title_author(self, title: str, author: str) -> Optional[str]:
        """Find existing book by exact title and author match. Returns book_id if found."""
        try:
            # Normalize title and author for comparison
            normalized_title = title.lower().strip()
            normalized_author = author.lower().strip()
            
            # Find books with exact title match and check for matching author
            # This prevents false positives where similar titles (e.g., "Die Henkerstochter" 
            # vs "Die Henkerstochter und der schwarze Mönch") are incorrectly detected as duplicates
            title_query = """
            MATCH (b:Book)
            WHERE toLower(b.title) = $title
            RETURN b.id, b.title
            """
            
            title_results = safe_execute_kuzu_query(title_query, {"title": normalized_title})
            title_results_list = self._convert_query_result_to_list(title_results)
            
            if title_results_list:
                # If we found books with matching titles, check if any have matching authors
                for result in title_results_list:
                    book_id = result['col_0']
                    
                    # Check if this book has the author we're looking for
                    # Use CONTAINS for author matching to handle variations in author names
                    author_query = """
                    MATCH (b:Book {id: $book_id})<-[:AUTHORED {role: 'authored'}]-(p:Person)
                    WHERE toLower(p.name) CONTAINS $author OR $author CONTAINS toLower(p.name)
                    RETURN p.name
                    LIMIT 1
                    """
                    
                    author_results = safe_execute_kuzu_query(author_query, {
                        "book_id": book_id,
                        "author": normalized_author
                    })
                    author_results_list = self._convert_query_result_to_list(author_results)
                    
                    if author_results_list:
                        print(f"🔍 [DUPLICATE] Found matching book: '{title}' by author containing '{author}'")
                        return book_id
            
            # No exact match found - book is unique
            return None
            
        except Exception as e:
            print(f"Error finding book by title/author: {e}")
            return None
    
    async def find_or_create_book(self, book_data: SimplifiedBook) -> Optional[str]:
        """
        Find existing book or create new one.
        Returns book_id if successful.
        Raises BookAlreadyExistsError if duplicate is found.
        """
        try:
            # Try to find existing book by ISBN first (most reliable)
            existing_id = None
            
            if book_data.isbn13:
                existing_id = self.find_book_by_isbn(book_data.isbn13)
                if existing_id:
                    print(f"🔍 [DUPLICATE] Found existing book by ISBN13: {book_data.isbn13}")
                    raise BookAlreadyExistsError(existing_id, f"Book already exists with ISBN13: {book_data.isbn13}")
            
            if book_data.isbn10 and not existing_id:
                existing_id = self.find_book_by_isbn(book_data.isbn10)
                if existing_id:
                    print(f"🔍 [DUPLICATE] Found existing book by ISBN10: {book_data.isbn10}")
                    raise BookAlreadyExistsError(existing_id, f"Book already exists with ISBN10: {book_data.isbn10}")
            
            # If no ISBN or ISBN not found, check by title and author
            if not existing_id and book_data.title and book_data.author:
                existing_id = self.find_book_by_title_author(book_data.title, book_data.author)
                if existing_id:
                    print(f"🔍 [DUPLICATE] Found existing book by title/author: '{book_data.title}' by '{book_data.author}'")
                    raise BookAlreadyExistsError(existing_id, f"Book already exists: '{book_data.title}' by '{book_data.author}'")
            
            # Book doesn't exist, create new one
            return await self.create_standalone_book(book_data)
            
        except BookAlreadyExistsError:
            # Re-raise duplicate error
            raise
        except Exception as e:
            print(f"Error in find_or_create_book: {e}")
            return None
    
    def build_book_data_from_row(self, row, mappings, book_meta_map=None, author_meta_map=None):
        """
        Build SimplifiedBook data from a CSV row with field mappings.
        
        Args:
            row: Dict of CSV row data
            mappings: Field mapping dict (CSV field -> book field)
            book_meta_map: Optional dict of ISBN -> book metadata from APIs
            author_meta_map: Optional dict of author name -> author metadata from APIs
            
        Returns:
            SimplifiedBook instance
        """
        # Building book from row data (reduced logging)
        
        # Initialize book data
        book_data = {
            'title': '',
            'author': '',
            'isbn13': None,
            'isbn10': None,
            'asin': None,
            'subtitle': None,
            'description': None,
            'publisher': None,
            'published_date': None,
            'page_count': None,
            'language': 'en',
            'cover_url': None,
            'series': None,
            'series_volume': None,
            'series_order': None,
            'categories': [],
            'google_books_id': None,
            'openlibrary_id': None,
            'average_rating': None,
            'rating_count': None,
            # Additional person type fields
            'additional_authors': None,
            'narrator': None,
            'editor': None,
            'translator': None,
            'illustrator': None,
            # Custom metadata fields
            'global_custom_metadata': {},
            'personal_custom_metadata': {}
        }
        
        # Map CSV fields to book fields
        for csv_field, book_field in mappings.items():
            if csv_field in row and row[csv_field]:
                value = row[csv_field].strip() if isinstance(row[csv_field], str) else row[csv_field]
                
                if book_field == 'title':
                    book_data['title'] = value
                elif book_field == 'author':
                    # Support StoryGraph "Authors" where multiple names are comma-separated
                    if isinstance(value, str) and (',' in value):
                        parts = [p.strip() for p in value.split(',') if p.strip()]
                        if parts:
                            book_data['author'] = parts[0]
                            if len(parts) > 1:
                                book_data['additional_authors'] = ', '.join(parts[1:])
                    else:
                        book_data['author'] = value
                elif book_field == 'contributors':
                    # Parse entries like "Jane Doe (translator); John Smith (editor)" or comma/pipe-separated
                    text = value if isinstance(value, str) else str(value)
                    # Prefer semicolon or pipe as record separator; fallback to comma-space
                    if ';' in text:
                        parts = [p.strip() for p in text.split(';') if p.strip()]
                    elif '|' in text:
                        parts = [p.strip() for p in text.split('|') if p.strip()]
                    elif ', ' in text:
                        parts = [p.strip() for p in text.split(', ') if p.strip()]
                    else:
                        parts = [text.strip()] if text.strip() else []

                    def push_unique(lst, name):
                        n = (name or '').strip()
                        if n and n not in lst:
                            lst.append(n)

                    addl, editors, translators, narrators, illustrators = [], [], [], [], []
                    for p in parts:
                        name = p
                        role = None
                        if '(' in p and ')' in p and p.index('(') < p.index(')'):
                            i = p.index('(')
                            j = p.index(')', i+1)
                            role_text = p[i+1:j].strip().lower()
                            name = (p[:i] + p[j+1:]).strip().strip(',')
                            if 'editor' in role_text:
                                role = 'editor'
                            elif 'translator' in role_text:
                                role = 'translator'
                            elif 'narrator' in role_text:
                                role = 'narrator'
                            elif 'illustrator' in role_text or 'illustration' in role_text:
                                role = 'illustrator'
                            elif 'author' in role_text:
                                role = 'author'

                        name = name.strip()
                        if role == 'editor':
                            push_unique(editors, name)
                        elif role == 'translator':
                            push_unique(translators, name)
                        elif role == 'narrator':
                            push_unique(narrators, name)
                        elif role == 'illustrator':
                            push_unique(illustrators, name)
                        elif role == 'author':
                            # if primary differs, treat as additional author
                            if not book_data.get('author') or name.lower() != str(book_data['author']).lower():
                                push_unique(addl, name)
                        else:
                            # Unknown role → additional author if not primary
                            if not book_data.get('author') or name.lower() != str(book_data['author']).lower():
                                push_unique(addl, name)

                    def merge(field, incoming):
                        if not incoming:
                            return
                        existing = book_data.get(field)
                        if existing:
                            current = [s.strip() for s in str(existing).split(',') if s.strip()]
                            for n in incoming:
                                if n not in current:
                                    current.append(n)
                            book_data[field] = ', '.join(current)
                        else:
                            book_data[field] = ', '.join(incoming)

                    merge('additional_authors', addl)
                    merge('editor', editors)
                    merge('translator', translators)
                    merge('narrator', narrators)
                    merge('illustrator', illustrators)
                elif book_field == 'isbn':
                    # Clean and assign ISBN (preserve uppercase X for check digit)
                    clean_isbn = ''.join(c for c in str(value).strip().upper() if c.isdigit() or c == 'X')
                    if len(clean_isbn) == 13:
                        book_data['isbn13'] = clean_isbn
                    elif len(clean_isbn) == 10:
                        book_data['isbn10'] = clean_isbn
                elif book_field == 'isbn13':
                    # Normalize Goodreads format and clean ISBN13
                    normalized_isbn = normalize_goodreads_value(value, 'isbn')
                    if normalized_isbn:
                        clean_isbn = ''.join(c for c in str(normalized_isbn).strip().upper() if c.isdigit() or c == 'X')
                        if len(clean_isbn) == 13:
                            book_data['isbn13'] = clean_isbn
                        elif len(clean_isbn) == 10:
                            book_data['isbn10'] = clean_isbn
                elif book_field == 'isbn10':
                    # Normalize Goodreads format and clean ISBN10
                    normalized_isbn = normalize_goodreads_value(value, 'isbn')
                    if normalized_isbn:
                        clean_isbn = ''.join(c for c in str(normalized_isbn).strip().upper() if c.isdigit() or c == 'X')
                        if len(clean_isbn) == 10:
                            book_data['isbn10'] = clean_isbn
                        elif len(clean_isbn) == 13:
                            book_data['isbn13'] = clean_isbn
                elif book_field == 'asin':
                    # Normalize ASIN (alphanumeric, often 10 chars)
                    if value:
                        s = str(value).strip()
                        # Remove wrappers like ="..."
                        if s.startswith('="') and s.endswith('"') and len(s) >= 3:
                            s = s[2:-1].strip()
                        # Strip quotes
                        if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
                            s = s[1:-1].strip()
                        book_data['asin'] = s or None
                elif book_field == 'publisher':
                    # Normalize publisher consistently from CSV
                    book_data['publisher'] = _normalize_publisher_name(value)
                elif book_field == 'page_count':
                    try:
                        book_data['page_count'] = int(value) if value else None
                    except (ValueError, TypeError):
                        pass
                elif book_field == 'publication_year':
                    try:
                        year = int(value) if value else None
                        if year:
                            book_data['published_date'] = f"{year}-01-01"
                    except (ValueError, TypeError):
                        pass
                elif book_field == 'published_date':
                    # Handle full publication dates - preserve them as-is if valid
                    if value and value.strip():
                        book_data['published_date'] = value.strip()
                        print(f"📅 [CSV] Set published_date from CSV: '{value.strip()}'")
                elif book_field == 'additional_authors':
                    book_data['additional_authors'] = value
                elif book_field == 'additional_author':  # Handle singular form too
                    book_data['additional_authors'] = value
                elif book_field == 'narrator':
                    book_data['narrator'] = value
                elif book_field == 'editor':
                    book_data['editor'] = value
                elif book_field == 'translator':
                    book_data['translator'] = value
                elif book_field == 'illustrator':
                    book_data['illustrator'] = value
                elif book_field.startswith('custom_'):
                    # Handle custom fields by parsing prefix and storing in metadata
                    if book_field.startswith('custom_global_'):
                        field_name = book_field[14:]  # Remove 'custom_global_' prefix
                        book_data['global_custom_metadata'][field_name] = value
                    elif book_field.startswith('custom_personal_'):
                        field_name = book_field[16:]  # Remove 'custom_personal_' prefix
                        book_data['personal_custom_metadata'][field_name] = value
                    else:
                        # Default to global if just 'custom_'
                        field_name = book_field[7:]  # Remove 'custom_' prefix
                        book_data['global_custom_metadata'][field_name] = value
                elif book_field == 'average_rating':
                    # Convert average rating to float
                    try:
                        book_data['average_rating'] = float(value) if value else None
                    except (ValueError, TypeError):
                        book_data['average_rating'] = None
                elif book_field == 'rating_count':
                    # Convert rating count to int
                    try:
                        book_data['rating_count'] = int(value) if value else None
                    except (ValueError, TypeError):
                        book_data['rating_count'] = None
                elif book_field == 'categories':
                    # Handle categories field - ensure it's always a list
                    if value:
                        if isinstance(value, list):
                            book_data['categories'] = value
                        elif isinstance(value, str):
                            # Split comma-separated categories and clean them
                            book_data['categories'] = [cat.strip() for cat in value.split(',') if cat.strip()]
                        else:
                            book_data['categories'] = [str(value)]
                    else:
                        book_data['categories'] = []
                elif book_field not in ['ignore', 'reading_status', 'rating', 'finish_date', 'personal_notes']:
                    # Map other standard fields
                    if book_field in book_data:
                        book_data[book_field] = value
        
        # Enhance with API metadata if available
        isbn_for_lookup = book_data['isbn13'] or book_data['isbn10']
        if isbn_for_lookup and book_meta_map and isbn_for_lookup in book_meta_map:
            api_data = book_meta_map[isbn_for_lookup]
            # Merge API data, preferring API values for richer metadata
            for field, api_value in api_data.items():
                if api_value and field in book_data:
                    book_data[field] = api_value
        
        # Create SimplifiedBook instance (reduced logging)
        simplified_book = SimplifiedBook(**book_data)
        
        return simplified_book
    
    async def add_book_to_user_library(self, book_data: SimplifiedBook, user_id: str, 
                                reading_status: str = "",
                                ownership_status: str = "owned",
                                media_type: Optional[str] = None,
                                user_rating: Optional[float] = None,
                                personal_notes: Optional[str] = None,
                                location_id: Optional[str] = None,
                                custom_metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Complete workflow: Find/create book + create user ownership.
        This is the main entry point for the simplified architecture.
        Raises BookAlreadyExistsError if book already exists in communal library.
        """
        try:
            # Validate and set media_type on book_data BEFORE creating the book
            if not media_type:
                media_type = get_default_book_format()
            else:
                try:
                    media_type = MediaType(media_type).value
                except Exception:
                    media_type = get_default_book_format()
            
            # Set media_type on the book_data so it gets saved to the Book node
            book_data.media_type = media_type

            # Step 1: Find or create standalone book
            # This will raise BookAlreadyExistsError if duplicate is found
            book_id = await self.find_or_create_book(book_data)
            if not book_id:
                return False
            
            # If we get here, it's a new book, so we can proceed with the rest
            # Note: In a communal library, we might not need user ownership relationships
            # but keeping this for compatibility with the current system
            
            # Step 2: Universal library model - no user ownership relationships needed
            # Books are shared and stored at locations, not owned by users
            print(f"📚 [UNIVERSAL_LIBRARY] Book {book_id} created in universal library (no ownership)")

            # Step 3: Handle location assignment (NEW)
            if location_id:
                try:
                    from app.location_service import LocationService
                    from app.utils.safe_kuzu_manager import safe_get_connection
                    
                    location_service = LocationService()
                    location_success = location_service.add_book_to_location(book_id, location_id, user_id)
                    if location_success:
                        print(f"✅ [LOCATION_SERVICE] Successfully added book {book_id} to location {location_id}")
                    else:
                        print(f"❌ [LOCATION_SERVICE] Failed to add book {book_id} to location {location_id}")
                            
                except Exception as e:
                    print(f"❌ [LOCATION_SERVICE] Exception adding book to location: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                try:
                    from app.location_service import LocationService
                    from app.utils.safe_kuzu_manager import safe_get_connection
                    
                    location_service = LocationService()
                    
                    # Get or create default location (universal)
                    default_location = location_service.get_default_location()
                    if not default_location:
                        default_locations = location_service.setup_default_locations()
                        if default_locations:
                            default_location = default_locations[0]
                    
                    if default_location and default_location.id:
                        location_success = location_service.add_book_to_location(book_id, default_location.id, user_id)
                        if location_success:
                            print(f"✅ [DEFAULT_LOCATION] Successfully added book {book_id} to default location {default_location.id}")
                        else:
                            print(f"❌ [DEFAULT_LOCATION] Failed to add book {book_id} to default location")
                    else:
                        print(f"❌ [DEFAULT_LOCATION] No default location available for book {book_id}")
                            
                except Exception as e:
                    print(f"❌ [DEFAULT_LOCATION] Exception assigning book to default location: {e}")
            
            # Step 4: Persist personal metadata (standard fields + custom) AFTER book creation & location assignment
            try:
                from app.services.personal_metadata_service import personal_metadata_service
                from datetime import datetime as dt
                
                # Build custom_updates dict with all personal fields
                custom_updates = {}
                
                # Add standard personal fields (NOT media_type - that's Book metadata, not personal)
                if reading_status:
                    custom_updates['reading_status'] = reading_status
                if ownership_status:
                    custom_updates['ownership_status'] = ownership_status
                if user_rating is not None:
                    custom_updates['user_rating'] = user_rating
                
                # Add any custom metadata from book_data
                if getattr(book_data, 'personal_custom_metadata', None):
                    pcm = book_data.personal_custom_metadata
                    if pcm:
                        print(f"📝 [UNIVERSAL_LIBRARY] Including {len(pcm)} personal custom fields")
                        custom_updates.update(pcm)
                
                # Add custom_metadata parameter if provided (but extract dates separately)
                start_date_value = None
                finish_date_value = None
                if custom_metadata:
                    # Extract start_date and finish_date for separate parameters
                    if 'start_date' in custom_metadata:
                        sd_raw = custom_metadata.pop('start_date')
                        if sd_raw:
                            try:
                                if isinstance(sd_raw, dt):
                                    start_date_value = sd_raw
                                elif isinstance(sd_raw, str):
                                    start_date_value = dt.fromisoformat(sd_raw.replace('Z', '+00:00'))
                            except Exception:
                                pass
                    if 'finish_date' in custom_metadata:
                        fd_raw = custom_metadata.pop('finish_date')
                        if fd_raw:
                            try:
                                if isinstance(fd_raw, dt):
                                    finish_date_value = fd_raw
                                elif isinstance(fd_raw, str):
                                    finish_date_value = dt.fromisoformat(fd_raw.replace('Z', '+00:00'))
                            except Exception:
                                pass
                    # Merge remaining custom fields
                    custom_updates.update(custom_metadata)
                
                # Save all personal metadata (media_type is already on Book node)
                if custom_updates or personal_notes or start_date_value or finish_date_value:
                    print(f"📝 [UNIVERSAL_LIBRARY] Saving personal metadata for user {user_id}")
                    personal_metadata_service.update_personal_metadata(
                        user_id=user_id,
                        book_id=book_id,
                        personal_notes=personal_notes,
                        start_date=start_date_value,
                        finish_date=finish_date_value,
                        custom_updates=custom_updates,
                        merge=False  # Don't merge, this is a new book
                    )
                    print(f"✅ [UNIVERSAL_LIBRARY] Personal metadata saved successfully")
                    
            except Exception as e:
                print(f"❌ [UNIVERSAL_LIBRARY] Error saving personal metadata: {e}")
                import traceback
                traceback.print_exc()
            
            return True
            
        except BookAlreadyExistsError:
            # Re-raise the duplicate error so it can be handled by the calling code
            raise
        except Exception as e:
            print(f"Error in add_book_to_user_library: {e}")
            return False
    
    def add_book_to_user_library_sync(self, book_data: SimplifiedBook, user_id: str, 
                                     reading_status: str = "",
                                     ownership_status: str = "owned",
                                     media_type: Optional[str] = None,
                                     user_rating: Optional[float] = None,
                                     personal_notes: Optional[str] = None,
                                     location_id: Optional[str] = None,
                                     custom_metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Synchronous wrapper for add_book_to_user_library.
        Use this method from Flask routes and other sync contexts.
        Raises BookAlreadyExistsError if book already exists in communal library.
        """
        import asyncio
        
        # Create coroutine
        coro = self.add_book_to_user_library(
            book_data=book_data,
            user_id=user_id,
            reading_status=reading_status,
            ownership_status=ownership_status,
            media_type=media_type,
            user_rating=user_rating,
            personal_notes=personal_notes,
            location_id=location_id,
            custom_metadata=custom_metadata
        )
        
        # Run the coroutine synchronously
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        if loop.is_running():
            # If we're already in an async context, create a new thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                def run_in_new_loop():
                    new_loop = asyncio.new_event_loop()
                    try:
                        return new_loop.run_until_complete(coro)
                    finally:
                        new_loop.close()
                future = executor.submit(run_in_new_loop)
                return future.result()
        else:
            return loop.run_until_complete(coro)
    
    def create_standalone_book_sync(self, book_data: SimplifiedBook) -> Optional[str]:
        """
        Synchronous wrapper for create_standalone_book.
        Use this method from Flask routes and other sync contexts.
        Returns book_id if successful, None if failed.
        """
        import asyncio
        
        # Create coroutine
        coro = self.create_standalone_book(book_data)
        
        # Run the coroutine synchronously
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        if loop.is_running():
            # If we're already in an async context, create a new thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                def run_in_new_loop():
                    new_loop = asyncio.new_event_loop()
                    try:
                        return new_loop.run_until_complete(coro)
                    finally:
                        new_loop.close()
                future = executor.submit(run_in_new_loop)
                return future.result()
        else:
            return loop.run_until_complete(coro)


# Convenience function for current routes
def create_simplified_book_service():
    """Factory function to create simplified book service."""
    return SimplifiedBookService()
