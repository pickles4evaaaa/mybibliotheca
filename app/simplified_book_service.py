"""
Simplified Book Service - Decoupled Architecture
Separates book creation from user relationships for better persistence.
"""

import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field

from .domain.models import Book, Person, Publisher, Series, Category, BookContribution, ContributionType
from .infrastructure.kuzu_graph import get_graph_storage
from .services.kuzu_custom_field_service import KuzuCustomFieldService


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
    google_books_id: Optional[str] = None
    openlibrary_id: Optional[str] = None
    average_rating: Optional[float] = None
    rating_count: Optional[int] = None
    
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
class UserBookOwnership:
    """Simplified ownership relationship - just the essentials."""
    user_id: str
    book_id: str
    reading_status: str = "plan_to_read"
    ownership_status: str = "owned" 
    media_type: str = "physical"
    date_added: Optional[datetime] = None
    user_rating: Optional[float] = None
    personal_notes: Optional[str] = None
    location_id: Optional[str] = None
    custom_metadata: Optional[Dict[str, Any]] = None


class SimplifiedBookService:
    """
    Simplified book service with clean separation:
    1. Create books as standalone global entities
    2. Create user relationships separately
    """
    
    def __init__(self):
        self.storage = get_graph_storage()
        self.custom_field_service = KuzuCustomFieldService()
        # Log the database instance ID to verify single instance usage
        print(f"🔥 [SIMPLIFIED_SERVICE] Using KuzuDB instance: {id(self.storage.connection)}")
    
    async def create_standalone_book(self, book_data: SimplifiedBook) -> Optional[str]:
        """
        Create a book as a standalone global entity.
        Returns book_id if successful, None if failed.
        """
        try:
            book_id = str(uuid.uuid4())
            
            # Debug: Print all contributor data
            print(f"🎯 [SIMPLIFIED] Creating standalone book with contributors:")
            print(f"   Primary author: '{book_data.author}'")
            print(f"   Additional authors: '{book_data.additional_authors}'")
            print(f"   Editor: '{book_data.editor}'")
            print(f"   Translator: '{book_data.translator}'")
            print(f"   Narrator: '{book_data.narrator}'")
            print(f"   Illustrator: '{book_data.illustrator}'")
            
            # Prepare core book node data
            book_node_data = {
                'id': book_id,
                'title': book_data.title,
                'normalized_title': book_data.title.lower(),
                'subtitle': book_data.subtitle,
                'description': book_data.description,
                'published_date': book_data.published_date,
                'page_count': book_data.page_count,
                'language': book_data.language,
                # Only store cover_url (the schema field that exists)
                'cover_url': book_data.cover_url,
                # Store both ISBN formats 
                'isbn13': book_data.isbn13,
                'isbn10': book_data.isbn10,
                'google_books_id': book_data.google_books_id,
                'openlibrary_id': book_data.openlibrary_id,
                'average_rating': book_data.average_rating,
                'rating_count': book_data.rating_count,
                'series': book_data.series,
                'series_volume': book_data.series_volume,
                'series_order': book_data.series_order,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }
            
            # Remove None values
            book_node_data = {k: v for k, v in book_node_data.items() if v is not None}
            
            # Enhanced debugging for ISBN fields
            print(f"   ISBN13: {book_node_data.get('isbn13')}")
            print(f"   ISBN10: {book_node_data.get('isbn10')}")
            print(f"   Cover URL: {book_node_data.get('cover_url')}")
            print(f"   Title: {book_node_data.get('title')}")
            description = book_node_data.get('description')
            print(f"   Description: {description[:50] + '...' if description else 'None'}")
            
            # 1. Create book node (SINGLE TRANSACTION)
            book_success = self.storage.store_node('Book', book_id, book_node_data)
            if not book_success:
                return None
            
            
            # Initialize book repository for relationship creation
            from .infrastructure.kuzu_repositories import KuzuBookRepository
            book_repo = KuzuBookRepository()
            
            # Helper function to create person data objects
            def create_person_data(name):
                class PersonData:
                    def __init__(self, name):
                        self.id = str(uuid.uuid4())
                        self.name = name
                        self.birth_year = None
                        self.death_year = None
                        self.bio = ""
                        self.openlibrary_id = None
                        self.image_url = None
                        self.birth_place = None
                        self.website = None
                        self.created_at = datetime.utcnow()
                return PersonData(name)
            
            # 2. Create author relationship using clean repository (with auto-fetch)
            if book_data.author:
                try:
                    
                    person_data = create_person_data(book_data.author)
                    
                    # Use the book repository's _ensure_person_exists method
                    author_id = await book_repo._ensure_person_exists(person_data)
                    if author_id:
                        
                        # Create AUTHORED relationship
                        authored_success = self.storage.create_relationship(
                            'Person', author_id, 'AUTHORED', 'Book', book_id,
                            {
                                'role': 'authored',
                                'order_index': 0,
                                'created_at': datetime.utcnow()
                            }
                        )
                        if authored_success:
                            pass  # Author relationship created successfully
                        else:
                            pass  # Author relationship creation failed
                    else:
                        pass  # Author creation failed
                except Exception as e:
                    import traceback
                    traceback.print_exc()
            
            # 2.5. Handle additional authors if present
            if book_data.additional_authors:
                try:
                    additional_authors_list = [name.strip() for name in book_data.additional_authors.split(',') if name.strip()]
                    for index, author_name in enumerate(additional_authors_list):
                        
                        person_data = create_person_data(author_name)
                        author_id = await book_repo._ensure_person_exists(person_data)
                        
                        if author_id:
                            authored_success = self.storage.create_relationship(
                                'Person', author_id, 'AUTHORED', 'Book', book_id,
                                {
                                    'role': 'authored',
                                    'order_index': index + 1,
                                    'created_at': datetime.utcnow()
                                }
                            )
                            if authored_success:
                                pass  # Author relationship created successfully
                            else:
                                pass  # Author relationship creation failed
                        else:
                            pass  # Author creation failed
                    else:
                        pass  # Author creation failed
                except Exception as e:
                    pass  # Error creating author relationship
            
            # 2.6. Handle narrator if present
            if book_data.narrator:
                try:
                    narrator_list = [name.strip() for name in book_data.narrator.split(',') if name.strip()]
                    for index, narrator_name in enumerate(narrator_list):
                        
                        person_data = create_person_data(narrator_name)
                        narrator_id = await book_repo._ensure_person_exists(person_data)
                        
                        if narrator_id:
                            narrated_success = self.storage.create_relationship(
                                'Person', narrator_id, 'NARRATED', 'Book', book_id,
                                {
                                    'role': 'narrated',
                                    'order_index': index,
                                    'created_at': datetime.utcnow()
                                }
                            )
                            if narrated_success:
                                pass  # Narrator relationship created successfully
                            else:
                                pass  # Narrator relationship creation failed
                        else:
                            pass  # Narrator creation failed
                    else:
                        pass  # Narrator creation failed
                except Exception as e:
                    pass  # Error creating narrator relationship
            
            # 2.7. Handle editor if present
            if book_data.editor:
                try:
                    editor_list = [name.strip() for name in book_data.editor.split(',') if name.strip()]
                    for index, editor_name in enumerate(editor_list):
                        
                        person_data = create_person_data(editor_name)
                        editor_id = await book_repo._ensure_person_exists(person_data)
                        
                        if editor_id:
                            edited_success = self.storage.create_relationship(
                                'Person', editor_id, 'EDITED', 'Book', book_id,
                                {
                                    'role': 'edited',
                                    'order_index': index,
                                    'created_at': datetime.utcnow()
                                }
                            )
                            if edited_success:
                                print(f"✅ [SIMPLIFIED] Created EDITED relationship for {editor_name}")
                            else:
                                print(f"❌ [SIMPLIFIED] Failed to create EDITED relationship for {editor_name}")
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
                            translated_success = self.storage.create_relationship(
                                'Person', translator_id, 'TRANSLATED', 'Book', book_id,
                                {
                                    'role': 'translated',
                                    'order_index': index,
                                    'created_at': datetime.utcnow()
                                }
                            )
                            if translated_success:
                                print(f"✅ [SIMPLIFIED] Created TRANSLATED relationship for {translator_name}")
                            else:
                                print(f"❌ [SIMPLIFIED] Failed to create TRANSLATED relationship for {translator_name}")
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
                            illustrated_success = self.storage.create_relationship(
                                'Person', illustrator_id, 'ILLUSTRATED', 'Book', book_id,
                                {
                                    'role': 'illustrated',
                                    'order_index': index,
                                    'created_at': datetime.utcnow()
                                }
                            )
                            if illustrated_success:
                                print(f"✅ [SIMPLIFIED] Created ILLUSTRATED relationship for {illustrator_name}")
                            else:
                                print(f"❌ [SIMPLIFIED] Failed to create ILLUSTRATED relationship for {illustrator_name}")
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
                        # Enhanced publication_date conversion with comprehensive format support
                        pub_date = None
                        if book_data.published_date:
                            print(f"📅 [BOOK_SERVICE] Converting published_date: '{book_data.published_date}' (type: {type(book_data.published_date)})")
                            if isinstance(book_data.published_date, str):
                                try:
                                    # Enhanced date parsing - handle common formats from APIs
                                    date_str = book_data.published_date.strip()
                                    formats = [
                                        '%Y-%m-%d',    # 2023-12-25
                                        '%Y/%m/%d',    # 2023/12/25
                                        '%m/%d/%Y',    # 12/25/2023
                                        '%d/%m/%Y',    # 25/12/2023
                                        '%Y-%m',       # 2023-12
                                        '%Y/%m',       # 2023/12
                                        '%m/%Y',       # 12/2023
                                        '%Y',          # 2023
                                        '%B %d, %Y',   # December 25, 2023
                                        '%b %d, %Y',   # Dec 25, 2023
                                        '%d %B %Y',    # 25 December 2023
                                        '%d %b %Y',    # 25 Dec 2023
                                    ]
                                    
                                    for fmt in formats:
                                        try:
                                            pub_date = datetime.strptime(date_str, fmt).date()
                                            print(f"✅ [BOOK_SERVICE] Successfully parsed '{date_str}' using format '{fmt}' -> {pub_date}")
                                            break
                                        except ValueError:
                                            continue
                                    
                                    if not pub_date:
                                        # Try extracting just the year if other formats fail
                                        import re
                                        year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
                                        if year_match:
                                            year = int(year_match.group())
                                            pub_date = datetime(year, 1, 1).date()
                                            print(f"✅ [BOOK_SERVICE] Extracted year {year} from '{date_str}' -> {pub_date}")
                                        else:
                                            print(f"❌ [BOOK_SERVICE] Could not parse any date from '{date_str}'")
                                        
                                except Exception as e:
                                    print(f"⚠️ [BOOK_SERVICE] Failed to parse published_date '{book_data.published_date}': {e}")
                                    pub_date = None
                            else:
                                # Already a date object
                                pub_date = book_data.published_date
                                print(f"✅ [BOOK_SERVICE] Using existing date object: {pub_date}")
                        else:
                            print(f"📅 [BOOK_SERVICE] No published_date provided")
                        
                        published_success = self.storage.create_relationship(
                            'Book', book_id, 'PUBLISHED_BY', 'Publisher', publisher_id,
                            {
                                'publication_date': pub_date,
                                'created_at': datetime.utcnow()
                            }
                        )
                        if published_success:
                            pass  # Publisher relationship created successfully
                        else:
                            pass  # Publisher relationship creation failed
                    else:
                        pass  # Publisher creation failed
                except Exception as e:
                    pass  # Error creating publisher relationship
            
            # 4. Create category relationships using clean repository
            if book_data.categories:
                try:
                    for category_name in book_data.categories:
                        if not category_name.strip():
                            continue
                            
                        category_id = await book_repo._ensure_category_exists(category_name.strip())
                        if category_id:
                            # Create CATEGORIZED_AS relationship
                            categorized_success = self.storage.create_relationship(
                                'Book', book_id, 'CATEGORIZED_AS', 'Category', category_id,
                                {
                                    'created_at': datetime.utcnow()
                                }
                            )
                            if categorized_success:
                                pass  # Category relationship created successfully
                            else:
                                pass  # Category relationship creation failed
                        else:
                            pass  # Category creation failed
                except Exception as e:
                    pass  # Error creating category relationship
            
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
            self.storage.safe_checkpoint()
            
            return book_id
            
        except Exception as e:
            return None
    
    def create_user_ownership(self, ownership: UserBookOwnership) -> bool:
        """
        Create user ownership relationship - completely separate from book creation.
        Returns True if successful, False if failed.
        """
        try:
            
            # Ensure date_added is set
            if ownership.date_added is None:
                ownership.date_added = datetime.utcnow()
            
            # Prepare ownership relationship data
            ownership_data = {
                'reading_status': ownership.reading_status,
                'ownership_status': ownership.ownership_status,
                'media_type': ownership.media_type,
                'date_added': ownership.date_added,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }
            
            # Add optional fields if present
            if ownership.user_rating is not None:
                ownership_data['user_rating'] = ownership.user_rating
            if ownership.personal_notes:
                ownership_data['personal_notes'] = ownership.personal_notes
            if ownership.location_id:
                ownership_data['location_id'] = ownership.location_id
            if ownership.custom_metadata:
                import json
                ownership_data['custom_metadata'] = json.dumps(ownership.custom_metadata)
            
            # Create OWNS relationship (SINGLE TRANSACTION)
            success = self.storage.create_relationship(
                'User', ownership.user_id, 'OWNS', 'Book', ownership.book_id,
                ownership_data
            )
            
            if success:
                
                # Handle personal custom metadata through custom field service
                if ownership.custom_metadata:
                    try:
                        print(f"📝 [SIMPLIFIED] Processing {len(ownership.custom_metadata)} personal custom fields")
                        
                        # Ensure field definitions exist
                        fields_ensured = self.custom_field_service.ensure_custom_fields_exist(
                            ownership.user_id, {}, ownership.custom_metadata
                        )
                        
                        if fields_ensured:
                            # Save personal custom metadata
                            personal_saved = self.custom_field_service.save_custom_metadata_sync(
                                ownership.book_id, ownership.user_id, ownership.custom_metadata
                            )
                            
                            if personal_saved:
                                pass  # Personal custom metadata saved successfully
                            else:
                                pass  # Personal custom metadata save failed
                        else:
                            pass  # User not found for custom metadata
                            
                    except Exception as e:
                        pass  # Error saving personal custom metadata
                
                # KuzuDB handles its own persistence automatically via WAL
                # No manual checkpoint needed - ownership data is already durable
                
                return True
            else:
                return False
                
        except Exception as e:
            return False
    
    def find_book_by_isbn(self, isbn: str) -> Optional[str]:
        """Find existing book by ISBN. Returns book_id if found."""
        try:
            # Normalize ISBN
            normalized_isbn = ''.join(filter(str.isdigit, isbn))
            
            # Search by ISBN13
            if len(normalized_isbn) == 13:
                query = "MATCH (b:Book {isbn13: $isbn}) RETURN b.id"
                result = self.storage.execute_cypher(query, {"isbn": normalized_isbn})
                if result:
                    return result[0]['col_0']
            
            # Search by ISBN10  
            elif len(normalized_isbn) == 10:
                query = "MATCH (b:Book {isbn10: $isbn}) RETURN b.id"
                result = self.storage.execute_cypher(query, {"isbn": normalized_isbn})
                if result:
                    return result[0]['col_0']
            
            return None
            
        except Exception as e:
            return None
    
    async def find_or_create_book(self, book_data: SimplifiedBook) -> Optional[str]:
        """
        Find existing book or create new one.
        Returns book_id if successful.
        """
        try:
            # Try to find existing book by ISBN
            if book_data.isbn13:
                existing_id = self.find_book_by_isbn(book_data.isbn13)
                if existing_id:
                    return existing_id
            
            if book_data.isbn10:
                existing_id = self.find_book_by_isbn(book_data.isbn10)
                if existing_id:
                    return existing_id
            
            # Book doesn't exist, create new one
            return await self.create_standalone_book(book_data)
            
        except Exception as e:
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
        print(f"🔧 [BUILD_BOOK] Building book from row: {list(row.keys())[:5]}...")
        
        # Initialize book data
        book_data = {
            'title': '',
            'author': '',
            'isbn13': None,
            'isbn10': None,
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
                    book_data['author'] = value
                elif book_field == 'isbn':
                    # Clean and assign ISBN
                    clean_isbn = ''.join(c for c in str(value) if c.isdigit() or c.upper() == 'X')
                    if len(clean_isbn) == 13:
                        book_data['isbn13'] = clean_isbn
                    elif len(clean_isbn) == 10:
                        book_data['isbn10'] = clean_isbn
                elif book_field == 'isbn13':
                    # Normalize Goodreads format and clean ISBN13
                    normalized_isbn = normalize_goodreads_value(value, 'isbn')
                    if normalized_isbn:
                        clean_isbn = ''.join(c for c in str(normalized_isbn) if c.isdigit() or c.upper() == 'X')
                        if len(clean_isbn) == 13:
                            book_data['isbn13'] = clean_isbn
                        elif len(clean_isbn) == 10:
                            book_data['isbn10'] = clean_isbn
                elif book_field == 'isbn10':
                    # Normalize Goodreads format and clean ISBN10
                    normalized_isbn = normalize_goodreads_value(value, 'isbn')
                    if normalized_isbn:
                        clean_isbn = ''.join(c for c in str(normalized_isbn) if c.isdigit() or c.upper() == 'X')
                        if len(clean_isbn) == 10:
                            book_data['isbn10'] = clean_isbn
                        elif len(clean_isbn) == 13:
                            book_data['isbn13'] = clean_isbn
                elif book_field == 'publisher':
                    book_data['publisher'] = value
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
        
        # Create SimplifiedBook instance
        print(f"🔧 [BUILD_BOOK] Final book_data before SimplifiedBook creation: {book_data}")
        print(f"🔧 [BUILD_BOOK] book_data keys: {list(book_data.keys())}")
        print(f"🔧 [BUILD_BOOK] additional_authors in book_data: {'additional_authors' in book_data}")
        if 'additional_authors' in book_data:
            print(f"🔧 [BUILD_BOOK] additional_authors value: '{book_data['additional_authors']}'")
        
        simplified_book = SimplifiedBook(**book_data)
        print(f"🔧 [BUILD_BOOK] Created SimplifiedBook, has additional_authors: {hasattr(simplified_book, 'additional_authors')}")
        if hasattr(simplified_book, 'additional_authors'):
            print(f"🔧 [BUILD_BOOK] SimplifiedBook.additional_authors value: '{simplified_book.additional_authors}'")
        
        return simplified_book
    
    async def add_book_to_user_library(self, book_data: SimplifiedBook, user_id: str, 
                                reading_status: str = "plan_to_read",
                                ownership_status: str = "owned",
                                media_type: str = "physical",
                                user_rating: Optional[float] = None,
                                personal_notes: Optional[str] = None,
                                location_id: Optional[str] = None,
                                custom_metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Complete workflow: Find/create book + create user ownership.
        This is the main entry point for the simplified architecture.
        """
        try:
            print(f"🎯 [SIMPLIFIED] Starting add_book_to_user_library for: {book_data.title}")
            
            # Step 1: Find or create standalone book
            book_id = await self.find_or_create_book(book_data)
            if not book_id:
                return False
            
            # Step 2: Create user ownership relationship
            ownership = UserBookOwnership(
                user_id=user_id,
                book_id=book_id,
                reading_status=reading_status,
                ownership_status=ownership_status,
                media_type=media_type,
                date_added=datetime.utcnow(),
                user_rating=user_rating,
                personal_notes=personal_notes,
                location_id=location_id,
                custom_metadata=custom_metadata
            )
            
            ownership_success = self.create_user_ownership(ownership)
            if not ownership_success:
                return False

            # Step 3: Handle location assignment (NEW)
            if location_id:
                try:
                    from .location_service import LocationService
                    from .infrastructure.kuzu_graph import get_kuzu_connection
                    
                    kuzu_connection = get_kuzu_connection()
                    print(f"🔥 [LOCATION_SERVICE] Using KuzuDB instance: {id(kuzu_connection)}")
                    location_service = LocationService(kuzu_connection.connect())
                    
                    location_success = location_service.add_book_to_location(book_id, location_id, user_id)
                    if location_success:
                        pass  # Book added to location successfully
                    else:
                        pass  # Failed to add book to location
                        
                except Exception as e:
                    pass  # Don't fail the entire operation for location assignment issues
            else:
                try:
                    from .location_service import LocationService
                    from .infrastructure.kuzu_graph import get_kuzu_connection
                    
                    kuzu_connection = get_kuzu_connection()
                    print(f"🔥 [DEFAULT_LOCATION] Using KuzuDB instance: {id(kuzu_connection)}")
                    location_service = LocationService(kuzu_connection.connect())
                    
                    # Get or create default location
                    default_location = location_service.get_default_location(user_id)
                    if not default_location:
                        default_locations = location_service.setup_default_locations()
                        if default_locations:
                            default_location = default_locations[0]
                    
                    if default_location and default_location.id:
                        location_success = location_service.add_book_to_location(book_id, default_location.id, user_id)
                        if location_success:
                            pass  # Book added to default location successfully
                        else:
                            pass  # Failed to add book to default location
                    else:
                        pass  # No default location available
                        
                except Exception as e:
                    pass  # Don't fail the entire operation for location assignment issues
            
            print(f"🎉 [SIMPLIFIED] Successfully added book to user library")
            
            # Use safe checkpoint to ensure ownership data is visible after container restarts
            self.storage.safe_checkpoint()
            
            return True
            
        except Exception as e:
            return False
    
    def add_book_to_user_library_sync(self, book_data: SimplifiedBook, user_id: str, 
                                     reading_status: str = "plan_to_read",
                                     ownership_status: str = "owned",
                                     media_type: str = "physical",
                                     user_rating: Optional[float] = None,
                                     personal_notes: Optional[str] = None,
                                     location_id: Optional[str] = None,
                                     custom_metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Synchronous wrapper for add_book_to_user_library.
        Use this method from Flask routes and other sync contexts.
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


# Convenience function for current routes
def create_simplified_book_service():
    """Factory function to create simplified book service."""
    return SimplifiedBookService()
