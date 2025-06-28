"""
Kuzu-based repository implementations.

Concrete implementations of the domain repository interfaces using Kuzu as the storage backend.
"""

import uuid
import json
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, date
from dataclasses import asdict
from enum import Enum

from flask import current_app

from ..domain.models import Book, User, Author, Person, BookContribution, ContributionType, Publisher, Series, Category, UserBookRelationship, ReadingStatus, OwnershipStatus, MediaType, CustomFieldDefinition, ImportMappingTemplate, CustomFieldType
from ..domain.repositories import BookRepository, UserRepository, AuthorRepository, UserBookRepository, CustomFieldRepository, ImportMappingRepository
from .kuzu_graph import KuzuGraphStorage


def _serialize_for_kuzu(obj: Any) -> Any:
    """Convert objects to Kuzu-compatible format."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, date):
        return obj.isoformat()
    elif isinstance(obj, Enum):
        return obj.value
    elif hasattr(obj, '__dict__'):
        # Handle dataclass or object with attributes
        return {k: _serialize_for_kuzu(v) for k, v in obj.__dict__.items()}
    elif isinstance(obj, dict):
        return {k: _serialize_for_kuzu(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_serialize_for_kuzu(item) for item in obj]
    else:
        return obj


def _ensure_datetime_fields(data: dict, fields: list):
    """Ensure specified fields in data are Python datetime objects, handling both strings and datetime objects."""
    for field in fields:
        if field in data:
            val = data[field]
            if isinstance(val, str):
                try:
                    data[field] = _safe_fromisoformat(val)
                except Exception as e:
                    print(f"[KUZU_REPO][WARN] Could not convert field '{field}' value '{val}' to datetime: {e}")
            elif isinstance(val, datetime):
                # Already a datetime object, no conversion needed
                pass
            elif val is not None:
                print(f"[KUZU_REPO][WARN] Field '{field}' is not datetime or str: {type(val)}")
    return data

def _safe_fromisoformat(value):
    """Safely convert a value to datetime, handling both strings and datetime objects."""
    if isinstance(value, datetime):
        return value
    elif isinstance(value, str):
        return datetime.fromisoformat(value)
    else:
        return value

def _safe_date_fromisoformat(value):
    """Safely convert a value to date, handling both strings, dates, and datetime objects."""
    if isinstance(value, date):
        return value
    elif isinstance(value, datetime):
        return value.date()
    elif isinstance(value, str):
        try:
            # Try parsing as ISO date string
            return datetime.fromisoformat(value).date()
        except ValueError:
            try:
                # Try parsing as date-only string
                return datetime.strptime(value, '%Y-%m-%d').date()
            except ValueError:
                try:
                    # Try parsing with full timestamp and extract date
                    return datetime.fromisoformat(value.replace('Z', '+00:00')).date()
                except ValueError:
                    print(f"[KUZU_REPO][WARN] Could not parse date string: {value}")
                    return None
    else:
        return value

def _clean_kuzu_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Clean Kuzu data by removing internal fields like _id."""
    if not isinstance(data, dict):
        return data
    
    cleaned = {}
    for key, value in data.items():
        # Skip internal Kuzu fields that start with underscore
        if key.startswith('_'):
            continue
        cleaned[key] = value
    return cleaned


def _filter_model_fields(data: Dict[str, Any], model_class) -> Dict[str, Any]:
    """Filter data to only include fields that exist in the target dataclass model."""
    import inspect
    
    # Get field names from the dataclass if available
    if hasattr(model_class, '__dataclass_fields__'):
        allowed_fields = set(model_class.__dataclass_fields__.keys())
    else:
        # Fallback for non-dataclasses - inspect the __init__ method
        sig = inspect.signature(model_class.__init__)
        allowed_fields = set(sig.parameters.keys()) - {'self'}
    
    return {k: v for k, v in data.items() if k in allowed_fields}


def _clean_custom_field_data(field_data: Dict[str, Any]) -> Dict[str, Any]:
    """Clean and map custom field data from database to model format."""
    field_data = _clean_kuzu_data(field_data)
    
    # Map field names from database schema to model field names
    if 'user_id' in field_data:
        field_data['created_by_user_id'] = field_data.pop('user_id')
    
    # Remove fields that don't exist in the CustomFieldDefinition model
    model_fields = {
        'id', 'name', 'display_name', 'field_type', 'description', 
        'created_by_user_id', 'is_shareable', 'is_global', 'default_value',
        'placeholder_text', 'help_text', 'predefined_options', 'allow_custom_options',
        'rating_min', 'rating_max', 'rating_labels', 'usage_count', 'created_at', 'updated_at'
    }
    field_data = {k: v for k, v in field_data.items() if k in model_fields}
    
    # Type conversions
    if field_data.get('created_at'):
        field_data['created_at'] = _safe_fromisoformat(field_data['created_at'])
    if field_data.get('field_type'):
        field_data['field_type'] = CustomFieldType(field_data['field_type'])
    
    return field_data


def _old_filter_model_fields_implementation(data: Dict[str, Any], model_class) -> Dict[str, Any]:
    """Filter data to only include fields that exist in the target dataclass model."""
    import dataclasses
    
    if not dataclasses.is_dataclass(model_class):
        return data
    
    # Get all field names from the dataclass
    field_names = {field.name for field in dataclasses.fields(model_class)}
    
    # Filter data to only include valid fields
    filtered = {}
    for key, value in data.items():
        if key in field_names:
            filtered[key] = value
    
    return filtered


class KuzuBookRepository(BookRepository):
    """Kuzu-based implementation of BookRepository."""
    
    def __init__(self, storage: KuzuGraphStorage):
        self.storage = storage
        
    async def create(self, book: Book) -> Book:
        """Create a new book."""
        if not book.id:
            book.id = str(uuid.uuid4())
        
        print(f"🔍 [KUZU_REPO] Creating book: {book.title}")
        print(f"📚 [KUZU_REPO] Book has {len(book.authors)} authors")
        
        # Convert book to dictionary for storage
        book_data = asdict(book)
        
        # Handle DATE fields specifically - ensure they are proper date objects or None
        if 'published_date' in book_data and book_data['published_date'] is not None:
            book_data['published_date'] = _safe_date_fromisoformat(book_data['published_date'])
            print(f"[KUZU_REPO][DEBUG] published_date converted to: {book_data['published_date']} (type: {type(book_data['published_date'])})")
        
        # Remove None values for optional fields that should be omitted if empty
        optional_fields = ['published_date']
        for field in optional_fields:
            if book_data.get(field) is None:
                book_data.pop(field, None)
        
        # Ensure TIMESTAMP fields are datetime objects
        for ts_field in ['created_at', 'updated_at']:
            if ts_field in book_data:
                if isinstance(book_data[ts_field], str):
                    try:
                        book_data[ts_field] = _safe_fromisoformat(book_data[ts_field])
                    except Exception as e:
                        print(f"[KUZU_REPO][ERROR] Could not convert {ts_field}: {e}")
        
        if not book_data.get('created_at'):
            book_data['created_at'] = datetime.utcnow()
        if not book_data.get('updated_at'):
            book_data['updated_at'] = datetime.utcnow()
            
        # Ensure normalized_title is set (required by schema)
        if not book_data.get('normalized_title'):
            book_data['normalized_title'] = book.title.strip().lower() if book.title else ""
        
        # Handle complex fields that need JSON serialization
        if 'custom_metadata' in book_data and book_data['custom_metadata']:
            book_data['custom_metadata'] = json.dumps(book_data['custom_metadata'])
        elif 'custom_metadata' in book_data:
            book_data['custom_metadata'] = ""  # Empty string for empty dict
            
        if 'raw_categories' in book_data and book_data['raw_categories']:
            book_data['raw_categories'] = json.dumps(book_data['raw_categories'])
        elif 'raw_categories' in book_data:
            book_data['raw_categories'] = ""  # Empty string for None
        
        # Remove legacy/invalid fields for Book dataclass
        book_data.pop('isbn', None)  # Remove if present (not a Book field)
        # Remove nested objects (handled separately)
        book_data.pop('authors', None)
        book_data.pop('categories', None)
        book_data.pop('publisher', None)
        book_data.pop('series', None)
        book_data.pop('contributors', None)  # Remove contributors (handled via relationships)
        # Debug output for TIMESTAMP fields
        for ts_field in ['created_at', 'updated_at']:
            if ts_field in book_data:
                print(f"[KUZU_REPO][DEBUG] {ts_field}: {book_data[ts_field]} (type: {type(book_data[ts_field])})")
        success = self.storage.store_node('Book', book.id, book_data)
        if not success:
            raise Exception(f"Failed to create book: {book.title}")
        
        # Create author relationships (store as Person nodes)
        for author in book.authors:
            if not author.id:
                author.id = str(uuid.uuid4())
            
            # Convert Author to Person for storage
            from ..domain.models import Person
            person = Person(
                id=author.id,
                name=author.name,
                normalized_name=author.normalized_name,
                birth_year=author.birth_year,
                death_year=author.death_year,
                bio=author.bio,
                created_at=author.created_at if hasattr(author, 'created_at') else datetime.utcnow()
            )
            
            # Store person node if not exists
            existing_person = self.storage.get_node('Person', person.id)
            if not existing_person:
                person_data = asdict(person)
                self.storage.store_node('Person', person.id, person_data)
            
            # Create AUTHORED relationship
            self.storage.create_relationship(
                'Person', person.id, 'AUTHORED', 'Book', book.id,
                {'created_at': datetime.utcnow().isoformat()}
            )
        
        # Create category relationships
        for category in book.categories:
            # Handle both Category objects and strings
            if isinstance(category, str):
                # Convert string to Category object
                from ..domain.models import Category
                category_obj = Category(
                    id=str(uuid.uuid4()),
                    name=category,
                    normalized_name=category.lower().strip()
                )
            else:
                category_obj = category
                if not category_obj.id:
                    category_obj.id = str(uuid.uuid4())
            
            # Store category node if not exists
            existing_category = self.storage.get_node('Category', category_obj.id)
            if not existing_category:
                category_data = asdict(category_obj)
                self.storage.store_node('Category', category_obj.id, category_data)
            
            # Create CATEGORIZED_AS relationship
            self.storage.create_relationship(
                'Book', book.id, 'CATEGORIZED_AS', 'Category', category_obj.id,
                {'created_at': datetime.utcnow().isoformat()}
            )
        
        # Create publisher relationship
        if book.publisher:
            if not book.publisher.id:
                book.publisher.id = str(uuid.uuid4())
            
            # Store publisher node if not exists
            existing_publisher = self.storage.get_node('Publisher', book.publisher.id)
            if not existing_publisher:
                publisher_data = asdict(book.publisher)
                self.storage.store_node('Publisher', book.publisher.id, publisher_data)
            
            # Create PUBLISHED_BY relationship
            self.storage.create_relationship(
                'Book', book.id, 'PUBLISHED_BY', 'Publisher', book.publisher.id,
                {'publication_date': book.published_date.isoformat() if book.published_date else None, 'created_at': datetime.utcnow().isoformat()}
            )
        
        # Create series relationship
        if book.series:
            if not book.series.id:
                book.series.id = str(uuid.uuid4())
            
            # Store series node if not exists
            existing_series = self.storage.get_node('Series', book.series.id)
            if not existing_series:
                series_data = asdict(book.series)
                self.storage.store_node('Series', book.series.id, series_data)
            
            # Create PART_OF_SERIES relationship
            self.storage.create_relationship(
                'Book', book.id, 'PART_OF_SERIES', 'Series', book.series.id,
                {'volume_number': getattr(book, 'volume_number', None), 'created_at': datetime.utcnow().isoformat()}
            )
        
        # Create contributor relationships
        for contributor in book.contributors:
            if not contributor.person.id:
                contributor.person.id = str(uuid.uuid4())
            
            # Store person node if not exists
            existing_person = self.storage.get_node('Person', contributor.person.id)
            if not existing_person:
                person_data = asdict(contributor.person)
                self.storage.store_node('Person', contributor.person.id, person_data)
            
            # Create WRITTEN_BY relationship
            self.storage.create_relationship(
                'Book', book.id, 'WRITTEN_BY', 'Person', contributor.person.id,
                {
                    'contribution_type': contributor.contribution_type.value,  # Convert enum to string
                    'role': getattr(contributor, 'role', None),
                    'created_at': datetime.utcnow().isoformat()
                }
            )
        
        print(f"✅ [KUZU_REPO] Successfully created book: {book.title}")
        return book
    
    async def get_by_id(self, book_id: str) -> Optional[Book]:
        """Get a book by ID with all relationships."""
        book_data = self.storage.get_node('Book', book_id)
        if not book_data:
            return None
        
        # Get relationships to rebuild the complete book object
        book = self._build_book_from_data(book_data, book_id)
        return book
    
    def _build_book_from_data(self, book_data: Dict[str, Any], book_id: str) -> Book:
        """Build a complete Book object from stored data and relationships."""
        # Clean Kuzu data
        book_data = _clean_kuzu_data(book_data)
        
        # Get authors via AUTHORED relationships
        author_rels = self.storage.get_relationships('Author', None, 'AUTHORED')
        authors = []
        for rel in author_rels:
            if rel['target'].get('id') == book_id:
                author_data = self.storage.get_node('Author', rel['relationship'].get('from_id'))
                if author_data:
                    author_data = _clean_kuzu_data(author_data)
                    authors.append(Author(**author_data))
        
        # Get categories via CATEGORIZED_AS relationships
        category_rels = self.storage.get_relationships('Book', book_id, 'CATEGORIZED_AS')
        categories = []
        for rel in category_rels:
            category_data = _clean_kuzu_data(rel['target'])
            categories.append(Category(**category_data))
        
        # Get publisher via PUBLISHED_BY relationships
        publisher = None
        publisher_rels = self.storage.get_relationships('Book', book_id, 'PUBLISHED_BY')
        for rel in publisher_rels:
            if rel['target'].get('id') == book_id:
                publisher_data = self.storage.get_node('Publisher', rel['relationship'].get('from_id'))
                if publisher_data:
                    publisher_data = _clean_kuzu_data(publisher_data)
                    publisher = Publisher(**publisher_data)
                    break
        
        # Get series via PART_OF_SERIES relationships
        series = None
        series_rels = self.storage.get_relationships('Book', book_id, 'PART_OF_SERIES')
        if series_rels:
            series_data = _clean_kuzu_data(series_rels[0]['target'])
            series = Series(**series_data)
        
        # Convert date strings back to date objects
        if book_data.get('published_date'):
            book_data['published_date'] = _safe_date_fromisoformat(book_data['published_date'])
        if book_data.get('created_at'):
            book_data['created_at'] = _safe_fromisoformat(book_data['created_at'])
        if book_data.get('updated_at'):
            book_data['updated_at'] = _safe_fromisoformat(book_data['updated_at'])
        
        # Convert authors to contributors
        contributors = []
        for author in authors:
            # Convert Author to Person first
            person = Person(
                id=author.id,
                name=author.name,
                normalized_name=author.normalized_name,
                birth_year=author.birth_year,
                death_year=author.death_year,
                bio=author.bio,
                created_at=author.created_at if hasattr(author, 'created_at') else datetime.utcnow()
            )
            
            # Create BookContribution
            contribution = BookContribution(
                person=person,
                contribution_type=ContributionType.AUTHORED,
                created_at=datetime.utcnow()
            )
            contributors.append(contribution)
        
        # Get contributors via WRITTEN_BY relationships
        contributed_rels = self.storage.get_relationships('Book', book_id, 'WRITTEN_BY')
        for rel in contributed_rels:
            if rel['target'].get('id') == book_id:
                person_data = self.storage.get_node('Person', rel['relationship'].get('from_id'))
                if person_data:
                    person_data = _clean_kuzu_data(person_data)
                    person = Person(**person_data)
                    
                    # Get contribution type from relationship properties
                    rel_props = rel['relationship']
                    contribution_type_str = rel_props.get('contribution_type', 'contributed')
                    try:
                        contribution_type = ContributionType(contribution_type_str)
                    except ValueError:
                        contribution_type = ContributionType.CONTRIBUTED
                    
                    # Create BookContribution
                    contribution = BookContribution(
                        person=person,
                        contribution_type=contribution_type,
                        role=rel_props.get('role'),
                        created_at=_safe_fromisoformat(rel_props.get('created_at')) if rel_props.get('created_at') else datetime.utcnow()
                    )
                    contributors.append(contribution)
        
        # Remove legacy/invalid fields for Book dataclass
        book_data.pop('isbn', None)
        book_data.pop('authors', None)
        book_data.pop('categories', None)
        book_data.pop('publisher', None)
        book_data.pop('series', None)
        book_data.pop('contributors', None)
        
        # Create Book object
        book = Book(
            **book_data,
            contributors=contributors,
            categories=categories,
            publisher=publisher,
            series=series
        )
        
        return book
    
    async def update(self, book: Book) -> Optional[Book]:
        """Update an existing book."""
        if not book.id:
            return None
        
        book_data = asdict(book)
        # Remove None values for optional fields
        optional_fields = ['published_date', 'created_at', 'updated_at']
        for field in optional_fields:
            if book_data.get(field) is None:
                book_data.pop(field, None)
        # Ensure TIMESTAMP fields are datetime objects
        for ts_field in ['created_at', 'updated_at']:
            if ts_field in book_data:
                if isinstance(book_data[ts_field], str):
                    try:
                        book_data[ts_field] = _safe_fromisoformat(book_data[ts_field])
                    except Exception as e:
                        print(f"[KUZU_REPO][ERROR] Could not convert {ts_field}: {e}")
        if not book_data.get('updated_at'):
            book_data['updated_at'] = datetime.utcnow()
        book_data.pop('isbn', None)
        book_data.pop('authors', None)
        book_data.pop('categories', None)
        book_data.pop('publisher', None)
        book_data.pop('series', None)
        book_data.pop('contributors', None)
        for ts_field in ['created_at', 'updated_at']:
            if ts_field in book_data:
                print(f"[KUZU_REPO][DEBUG] {ts_field}: {book_data[ts_field]} (type: {type(book_data[ts_field])})")
        success = self.storage.update_node('Book', book.id, book_data)
        return book if success else None
    
    async def delete(self, book_id: str) -> bool:
        """Delete a book and all its relationships."""
        return self.storage.delete_node('Book', book_id)
    
    async def list_all(self, limit: int = 100, offset: int = 0) -> List[Book]:
        """List all books with pagination."""
        book_nodes = self.storage.get_nodes_by_type('Book', limit, offset)
        books = []
        for book_data in book_nodes:
            book = self._build_book_from_data(book_data, book_data['id'])
            books.append(book)
        return books
    
    async def search_by_title(self, title: str, limit: int = 10) -> List[Book]:
        """Search books by title."""
        query = """
        MATCH (b:Book)
        WHERE b.title CONTAINS $title
        RETURN b
        LIMIT $limit
        """
        results = self.storage.execute_cypher(query, {"title": title, "limit": limit})
        books = []
        for result in results:
            book_data = result['col_0']
            book = self._build_book_from_data(book_data, book_data['id'])
            books.append(book)
        return books
    
    async def search_by_author(self, author_name: str, limit: int = 10) -> List[Book]:
        """Search books by author name."""
        query = """
        MATCH (a:Author)-[:AUTHORED]->(b:Book)
        WHERE a.name CONTAINS $author_name
        RETURN DISTINCT b
        LIMIT $limit
        """
        results = self.storage.execute_cypher(query, {"author_name": author_name, "limit": limit})
        books = []
        for result in results:
            book_data = result['col_0']
            book = self._build_book_from_data(book_data, book_data['id'])
            books.append(book)
        return books
    
    async def get_by_isbn(self, isbn: str) -> Optional[Book]:
        """Get a book by ISBN."""
        query = """
        MATCH (b:Book)
        WHERE b.isbn13 = $isbn OR b.isbn10 = $isbn
        RETURN b
        """
        results = self.storage.execute_cypher(query, {"isbn": isbn})
        if results:
            book_data = results[0]['col_0']
            return self._build_book_from_data(book_data, book_data['id'])
        return None
    
    async def find_duplicates(self, book: Book) -> List[Tuple[Book, float]]:
        """Find potential duplicate books with confidence scores."""
        # Basic implementation - can be enhanced with more sophisticated matching
        duplicates = []
        
        # Search by ISBN first (exact match)
        if book.isbn13:
            isbn_match = await self.get_by_isbn(book.isbn13)
            if isbn_match and isbn_match.id != book.id:
                duplicates.append((isbn_match, 1.0))  # Perfect match
        
        # Search by title (fuzzy match)
        if book.title:
            title_matches = await self.search_by_title(book.title, limit=5)
            for match in title_matches:
                if match.id != book.id:
                    # Simple confidence based on title similarity
                    confidence = 0.8 if match.title.lower() == book.title.lower() else 0.6
                    duplicates.append((match, confidence))
        
        # Remove duplicates and sort by confidence
        seen_ids = set()
        unique_duplicates = []
        for dup_book, confidence in duplicates:
            if dup_book.id not in seen_ids:
                unique_duplicates.append((dup_book, confidence))
                seen_ids.add(dup_book.id)
        
        return sorted(unique_duplicates, key=lambda x: x[1], reverse=True)
    
    async def search(self, query: str, filters: Dict[str, Any] = None) -> List[Book]:
        """Search books with optional filters."""
        # Build dynamic query based on filters
        where_conditions = []
        params = {"query": f"%{query}%"}
        
        # Basic text search across title, description, etc.
        where_conditions.append("(b.title CONTAINS $query OR b.description CONTAINS $query)")
        
        # Add filters if provided
        if filters:
            if filters.get('author'):
                where_conditions.append("EXISTS((b)-[:AUTHORED]-(a:Author)) AND a.name CONTAINS $author_filter")
                params['author_filter'] = f"%{filters['author']}%"
            
            if filters.get('category'):
                where_conditions.append("EXISTS((b)-[:CATEGORIZED_AS]-(c:Category)) AND c.name CONTAINS $category_filter")
                params['category_filter'] = f"%{filters['category']}%"
            
            if filters.get('isbn'):
                where_conditions.append("(b.isbn13 = $isbn_filter OR b.isbn10 = $isbn_filter)")
                params['isbn_filter'] = filters['isbn']
        
        where_clause = " AND ".join(where_conditions)
        cypher_query = f"""
        MATCH (b:Book)
        WHERE {where_clause}
        RETURN b
        LIMIT 50
        """
        
        results = self.storage.execute_cypher(cypher_query, params)
        books = []
        for result in results:
            book_data = result['col_0']
            book = self._build_book_from_data(book_data, book_data['id'])
            books.append(book)
        return books
    
    async def merge_books(self, source_id: str, target_id: str) -> Book:
        """Merge two duplicate book records (admin only)."""
        # Get both books
        source_book = await self.get_by_id(source_id)
        target_book = await self.get_by_id(target_id)
        
        if not source_book or not target_book:
            raise ValueError("One or both books not found")
        
        # Merge logic: update target with any missing data from source
        merged_data = {}
        
        # Copy non-empty fields from source to target if target field is empty
        for field in ['description', 'isbn13', 'isbn10', 'publisher', 'published_date', 
                     'page_count', 'language', 'cover_image_url']:
            source_val = getattr(source_book, field, None)
            target_val = getattr(target_book, field, None)
            if source_val and not target_val:
                merged_data[field] = source_val
        
        # Update target book with merged data
        if merged_data:
            for field, value in merged_data.items():
                setattr(target_book, field, value)
            target_book = await self.update(target_book)
        
        # Move all relationships from source to target
        # This would involve updating user relationships, reading logs, etc.
        # For now, we'll implement a basic version
        
        # Delete the source book after merging
        await self.delete(source_id)
        
        return target_book
    
    async def get_related_books(self, book_id: str) -> List[Book]:
        """Get books related by the same author or in the same category."""
        query = """
        MATCH (b:Book)-[:AUTHORED|CATEGORIZED_AS]-(related:Book)
        WHERE b.id = $book_id AND related.id <> $book_id
        RETURN DISTINCT related
        LIMIT 10
        """
        results = self.storage.execute_cypher(query, {"book_id": book_id})
        related_books = []
        for result in results:
            book_data = result['col_0']
            related_book = self._build_book_from_data(book_data, book_data['id'])
            related_books.append(related_book)
        return related_books


class KuzuUserRepository(UserRepository):
    """Kuzu-based implementation of UserRepository."""
    
    def __init__(self, storage: KuzuGraphStorage):
        self.storage = storage
    
    async def create(self, user: User) -> User:
        """Create a new user."""
        if not user.id:
            user.id = str(uuid.uuid4())
        
        user_data = asdict(user)
        # Remove None values for optional fields to avoid Kuzu type errors
        optional_fields = ['locked_until', 'last_login', 'password_changed_at', 'display_name', 'bio', 'location', 'website']
        for field in optional_fields:
            if user_data.get(field) is None:
                user_data.pop(field, None)
        # Ensure TIMESTAMP fields are datetime objects
        for ts_field in ['created_at', 'updated_at']:
            if ts_field in user_data:
                if isinstance(user_data[ts_field], str):
                    try:
                        user_data[ts_field] = _safe_fromisoformat(user_data[ts_field])
                    except Exception as e:
                        print(f"[KUZU_REPO][ERROR] Could not convert {ts_field}: {e}")
        if not user_data.get('created_at'):
            user_data['created_at'] = datetime.utcnow()
        if not user_data.get('updated_at'):
            user_data['updated_at'] = datetime.utcnow()
        for ts_field in ['created_at', 'updated_at']:
            if ts_field in user_data:
                print(f"[KUZU_REPO][DEBUG] {ts_field}: {user_data[ts_field]} (type: {type(user_data[ts_field])})")
        success = self.storage.store_node('User', user.id, user_data)
        if not success:
            raise Exception(f"Failed to create user: {user.username}")
        
        return user
    
    async def get_by_id(self, user_id: str) -> Optional[User]:
        """Get a user by ID."""
        user_data = self.storage.get_node('User', user_id)
        if not user_data:
            return None
        
        # Clean Kuzu data 
        user_data = _clean_kuzu_data(user_data)
        
        # Convert timestamp strings back to datetime objects
        if user_data.get('created_at'):
            user_data['created_at'] = _safe_fromisoformat(user_data['created_at'])
        if user_data.get('updated_at'):
            user_data['updated_at'] = _safe_fromisoformat(user_data['updated_at'])
        
        return User(**user_data)
    
    async def get_by_username(self, username: str) -> Optional[User]:
        """Get a user by username."""
        query = """
        MATCH (u:User)
        WHERE u.username = $username
        RETURN u
        """
        results = self.storage.execute_cypher(query, {"username": username})
        if results:
            user_data = _clean_kuzu_data(results[0]['col_0'])
            if user_data.get('created_at'):
                user_data['created_at'] = _safe_fromisoformat(user_data['created_at'])
            if user_data.get('updated_at'):
                user_data['updated_at'] = _safe_fromisoformat(user_data['updated_at'])
            return User(**user_data)
        return None
    
    async def get_by_email(self, email: str) -> Optional[User]:
        """Get a user by email."""
        query = """
        MATCH (u:User)
        WHERE u.email = $email
        RETURN u
        """
        results = self.storage.execute_cypher(query, {"email": email})
        if results:
            user_data = _clean_kuzu_data(results[0]['col_0'])
            if user_data.get('created_at'):
                user_data['created_at'] = _safe_fromisoformat(user_data['created_at'])
            if user_data.get('updated_at'):
                user_data['updated_at'] = _safe_fromisoformat(user_data['updated_at'])
            return User(**user_data)
        return None
    
    async def update(self, user: User) -> Optional[User]:
        """Update an existing user."""
        if not user.id:
            return None
        
        user_data = asdict(user)
        # Remove None values for optional fields
        optional_fields = ['locked_until', 'last_login', 'password_changed_at', 'display_name', 'bio', 'location', 'website']
        for field in optional_fields:
            if user_data.get(field) is None:
                user_data.pop(field, None)
        # Ensure TIMESTAMP fields are datetime objects
        for ts_field in ['created_at', 'updated_at']:
            if ts_field in user_data:
                if isinstance(user_data[ts_field], str):
                    try:
                        user_data[ts_field] = _safe_fromisoformat(user_data[ts_field])
                    except Exception as e:
                        print(f"[KUZU_REPO][ERROR] Could not convert {ts_field}: {e}")
        if not user_data.get('updated_at'):
            user_data['updated_at'] = datetime.utcnow()
        for ts_field in ['created_at', 'updated_at']:
            if ts_field in user_data:
                print(f"[KUZU_REPO][DEBUG] {ts_field}: {user_data[ts_field]} (type: {type(user_data[ts_field])})")
        success = self.storage.update_node('User', user.id, user_data)
        return user if success else None
    
    async def delete(self, user_id: str) -> bool:
        """Delete a user and all relationships."""
        return self.storage.delete_node('User', user_id)
    
    async def list_all(self, limit: int = 100, offset: int = 0) -> List[User]:
        """List all users with pagination."""
        user_nodes = self.storage.get_nodes_by_type('User', limit, offset)
        users = []
        for user_data in user_nodes:
            user_data = _clean_kuzu_data(user_data)
            if user_data.get('created_at'):
                user_data['created_at'] = _safe_fromisoformat(user_data['created_at'])
            if user_data.get('updated_at'):
                user_data['updated_at'] = _safe_fromisoformat(user_data['updated_at'])
            users.append(User(**user_data))
        return users
    
    async def get_all(self) -> List[User]:
        """Get all users (alias for list_all without pagination)."""
        return await self.list_all(limit=1000, offset=0)


class KuzuAuthorRepository(AuthorRepository):
    """Kuzu-based implementation of AuthorRepository."""
    
    def __init__(self, storage: KuzuGraphStorage):
        self.storage = storage
    
    async def create(self, author: Author) -> Author:
        """Create a new author."""
        if not author.id:
            author.id = str(uuid.uuid4())
        
        author_data = asdict(author)
        success = self.storage.store_node('Author', author.id, author_data)
        if not success:
            raise Exception(f"Failed to create author: {author.name}")
        
        return author
    
    async def get_by_id(self, author_id: str) -> Optional[Author]:
        """Get an author by ID."""
        author_data = self.storage.get_node('Author', author_id)
        if not author_data:
            return None
        
        # Clean Kuzu data
        author_data = _clean_kuzu_data(author_data)
        
        # Convert date strings back to date objects
        if author_data.get('birth_date'):
            author_data['birth_date'] = _safe_date_fromisoformat(author_data['birth_date'])
        if author_data.get('death_date'):
            author_data['death_date'] = _safe_date_fromisoformat(author_data['death_date'])
        if author_data.get('created_at'):
            author_data['created_at'] = _safe_fromisoformat(author_data['created_at'])
        
        return Author(**author_data)
    
    async def get_by_name(self, name: str) -> Optional[Author]:
        """Get an author by name."""
        query = """
        MATCH (a:Author)
        WHERE a.name = $name OR a.normalized_name = $normalized_name
        RETURN a
        """
        normalized_name = name.lower().strip()
        results = self.storage.execute_cypher(query, {"name": name, "normalized_name": normalized_name})
        if results:
            author_data = _clean_kuzu_data(results[0]['col_0'])
            if author_data.get('birth_date'):
                author_data['birth_date'] = _safe_date_fromisoformat(author_data['birth_date'])
            if author_data.get('death_date'):
                author_data['death_date'] = _safe_date_fromisoformat(author_data['death_date'])
            if author_data.get('created_at'):
                author_data['created_at'] = _safe_fromisoformat(author_data['created_at'])
            return Author(**author_data)
        return None
    
    async def search_by_name(self, name: str, limit: int = 10) -> List[Author]:
        """Search authors by name."""
        query = """
        MATCH (a:Author)
        WHERE a.name CONTAINS $name
        RETURN a
        LIMIT $limit
        """
        results = self.storage.execute_cypher(query, {"name": name, "limit": limit})
        authors = []
        for result in results:
            author_data = _clean_kuzu_data(result['col_0'])
            if author_data.get('birth_date'):
                author_data['birth_date'] = _safe_date_fromisoformat(author_data['birth_date'])
            if author_data.get('death_date'):
                author_data['death_date'] = _safe_date_fromisoformat(author_data['death_date'])
            if author_data.get('created_at'):
                author_data['created_at'] = _safe_fromisoformat(author_data['created_at'])
            authors.append(Author(**author_data))
        return authors
    
    async def update(self, author: Author) -> Optional[Author]:
        """Update an existing author."""
        if not author.id:
            return None
        
        author_data = asdict(author)
        success = self.storage.update_node('Author', author.id, author_data)
        return author if success else None
    
    async def delete(self, author_id: str) -> bool:
        """Delete an author and all relationships."""
        return self.storage.delete_node('Author', author_id)
    
    async def find_by_name(self, name: str) -> List[Author]:
        """Find authors by name (fuzzy matching)."""
        query = """
        MATCH (a:Author)
        WHERE a.name CONTAINS $name OR a.normalized_name CONTAINS $normalized_name
        RETURN a
        """
        normalized_name = name.lower().strip()
        results = self.storage.execute_cypher(query, {"name": name, "normalized_name": normalized_name})
        authors = []
        for result in results:
            author_data = _clean_kuzu_data(result['col_0'])
            if author_data.get('birth_date'):
                author_data['birth_date'] = _safe_date_fromisoformat(author_data['birth_date'])
            if author_data.get('death_date'):
                author_data['death_date'] = _safe_date_fromisoformat(author_data['death_date'])
            if author_data.get('created_at'):
                author_data['created_at'] = _safe_fromisoformat(author_data['created_at'])
            authors.append(Author(**author_data))
        return authors
    
    async def get_collaborators(self, author_id: str) -> List[Author]:
        """Get authors who have collaborated with this author."""
        query = """
        MATCH (a1:Author)-[:COLLABORATED_WITH]-(a2:Author)
        WHERE a1.id = $author_id
        RETURN a2
        """
        results = self.storage.execute_cypher(query, {"author_id": author_id})
        collaborators = []
        for result in results:
            author_data = _clean_kuzu_data(result['col_0'])
            if author_data.get('birth_date'):
                author_data['birth_date'] = _safe_date_fromisoformat(author_data['birth_date'])
            if author_data.get('death_date'):
                author_data['death_date'] = _safe_date_fromisoformat(author_data['death_date'])
            if author_data.get('created_at'):
                author_data['created_at'] = _safe_fromisoformat(author_data['created_at'])
            collaborators.append(Author(**author_data))
        return collaborators

    async def list_all(self, limit: int = 100, offset: int = 0) -> List[Author]:
        """List all authors with pagination."""
        author_nodes = self.storage.get_nodes_by_type('Author', limit, offset)
        authors = []
        for author_data in author_nodes:
            author_data = _clean_kuzu_data(author_data)
            if author_data.get('birth_date'):
                author_data['birth_date'] = _safe_date_fromisoformat(author_data['birth_date'])
            if author_data.get('death_date'):
                author_data['death_date'] = _safe_date_fromisoformat(author_data['death_date'])
            if author_data.get('created_at'):
                author_data['created_at'] = _safe_fromisoformat(author_data['created_at'])
            authors.append(Author(**author_data))
        return authors


class KuzuUserBookRepository(UserBookRepository):
    """Kuzu-based implementation of UserBookRepository for user-book relationships."""
    
    def __init__(self, storage: KuzuGraphStorage):
        self.storage = storage
    
    async def get_relationship(self, user_id: str, book_id: str) -> Optional[UserBookRelationship]:
        """Get a specific user-book relationship."""
        query = """
        MATCH (u:User)-[r:OWNS]->(b:Book)
        WHERE u.id = $user_id AND b.id = $book_id
        RETURN r
        """
        results = self.storage.execute_cypher(query, {"user_id": user_id, "book_id": book_id})
        if results:
            rel_data = results[0]['col_0']
            
            # Convert date strings back to date objects
            if rel_data.get('reading_start_date'):
                rel_data['reading_start_date'] = _safe_date_fromisoformat(rel_data['reading_start_date'])
            if rel_data.get('reading_end_date'):
                rel_data['reading_end_date'] = _safe_date_fromisoformat(rel_data['reading_end_date'])
            if rel_data.get('created_at'):
                rel_data['created_at'] = _safe_fromisoformat(rel_data['created_at'])
            if rel_data.get('updated_at'):
                rel_data['updated_at'] = _safe_fromisoformat(rel_data['updated_at'])
            
            # Convert string enums back to enum objects
            if rel_data.get('reading_status'):
                rel_data['reading_status'] = ReadingStatus(rel_data['reading_status'])
            if rel_data.get('ownership_status'):
                rel_data['ownership_status'] = OwnershipStatus(rel_data['ownership_status'])
            
            rel_data['user_id'] = user_id
            rel_data['book_id'] = book_id
            
            return UserBookRelationship(**rel_data)
        return None
    
    async def delete_relationship(self, user_id: str, book_id: str) -> bool:
        """Delete a user-book relationship."""
        return self.storage.delete_relationship('User', user_id, 'OWNS', 'Book', book_id)
    
    async def get_user_books(self, user_id: str, reading_status: Optional[ReadingStatus] = None) -> List[Tuple[Book, UserBookRelationship]]:
        """Get all books for a user, optionally filtered by reading status."""
        if reading_status:
            query = """
            MATCH (u:User)-[r:OWNS]->(b:Book)
            WHERE u.id = $user_id AND r.reading_status = $reading_status
            RETURN b, r
            """
            params = {"user_id": user_id, "reading_status": reading_status.value}
        else:
            query = """
            MATCH (u:User)-[r:OWNS]->(b:Book)
            WHERE u.id = $user_id
            RETURN b, r
            """
            params = {"user_id": user_id}
        
        results = self.storage.execute_cypher(query, params)
        books_with_relationships = []
        
        for result in results:
            book_data = result['col_0']
            rel_data = result['col_1']
            
            # Build complete book object
            book_repo = KuzuBookRepository(self.storage)
            book = book_repo._build_book_from_data(book_data, book_data['id'])
            
            # Build relationship object
            if rel_data.get('reading_start_date'):
                rel_data['reading_start_date'] = _safe_date_fromisoformat(rel_data['reading_start_date'])
            if rel_data.get('reading_end_date'):
                rel_data['reading_end_date'] = _safe_date_fromisoformat(rel_data['reading_end_date'])
            if rel_data.get('created_at'):
                rel_data['created_at'] = _safe_fromisoformat(rel_data['created_at'])
            if rel_data.get('updated_at'):
                rel_data['updated_at'] = _safe_fromisoformat(rel_data['updated_at'])
            
            if rel_data.get('reading_status'):
                rel_data['reading_status'] = ReadingStatus(rel_data['reading_status'])
            if rel_data.get('ownership_status'):
                rel_data['ownership_status'] = OwnershipStatus(rel_data['ownership_status'])
            
            rel_data['user_id'] = user_id
            rel_data['book_id'] = book_data['id']
            
            relationship = UserBookRelationship(**rel_data)
            books_with_relationships.append((book, relationship))
        
        return books_with_relationships
    
    async def create_relationship(self, relationship: UserBookRelationship) -> UserBookRelationship:
        """Create a user-book relationship."""
        rel_data = asdict(relationship)
        # Remove user_id and book_id from the relationship data as they're part of the relationship structure
        user_id = rel_data.pop('user_id')
        book_id = rel_data.pop('book_id')
        
        # Serialize enum values and dates for Kuzu storage
        serialized_rel_data = {}
        for key, value in rel_data.items():
            if isinstance(value, Enum):
                serialized_rel_data[key] = value.value
            elif isinstance(value, (datetime, date)):
                serialized_rel_data[key] = value.isoformat()
            elif value is not None:  # Only include non-None values
                serialized_rel_data[key] = value
        
        success = self.storage.create_relationship(
            'User', user_id, 'OWNS', 'Book', book_id, serialized_rel_data
        )
        if success:
            return relationship
        else:
            raise Exception(f"Failed to create user-book relationship for user {user_id} and book {book_id}")
    
    async def get_user_library(self, user_id: str, filters: Dict[str, Any] = None) -> List[UserBookRelationship]:
        """Get a user's library with optional filters."""
        query_conditions = ["u.id = $user_id"]
        params = {"user_id": user_id}
        
        # Apply filters if provided
        if filters:
            if filters.get('reading_status'):
                query_conditions.append("r.reading_status = $reading_status")
                params['reading_status'] = filters['reading_status'].value if hasattr(filters['reading_status'], 'value') else filters['reading_status']
            
            if filters.get('ownership_status'):
                query_conditions.append("r.ownership_status = $ownership_status")
                params['ownership_status'] = filters['ownership_status'].value if hasattr(filters['ownership_status'], 'value') else filters['ownership_status']
        
        where_clause = " AND ".join(query_conditions)
        query = f"""
        MATCH (u:User)-[r:OWNS]->(b:Book)
        WHERE {where_clause}
        RETURN r, u.id, b.id
        """
        
        results = self.storage.execute_cypher(query, params)
        relationships = []
        
        for result in results:
            rel_data = result['col_0']
            user_id = result['col_1']
            book_id = result['col_2']
            
            # Convert date strings back to date objects
            if rel_data.get('reading_start_date'):
                rel_data['reading_start_date'] = _safe_date_fromisoformat(rel_data['reading_start_date'])
            if rel_data.get('reading_end_date'):
                rel_data['reading_end_date'] = _safe_date_fromisoformat(rel_data['reading_end_date'])
            if rel_data.get('created_at'):
                rel_data['created_at'] = _safe_fromisoformat(rel_data['created_at'])
            if rel_data.get('updated_at'):
                rel_data['updated_at'] = _safe_fromisoformat(rel_data['updated_at'])
            
            # Convert string enums back to enum objects
            if rel_data.get('reading_status'):
                rel_data['reading_status'] = ReadingStatus(rel_data['reading_status'])
            if rel_data.get('ownership_status'):
                rel_data['ownership_status'] = OwnershipStatus(rel_data['ownership_status'])
            
            rel_data['user_id'] = user_id
            rel_data['book_id'] = book_id
            
            relationship = UserBookRelationship(**rel_data)
            relationships.append(relationship)
        
        return relationships
    
    async def update_relationship(self, relationship: UserBookRelationship) -> UserBookRelationship:
        """Update a user-book relationship."""
        if not relationship.user_id or not relationship.book_id:
            raise ValueError("User ID and Book ID are required for updating relationship")
        
        # Convert relationship to updates dict
        rel_data = asdict(relationship)
        user_id = rel_data.pop('user_id')
        book_id = rel_data.pop('book_id')
        
        # Serialize enum values and dates
        serialized_updates = {}
        for key, value in rel_data.items():
            if isinstance(value, Enum):
                serialized_updates[key] = value.value
            elif isinstance(value, (datetime, date)):
                serialized_updates[key] = value.isoformat()
            elif value is not None:  # Only include non-None values
                serialized_updates[key] = value
        
        serialized_updates['updated_at'] = datetime.utcnow().isoformat()
        
        query = """
        MATCH (u:User)-[r:OWNS]->(b:Book)
        WHERE u.id = $user_id AND b.id = $book_id
        SET r += $updates
        RETURN r
        """
        
        try:
            results = self.storage.execute_cypher(query, {
                "user_id": user_id, 
                "book_id": book_id, 
                "updates": serialized_updates
            })
            if results:
                return relationship
            else:
                raise Exception("Relationship not found or update failed")
        except Exception as e:
            raise Exception(f"Failed to update relationship: {e}")
    
    async def get_book_owners(self, book_id: str) -> List[str]:
        """Get all user IDs who own a specific book."""
        query = """
        MATCH (u:User)-[:OWNS]->(b:Book)
        WHERE b.id = $book_id
        RETURN u.id
        """
        results = self.storage.execute_cypher(query, {"book_id": book_id})
        return [result['col_0'] for result in results]
    
    async def get_community_stats(self, user_id: str) -> Dict[str, Any]:
        """Get community statistics visible to the user based on privacy settings."""
        # Basic implementation - can be enhanced with privacy controls
        stats = {}
        
        # Get total books in user's library
        user_books_query = """
        MATCH (u:User)-[:OWNS]->(b:Book)
        WHERE u.id = $user_id
        RETURN COUNT(b) as total_books
        """
        results = self.storage.execute_cypher(user_books_query, {"user_id": user_id})
        stats['total_books'] = results[0]['col_0'] if results else 0
        
        # Get reading status breakdown
        status_query = """
        MATCH (u:User)-[r:OWNS]->(b:Book)
        WHERE u.id = $user_id
        RETURN r.reading_status, COUNT(*) as count
        """
        results = self.storage.execute_cypher(status_query, {"user_id": user_id})
        status_breakdown = {}
        for result in results:
            status = result['col_0']
            count = result['col_1']
            status_breakdown[status] = count
        stats['reading_status_breakdown'] = status_breakdown
        
        # Get total authors in library
        authors_query = """
        MATCH (u:User)-[:OWNS]->(b:Book)<-[:AUTHORED]-(a:Author)
        WHERE u.id = $user_id
        RETURN COUNT(DISTINCT a) as total_authors
        """
        results = self.storage.execute_cypher(authors_query, {"user_id": user_id})
        stats['total_authors'] = results[0]['col_0'] if results else 0
        
        return stats


class KuzuCustomFieldRepository(CustomFieldRepository):
    """Kuzu-based implementation of CustomFieldRepository."""
    
    def __init__(self, storage: KuzuGraphStorage):
        self.storage = storage
    
    async def create(self, field_def: CustomFieldDefinition) -> CustomFieldDefinition:
        """Create a new custom field definition."""
        if not field_def.id:
            field_def.id = str(uuid.uuid4())
        
        field_data = asdict(field_def)
        # Convert enum to string to avoid Kuzu parameter type error
        if 'field_type' in field_data and hasattr(field_data['field_type'], 'value'):
            field_data['field_type'] = field_data['field_type'].value
        
        # Safe logging
        try:
            current_app.logger.info(f"🔧 [CUSTOM_FIELD_REPO] Creating custom field '{field_def.name}' with data: {field_data}")
        except:
            print(f"🔧 [CUSTOM_FIELD_REPO] Creating custom field '{field_def.name}' with data: {field_data}")
        
        success = self.storage.store_node('CustomField', field_def.id, field_data)
        if not success:
            try:
                current_app.logger.error(f"❌ [CUSTOM_FIELD_REPO] Failed to store custom field '{field_def.name}' to database")
            except:
                print(f"❌ [CUSTOM_FIELD_REPO] Failed to store custom field '{field_def.name}' to database")
            raise Exception(f"Failed to create custom field: {field_def.name}")
        
        try:
            current_app.logger.info(f"✅ [CUSTOM_FIELD_REPO] Successfully created custom field '{field_def.name}' with id {field_def.id}")
        except:
            print(f"✅ [CUSTOM_FIELD_REPO] Successfully created custom field '{field_def.name}' with id {field_def.id}")
        return field_def
    
    async def get_by_id(self, field_id: str) -> Optional[CustomFieldDefinition]:
        """Get a custom field by ID."""
        field_data = self.storage.get_node('CustomField', field_id)
        if not field_data:
            return None
        
        field_data = _clean_custom_field_data(field_data)
        return CustomFieldDefinition(**field_data)
    
    async def get_by_user(self, user_id: str) -> List[CustomFieldDefinition]:
        """Get all custom fields for a user."""
        query = """
        MATCH (cf:CustomField)
        WHERE cf.created_by_user_id = $user_id
        RETURN cf
        """
        results = self.storage.execute_cypher(query, {"user_id": user_id})
        fields = []
        for result in results:
            field_data = _clean_custom_field_data(result['col_0'])
            fields.append(CustomFieldDefinition(**field_data))
        return fields
    
    async def update(self, field_def: CustomFieldDefinition) -> Optional[CustomFieldDefinition]:
        """Update an existing custom field."""
        if not field_def.id:
            return None
        
        field_data = asdict(field_def)
        success = self.storage.update_node('CustomField', field_def.id, field_data)
        return field_def if success else None
    
    async def delete(self, field_id: str) -> bool:
        """Delete a custom field."""
        return self.storage.delete_node('CustomField', field_id)
    
    async def get_shareable(self, exclude_user_id: Optional[str] = None) -> List[CustomFieldDefinition]:
        """Get all shareable custom field definitions."""
        query_conditions = ["cf.is_shareable = true"]
        params = {}
        
        if exclude_user_id:
            query_conditions.append("cf.created_by_user_id <> $exclude_user_id")
            params['exclude_user_id'] = exclude_user_id
        
        where_clause = " AND ".join(query_conditions)
        query = f"""
        MATCH (cf:CustomField)
        WHERE {where_clause}
        RETURN cf
        ORDER BY cf.usage_count DESC
        """
        
        results = self.storage.execute_cypher(query, params)
        fields = []
        for result in results:
            field_data = _clean_custom_field_data(result['col_0'])
            fields.append(CustomFieldDefinition(**field_data))
        return fields
    
    async def search(self, query: str, user_id: Optional[str] = None) -> List[CustomFieldDefinition]:
        """Search custom field definitions."""
        query_conditions = []
        params = {"search_query": f"%{query}%"}
        
        # Search in name and description
        query_conditions.append("(cf.name CONTAINS $search_query OR cf.description CONTAINS $search_query)")
        
        # Optionally filter by user
        if user_id:
            query_conditions.append("(cf.created_by_user_id = $user_id OR cf.is_shareable = true)")
            params['user_id'] = user_id
        else:
            # Only show shareable fields if no user specified
            query_conditions.append("cf.is_shareable = true")
        
        where_clause = " AND ".join(query_conditions)
        cypher_query = f"""
        MATCH (cf:CustomField)
        WHERE {where_clause}
        RETURN cf
        ORDER BY cf.usage_count DESC
        LIMIT 50
        """
        
        results = self.storage.execute_cypher(cypher_query, params)
        fields = []
        for result in results:
            field_data = _clean_custom_field_data(result['col_0'])
            fields.append(CustomFieldDefinition(**field_data))
        return fields
    
    async def increment_usage(self, field_id: str) -> None:
        """Increment usage count for a field definition."""
        query = """
        MATCH (cf:CustomField)
        WHERE cf.id = $field_id
        SET cf.usage_count = COALESCE(cf.usage_count, 0) + 1
        """
        try:
            self.storage.execute_cypher(query, {"field_id": field_id})
        except Exception as e:
            print(f"Warning: Could not increment usage count for field {field_id}: {e}")
    
    async def get_popular(self, limit: int = 20) -> List[CustomFieldDefinition]:
        """Get most popular shareable custom field definitions."""
        query = """
        MATCH (cf:CustomField)
        WHERE cf.is_shareable = true
        RETURN cf
        ORDER BY COALESCE(cf.usage_count, 0) DESC
        LIMIT $limit
        """
        
        results = self.storage.execute_cypher(query, {"limit": limit})
        fields = []
        for result in results:
            field_data = _clean_custom_field_data(result['col_0'])
            fields.append(CustomFieldDefinition(**field_data))
        return fields
    
    async def get_by_name(self, name: str) -> Optional[CustomFieldDefinition]:
        """Get a custom field by name."""
        query = """
        MATCH (cf:CustomField)
        WHERE cf.name = $name
        RETURN cf
        LIMIT 1
        """
        results = self.storage.execute_cypher(query, {"name": name})
        if results:
            field_data = _clean_custom_field_data(results[0]['col_0'])
            return CustomFieldDefinition(**field_data)
        return None


class KuzuImportMappingRepository(ImportMappingRepository):
    """Kuzu-based implementation of ImportMappingRepository."""
    
    def __init__(self, storage: KuzuGraphStorage):
        self.storage = storage
    
    async def create(self, template: ImportMappingTemplate) -> ImportMappingTemplate:
        """Create a new import mapping template."""
        if not template.id:
            template.id = str(uuid.uuid4())
        
        # Check if template already exists
        existing_template = await self.get_by_id(template.id)
        if existing_template:
            print(f"[KUZU_REPO][INFO] Import mapping template {template.id} already exists, returning existing template")
            return existing_template
        
        template_data = asdict(template)
        
        # Map model field names to schema field names
        if 'times_used' in template_data:
            template_data['usage_count'] = template_data.pop('times_used')
        if 'last_used' in template_data:
            template_data.pop('last_used')  # Remove last_used as it's not in the schema
        
        success = self.storage.store_node('ImportMapping', template.id, template_data)
        if not success:
            raise Exception(f"Failed to create import mapping: {template.name}")
        
        return template
    
    async def get_by_id(self, template_id: str) -> Optional[ImportMappingTemplate]:
        """Get an import mapping template by ID."""
        template_data = self.storage.get_node('ImportMapping', template_id)
        if not template_data:
            return None
        
        # Clean Kuzu data to remove internal fields
        template_data = _clean_kuzu_data(template_data)
        
        # Map schema field names to model field names
        if 'usage_count' in template_data:
            template_data['times_used'] = template_data.pop('usage_count')
        
        # Filter to only include fields that exist in ImportMappingTemplate
        template_data = _filter_model_fields(template_data, ImportMappingTemplate)
        
        if template_data.get('created_at'):
            template_data['created_at'] = _safe_fromisoformat(template_data['created_at'])
        if template_data.get('updated_at'):
            template_data['updated_at'] = _safe_fromisoformat(template_data['updated_at'])
        
        return ImportMappingTemplate(**template_data)
    
    async def get_by_user(self, user_id: str) -> List[ImportMappingTemplate]:
        """Get all import mapping templates for a user."""
        query = """
        MATCH (im:ImportMapping)
        WHERE im.user_id = $user_id
        RETURN im
        """
        results = self.storage.execute_cypher(query, {"user_id": user_id})
        templates = []
        for result in results:
            template_data = _clean_kuzu_data(result['col_0'])
            
            # Map schema field names to model field names
            if 'usage_count' in template_data:
                template_data['times_used'] = template_data.pop('usage_count')
            
            # Filter to only include fields that exist in ImportMappingTemplate
            template_data = _filter_model_fields(template_data, ImportMappingTemplate)
            
            if template_data.get('created_at'):
                template_data['created_at'] = _safe_fromisoformat(template_data['created_at'])
            if template_data.get('updated_at'):
                template_data['updated_at'] = _safe_fromisoformat(template_data['updated_at'])
            templates.append(ImportMappingTemplate(**template_data))
        return templates
    
    async def get_system_templates(self) -> List[ImportMappingTemplate]:
        """Get all system default templates."""
        query = """
        MATCH (im:ImportMapping)
        WHERE im.user_id = '__system__'
        RETURN im
        """
        results = self.storage.execute_cypher(query)
        templates = []
        for result in results:
            template_data = _clean_kuzu_data(result['col_0'])
            
            # Map schema field names to model field names
            if 'usage_count' in template_data:
                template_data['times_used'] = template_data.pop('usage_count')
            
            # Filter to only include fields that exist in ImportMappingTemplate
            template_data = _filter_model_fields(template_data, ImportMappingTemplate)
            
            if template_data.get('created_at'):
                template_data['created_at'] = _safe_fromisoformat(template_data['created_at'])
            if template_data.get('updated_at'):
                template_data['updated_at'] = _safe_fromisoformat(template_data['updated_at'])
            templates.append(ImportMappingTemplate(**template_data))
        return templates
    
    async def update(self, template: ImportMappingTemplate) -> Optional[ImportMappingTemplate]:
        """Update an existing import mapping template."""
        if not template.id:
            return None
        
        template_data = asdict(template)
        # Map model field names to schema field names
        if 'times_used' in template_data:
            template_data['usage_count'] = template_data.pop('times_used')
        if 'last_used' in template_data:
            template_data.pop('last_used')  # Remove last_used as it's not in the schema
        
        success = self.storage.update_node('ImportMapping', template.id, template_data)
        return template if success else None
    
    async def delete(self, template_id: str) -> bool:
        """Delete an import mapping template."""
        return self.storage.delete_node('ImportMapping', template_id)
    
    async def detect_template(self, headers: List[str], user_id: str) -> Optional[ImportMappingTemplate]:
        """Detect matching template based on CSV headers."""
        # Convert headers to lowercase for case-insensitive matching
        headers_lower = [h.lower().strip() for h in headers]
        
        # First, try to find user's own templates
        user_templates = await self.get_by_user(user_id)
        
        # Then get system templates
        system_templates = await self.get_system_templates()
        
        # Combine all templates, user templates first (higher priority)
        all_templates = user_templates + system_templates
        
        best_match = None
        best_score = 0
        
        for template in all_templates:
            if not template.sample_headers:
                continue
                
            # Calculate match score
            template_headers_lower = [h.lower().strip() for h in template.sample_headers]
            
            # Count exact matches
            exact_matches = len(set(headers_lower) & set(template_headers_lower))
            
            # Calculate percentage match based on template headers
            if len(template_headers_lower) > 0:
                match_score = exact_matches / len(template_headers_lower)
                
                # Require at least 70% match
                if match_score >= 0.7 and match_score > best_score:
                    best_match = template
                    best_score = match_score
        
        return best_match
    
    async def increment_usage(self, template_id: str) -> None:
        """Increment usage count and update last used timestamp."""
        query = """
        MATCH (im:ImportMapping)
        WHERE im.id = $template_id
        SET im.usage_count = COALESCE(im.usage_count, 0) + 1,
            im.updated_at = $updated_at
        """
        try:
            self.storage.execute_cypher(query, {
                "template_id": template_id,
                "updated_at": datetime.utcnow().isoformat()
            })
        except Exception as e:
            print(f"Warning: Could not increment usage count for template {template_id}: {e}")


class KuzuCategoryRepository:
    """Kuzu-based implementation of CategoryRepository."""
    
    def __init__(self, storage: KuzuGraphStorage):
        self.storage = storage
    
    async def create(self, category: Category) -> Category:
        """Create a new category."""
        if not category.id:
            category.id = str(uuid.uuid4())
        
        category_data = asdict(category)
        success = self.storage.store_node('Category', category.id, category_data)
        if not success:
            raise Exception(f"Failed to create category: {category.name}")
        
        return category
    
    async def get_by_name(self, name: str) -> Optional[Category]:
        """Get a category by name."""
        query = """
        MATCH (c:Category)
        WHERE c.name = $name OR c.normalized_name = $normalized_name
        RETURN c
        """
        normalized_name = name.lower().strip()
        results = self.storage.execute_cypher(query, {"name": name, "normalized_name": normalized_name})
        if results:
            category_data = _clean_kuzu_data(results[0]['col_0'])
            if category_data.get('created_at'):
                category_data['created_at'] = _safe_fromisoformat(category_data['created_at'])
            return Category(**category_data)
        return None
    
    async def get_by_id(self, category_id: str) -> Optional[Category]:
        """Get a category by ID."""
        category_data = self.storage.get_node('Category', category_id)
        if not category_data:
            return None
        
        category_data = _clean_kuzu_data(category_data)
        if category_data.get('created_at'):
            category_data['created_at'] = _safe_fromisoformat(category_data['created_at'])
        
        return Category(**category_data)
    
    async def search_by_name(self, name: str, limit: int = 10) -> List[Category]:
        """Search categories by name."""
        query = """
        MATCH (c:Category)
        WHERE c.name CONTAINS $name
        RETURN c
        LIMIT $limit
        """
        results = self.storage.execute_cypher(query, {"name": name, "limit": limit})
        categories = []
        for result in results:
            category_data = _clean_kuzu_data(result['col_0'])
            if category_data.get('created_at'):
                category_data['created_at'] = _safe_fromisoformat(category_data['created_at'])
            categories.append(Category(**category_data))
        return categories
    
    async def get_all(self) -> List[Category]:
        """Get all categories."""
        category_nodes = self.storage.get_nodes_by_type('Category')
        categories = []
        for category_data in category_nodes:
            # Clean the data - convert _id to id if needed
            if '_id' in category_data and 'id' not in category_data:
                category_data['id'] = category_data.pop('_id')
            
            # Remove any other unexpected fields
            expected_fields = {'id', 'name', 'normalized_name', 'parent_id', 'description', 'level', 
                              'color', 'icon', 'aliases', 'book_count', 'user_book_count', 
                              'created_at', 'updated_at'}
            cleaned_data = {k: v for k, v in category_data.items() if k in expected_fields}
            
            if cleaned_data.get('created_at'):
                cleaned_data['created_at'] = _safe_fromisoformat(cleaned_data['created_at'])
            if cleaned_data.get('updated_at'):
                cleaned_data['updated_at'] = _safe_fromisoformat(cleaned_data['updated_at'])
                
            categories.append(Category(**cleaned_data))
        return categories
    
    async def update(self, category: Category) -> Optional[Category]:
        """Update an existing category."""
        if not category.id:
            return None
        
        category_data = asdict(category)
        success = self.storage.update_node('Category', category.id, category_data)
        return category if success else None
    
    async def delete(self, category_id: str) -> bool:
        """Delete a category."""
        return self.storage.delete_node('Category', category_id)
    
    async def find_or_create(self, category_name: str, parent_id: Optional[str] = None) -> Optional[Category]:
        """Find existing category by name or create a new one."""
        # First try to find existing category
        existing_category = await self.get_by_name(category_name)
        if existing_category:
            return existing_category
        
        # Create new category if not found
        new_category = Category(
            id=str(uuid.uuid4()),
            name=category_name,
            normalized_name=category_name.lower().strip(),
            parent_id=parent_id,
            created_at=datetime.utcnow()
        )
        
        return await self.create(new_category)
    
    async def get_children(self, category_id: str) -> List[Category]:
        """Get direct children of a category."""
        query = """
        MATCH (c:Category)
        WHERE c.parent_id = $parent_id
        RETURN c
        """
        results = self.storage.execute_cypher(query, {"parent_id": category_id})
        categories = []
        for result in results:
            category_data = _clean_kuzu_data(result['col_0'])
            if category_data.get('created_at'):
                category_data['created_at'] = _safe_fromisoformat(category_data['created_at'])
            categories.append(Category(**category_data))
        return categories
    
    async def get_root_categories(self) -> List[Category]:
        """Get top-level categories (no parent)."""
        query = """
        MATCH (c:Category)
        WHERE c.parent_id IS NULL OR c.parent_id = ''
        RETURN c
        """
        results = self.storage.execute_cypher(query)
        categories = []
        for result in results:
            category_data = _clean_kuzu_data(result['col_0'])
            if category_data.get('created_at'):
                category_data['created_at'] = _safe_fromisoformat(category_data['created_at'])
            categories.append(Category(**category_data))
        return categories
    
    async def build_hierarchy_for_categories(self, categories: List[Category]) -> List[Category]:
        """Build parent-child relationships for a list of categories."""
        category_map = {cat.id: cat for cat in categories}
        
        for category in categories:
            if category.parent_id and category.parent_id in category_map:
                category.parent = category_map[category.parent_id]
                if not hasattr(category.parent, 'children'):
                    category.parent.children = []
                category.parent.children.append(category)
        
        return categories
    
    async def get_category_usage_stats(self, category_id: str, user_id: Optional[str] = None) -> Dict[str, int]:
        """Get usage statistics for a category."""
        # Count books in this category
        query = """
        MATCH (b:Book)-[:CATEGORIZED_AS]->(c:Category)
        WHERE c.id = $category_id
        RETURN COUNT(b) as total_books
        """
        results = self.storage.execute_cypher(query, {"category_id": category_id})
        total_books = results[0]['col_0'] if results else 0
        
        return {"total_books": total_books}
    
    async def increment_book_count(self, category_id: str):
        """Increment book count for a category (for statistics)."""
        # This is handled automatically by relationship counting in Kuzu
        # No need to maintain separate counters
        pass

__all__ = [
    "KuzuBookRepository",
    "KuzuUserRepository",
    "KuzuAuthorRepository",
    "KuzuUserBookRepository",
    "KuzuCustomFieldRepository",
    "KuzuImportMappingRepository",
    "KuzuCategoryRepository"
]
