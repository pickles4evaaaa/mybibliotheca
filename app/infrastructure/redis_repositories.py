"""
Redis-based repository implementations.

Concrete implementations of the domain repository interfaces using Redis as the storage backend.
"""

import uuid
import json
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, date
from dataclasses import asdict
from enum import Enum

from ..domain.models import Book, User, Author, Person, BookContribution, ContributionType, Publisher, Series, Category, UserBookRelationship, ReadingStatus, OwnershipStatus, MediaType, CustomFieldDefinition, ImportMappingTemplate, CustomFieldType
from ..domain.repositories import BookRepository, UserRepository, AuthorRepository, UserBookRepository, CustomFieldRepository, ImportMappingRepository
from .redis_graph import RedisGraphStorage


def _serialize_for_json(obj: Any) -> Any:
    """Convert objects to JSON-serializable format."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, date):
        return obj.isoformat()
    elif isinstance(obj, Enum):
        return obj.value
    elif hasattr(obj, '__dict__'):
        # Handle dataclass or object with attributes
        return {k: _serialize_for_json(v) for k, v in obj.__dict__.items()}
    elif isinstance(obj, dict):
        return {k: _serialize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_serialize_for_json(item) for item in obj]
    else:
        return obj


class RedisBookRepository(BookRepository):
    """Redis-based implementation of BookRepository."""
    
    def __init__(self, storage: RedisGraphStorage):
        self.storage = storage
        
    async def create(self, book: Book) -> Book:
        """Create a new book."""
        if not book.id:
            book.id = str(uuid.uuid4())
        
        print(f"🔍 [REPO] Creating book: {book.title}")
        print(f"📚 [REPO] Book has {len(book.authors)} authors")
        
        # First, ensure all authors exist as separate entities
        updated_contributors = []
        
        # Preserve existing non-author contributors
        for contributor in book.contributors:
            if contributor.contribution_type != ContributionType.AUTHORED:
                updated_contributors.append(contributor)
        
        # Process authors and create new AUTHORED contributions
        for author in book.authors:
            if author.name:  # Only process authors with names
                # Find or create the person (unified approach)
                existing_person = await self.find_or_create_person(author.name)
                
                # Ensure person has an ID
                if not existing_person.id:
                    print(f"❌ [REPO] Person {existing_person.name} has no ID")
                    continue
                
                # Create BookContribution
                contribution = BookContribution(
                    person_id=existing_person.id,
                    book_id=book.id or "",  # Ensure book.id is not None
                    contribution_type=ContributionType.AUTHORED,
                    person=existing_person
                )
                updated_contributors.append(contribution)
            else:
                print(f"⚠️ [REPO] Skipping author with no name: {author}")
        
        # Update the book's contributors
        from dataclasses import replace
        book = replace(book, contributors=updated_contributors)
        print(f"📚 [REPO] Updated book with {len(book.authors)} processed authors")
        
        # Ensure book has an ID before proceeding
        if not book.id:
            book.id = str(uuid.uuid4())
            
        book_data = asdict(book)
        # Handle nested objects
        book_data['authors'] = [asdict(author) for author in book.authors]
        if book.publisher:
            book_data['publisher'] = asdict(book.publisher)
        if book.series:
            book_data['series'] = asdict(book.series)
        
        # Handle categories - they might be raw data or Category instances
        if book.categories:
            serialized_categories = []
            for category in book.categories:
                if hasattr(category, '__dataclass_fields__'):  # It's a dataclass
                    serialized_categories.append(asdict(category))
                else:  # It's raw data (string or dict)
                    if isinstance(category, str):
                        # Store as simple dict for raw string categories
                        serialized_categories.append({'name': category})
                    elif isinstance(category, dict):
                        serialized_categories.append(category)
                    else:
                        # Convert to string as fallback
                        serialized_categories.append({'name': str(category)})
            book_data['categories'] = serialized_categories
        else:
            book_data['categories'] = []
        
        # Serialize datetime objects for JSON storage
        book_data = _serialize_for_json(book_data)
        
        print(f"💾 [REPO] Storing book {book.title}")
        success = self.storage.store_node('book', book.id, book_data)
        if not success:
            raise Exception(f"Failed to create book {book.id}")
        print(f"✅ [REPO] Book {book.title} stored successfully")
            
        # Create relationships - now using person nodes for all contributors
        for contributor in book.contributors:
            if contributor.person and contributor.person.id:
                print(f"🔗 [REPO] Creating relationship: book {book.id} -> person {contributor.person.id} ({contributor.person.name}) as {contributor.contribution_type.value}")
                # Create relationship with contribution type information
                relationship_properties = {
                    'contribution_type': contributor.contribution_type.value,
                    'order': contributor.order or 0
                }
                self.storage.create_relationship('book', book.id, 'WRITTEN_BY', 'person', contributor.person.id, relationship_properties)
                
        if book.publisher and book.publisher.id:
            self.storage.create_relationship('book', book.id, 'PUBLISHED_BY', 'publisher', book.publisher.id)
            
        if book.series and book.series.id:
            properties = {}
            if book.series_volume:
                properties['volume'] = book.series_volume
            if book.series_order:
                properties['order'] = book.series_order
            self.storage.create_relationship('book', book.id, 'PART_OF_SERIES', 'series', book.series.id, properties)
            
        # Note: Category relationships are handled separately via process_book_categories
        # This avoids issues with raw category data vs Category objects
        print(f"📚 [REPO] Skipping direct category relationship creation - will be handled by process_book_categories")
        
        return book
    
    async def get_by_id(self, book_id: str) -> Optional[Book]:
        """Get a book by ID with contributors loaded from relationships."""
        book_data = self.storage.get_node('book', book_id)
        if not book_data:
            return None
        
        # Load contributors from relationships
        try:
            print(f"🔍 [REPO] Loading contributors for book {book_id}")
            # Test if storage is working
            try:
                test_key = f"rel:book:{book_id}:WRITTEN_BY"
                test_exists = self.storage.redis.exists(test_key)
                print(f"🧪 [REPO] Test key '{test_key}' exists: {test_exists}")
                if test_exists:
                    test_members = self.storage.redis.smembers(test_key)
                    print(f"🧪 [REPO] Test key members: {test_members}")
            except Exception as e:
                print(f"❌ [REPO] Error testing storage: {e}")
            
            # Get all WRITTEN_BY relationships for this book
            relationships = self.storage.get_relationships('book', book_id, 'WRITTEN_BY')
            print(f"📊 [REPO] Found {len(relationships) if relationships else 0} WRITTEN_BY relationships")
            contributors = []
            
            for rel in relationships:
                print(f"🔗 [REPO] Processing relationship: {rel}")
                target_id = rel.get('to_id')  # Fixed: Redis uses 'to_id' not 'target_id'
                if target_id:
                    print(f"👤 [REPO] Loading person {target_id}")
                    # Get the person data
                    person_data = self.storage.get_node('person', target_id)
                    if person_data:
                        print(f"✅ [REPO] Found person data for {target_id}: {person_data.get('name', 'Unknown')}")
                        
                        # Filter person_data to only include fields expected by Person model
                        valid_fields = {
                            'id', 'name', 'normalized_name', 'birth_year', 'death_year', 
                            'birth_place', 'bio', 'website', 'created_at', 'updated_at'
                        }
                        
                        # Create clean person data
                        clean_person_data = {}
                        for field in valid_fields:
                            if field in person_data:
                                clean_person_data[field] = person_data[field]
                        
                        # Map _id to id if needed
                        if 'id' not in clean_person_data and '_id' in person_data:
                            clean_person_data['id'] = person_data['_id']
                        
                        # Convert datetime strings
                        if 'created_at' in clean_person_data and isinstance(clean_person_data['created_at'], str):
                            try:
                                clean_person_data['created_at'] = datetime.fromisoformat(clean_person_data['created_at'])
                            except:
                                clean_person_data['created_at'] = datetime.utcnow()
                                
                        if 'updated_at' in clean_person_data and isinstance(clean_person_data['updated_at'], str):
                            try:
                                clean_person_data['updated_at'] = datetime.fromisoformat(clean_person_data['updated_at'])
                            except:
                                clean_person_data['updated_at'] = datetime.utcnow()
                        
                        person = Person(**clean_person_data)
                        
                        # Get contribution type from relationship properties
                        contribution_type = ContributionType.AUTHORED  # Default
                        rel_props = rel.get('properties', {})
                        if 'contribution_type' in rel_props:
                            try:
                                contribution_type = ContributionType(rel_props['contribution_type'])
                            except:
                                contribution_type = ContributionType.AUTHORED
                        
                        contribution = BookContribution(
                            person=person,
                            contribution_type=contribution_type,
                            created_at=datetime.utcnow()
                        )
                        contributors.append(contribution)
                        print(f"📚 [REPO] Added contributor: {person.name} ({contribution_type.value})")
                    else:
                        print(f"❌ [REPO] No person data found for {target_id}")
                else:
                    print(f"⚠️ [REPO] Relationship missing to_id: {rel}")
            
            # Add contributors to book_data
            if contributors:
                print(f"📋 [REPO] Adding {len(contributors)} contributors to book data")
                book_data['contributors'] = []
                for contrib in contributors:
                    contrib_data = {
                        'person': {
                            'id': contrib.person.id,
                            'name': contrib.person.name,
                            'normalized_name': contrib.person.normalized_name,
                            'birth_year': contrib.person.birth_year,
                            'death_year': contrib.person.death_year,
                            'birth_place': contrib.person.birth_place,
                            'bio': contrib.person.bio,
                            'website': contrib.person.website,
                            'created_at': contrib.person.created_at,
                            'updated_at': contrib.person.updated_at
                        },
                        'contribution_type': contrib.contribution_type.value,
                        'created_at': contrib.created_at
                    }
                    book_data['contributors'].append(contrib_data)
            else:
                print(f"⚪ [REPO] No contributors found for book {book_id}")
                    
        except Exception as e:
            print(f"⚠️ [REPO] Error loading contributors for book {book_id}: {e}")
            import traceback
            traceback.print_exc()
            
        return self._data_to_book(book_data)
    
    async def get_by_isbn(self, isbn: str) -> Optional[Book]:
        """Get a book by ISBN (13 or 10)."""
        # Search for books with matching ISBN
        search_results = self.storage.search_nodes('book', {'isbn13': isbn})
        if not search_results:
            search_results = self.storage.search_nodes('book', {'isbn10': isbn})
            
        if search_results:
            return self._data_to_book(search_results[0])
        return None
    
    async def get_books_by_isbn(self, isbn: str) -> List[Book]:
        """Get books by ISBN (searches globally, not user-specific)."""
        try:
            # Clean the ISBN
            clean_isbn = isbn.replace('-', '').replace(' ', '')
            
            # Get all books from Redis and filter by ISBN
            all_book_keys = self.storage.redis.keys('book:*')
            matching_books = []
            
            for key in all_book_keys:
                try:
                    book_data = self.storage.redis.get(key)
                    if book_data:
                        book_dict = json.loads(book_data)
                        
                        # Check ISBN13 and ISBN10
                        book_isbn13 = book_dict.get('isbn13', '').replace('-', '').replace(' ', '')
                        book_isbn10 = book_dict.get('isbn10', '').replace('-', '').replace(' ', '')
                        
                        if clean_isbn == book_isbn13 or clean_isbn == book_isbn10:
                            book = self._data_to_book(book_dict)
                            matching_books.append(book)
                except Exception as e:
                    print(f"Error processing book key {key}: {e}")
                    continue
            
            return matching_books
        except Exception as e:
            print(f"Error searching books by ISBN: {e}")
            return []

    async def find_duplicates(self, book: Book) -> List[Tuple[Book, float]]:
        """Find potential duplicate books with confidence scores."""
        duplicates = []
        
        # Primary match: ISBN
        if book.isbn13:
            existing = await self.get_by_isbn(book.isbn13)
            if existing and existing.id != book.id:
                duplicates.append((existing, 1.0))  # 100% confidence
                
        if book.isbn10:
            existing = await self.get_by_isbn(book.isbn10)
            if existing and existing.id != book.id:
                duplicates.append((existing, 1.0))  # 100% confidence
        
        # Secondary match: Title + Author (fuzzy)
        if book.title and book.authors:
            # Simple implementation - in production we'd use more sophisticated matching
            all_books = self.storage.find_nodes_by_type('book')
            for book_data in all_books:
                candidate = self._data_to_book(book_data)
                if candidate.id == book.id:
                    continue
                    
                # Simple title similarity
                title_similarity = self._calculate_similarity(book.normalized_title, candidate.normalized_title)
                
                # Check author overlap
                book_authors = {author.normalized_name for author in book.authors}
                candidate_authors = {author.normalized_name for author in candidate.authors}
                author_overlap = len(book_authors & candidate_authors) / max(len(book_authors), len(candidate_authors), 1)
                
                # Combined confidence
                confidence = (title_similarity + author_overlap) / 2
                
                if confidence > 0.8:  # 80% threshold
                    duplicates.append((candidate, confidence))
        
        # Sort by confidence
        duplicates.sort(key=lambda x: x[1], reverse=True)
        return duplicates
    
    async def search(self, query: str, filters: Dict[str, Any] = None) -> List[Book]:
        """Search books with optional filters."""
        # Simple implementation - search in title and description
        search_fields = {}
        
        # Add filters if provided
        if filters:
            search_fields.update(filters)
            
        # Get all books and filter (in production, we'd use proper search indexing)
        all_books = self.storage.find_nodes_by_type('book')
        results = []
        
        for book_data in all_books:
            book = self._data_to_book(book_data)
            
            # Apply filters
            if filters:
                filter_match = True
                for key, value in filters.items():
                    if hasattr(book, key) and getattr(book, key) != value:
                        filter_match = False
                        break
                if not filter_match:
                    continue
            
            # Apply text search
            if query:
                searchable_text = f"{book.title} {book.description or ''}"
                for author in book.authors:
                    searchable_text += f" {author.name}"
                
                if query.lower() in searchable_text.lower():
                    results.append(book)
            else:
                results.append(book)
                
        return results
    
    async def update(self, book: Book) -> Book:
        """Update an existing book."""
        if not book.id:
            raise ValueError("Book must have an ID to update")
            
        # Use the serialization function to properly handle datetime objects
        book_data = _serialize_for_json(asdict(book))
        
        success = self.storage.update_node('book', book.id, book_data)
        if not success:
            raise Exception(f"Failed to update book {book.id}")
            
        return book
    
    async def delete(self, book_id: str) -> bool:
        """Delete a book (admin only)."""
        return self.storage.delete_node('book', book_id)
    
    async def get_books_for_user(self, user_id: str, limit: int = 50, offset: int = 0) -> List[Book]:
        """Get all books for a user with their relationship data."""
        # Get all user-book relationships for this user
        relationships = self.storage.get_relationships('user', user_id, 'owns')
        
        books = []
        for rel in relationships[offset:offset + limit]:
            book_id = rel['to_id']
            book = await self.get_by_id(book_id)
            if book:
                # Add user relationship attributes to the book object
                rel_props = rel.get('properties', {})
                book.reading_status = rel_props.get('reading_status', 'plan_to_read')
                book.ownership_status = rel_props.get('ownership_status', 'owned')
                book.user_rating = rel_props.get('user_rating')
                book.personal_notes = rel_props.get('personal_notes')
                book.date_added = rel_props.get('date_added')
                book.want_to_read = rel_props.get('reading_status') == 'plan_to_read'
                book.library_only = rel_props.get('reading_status') == 'library_only'
                book.uid = book.id  # Ensure uid is available
                
                # Handle date fields
                if rel_props.get('start_date'):
                    try:
                        book.start_date = datetime.fromisoformat(rel_props['start_date']).date()
                    except:
                        book.start_date = None
                else:
                    book.start_date = None
                    
                if rel_props.get('finish_date'):
                    try:
                        book.finish_date = datetime.fromisoformat(rel_props['finish_date']).date()
                    except:
                        book.finish_date = None
                else:
                    book.finish_date = None
                
                books.append(book)
        
        return books
    
    async def merge_books(self, source_id: str, target_id: str) -> Book:
        """Merge two duplicate book records (admin only)."""
        # Get both books
        source_book = await self.get_by_id(source_id)
        target_book = await self.get_by_id(target_id)
        
        if not source_book or not target_book:
            raise ValueError("Both books must exist to merge")
        
        # Merge data (target wins, but fill in missing fields from source)
        if not target_book.description and source_book.description:
            target_book.description = source_book.description
        if not target_book.cover_url and source_book.cover_url:
            target_book.cover_url = source_book.cover_url
        if not target_book.isbn13 and source_book.isbn13:
            target_book.isbn13 = source_book.isbn13
        if not target_book.isbn10 and source_book.isbn10:
            target_book.isbn10 = source_book.isbn10
            
        # Update target book
        await self.update(target_book)
        
        # TODO: Move all user relationships from source to target
        # For now, just delete source
        await self.delete(source_id)
        
        return target_book
    
    async def find_or_create_person(self, person_name: str) -> Person:
        """Find existing person by name or create a new one."""
        try:
            print(f"🔍 [REPO] Looking for existing person: {person_name}")
            
            # Search for existing person by name
            normalized_name = person_name.strip().lower()
            
            # Get all persons and check for matches
            all_persons = self.storage.find_nodes_by_type('person')
            for person_data in all_persons:
                existing_name = person_data.get('name', '').strip().lower()
                existing_normalized = person_data.get('normalized_name', '').strip().lower()
                
                if existing_name == normalized_name or existing_normalized == normalized_name:
                    print(f"✅ [REPO] Found existing person: {person_data.get('name')} (ID: {person_data.get('_id')})")
                    # Convert back to Person object
                    from ..domain.models import Person
                    person = Person(
                        id=person_data.get('_id'),
                        name=person_data.get('name', ''),
                        normalized_name=person_data.get('normalized_name', ''),
                        birth_year=person_data.get('birth_year'),
                        death_year=person_data.get('death_year'),
                        birth_place=person_data.get('birth_place'),
                        bio=person_data.get('bio'),
                        website=person_data.get('website'),
                        created_at=datetime.utcnow(),  # Set defaults for dates
                        updated_at=datetime.utcnow()
                    )
                    return person
            
            # If not found, create new person
            print(f"📝 [REPO] Creating new person: {person_name}")
            from ..domain.models import Person
            person = Person(
                id=str(uuid.uuid4()),
                name=person_name,
                normalized_name=normalized_name,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            # Store the new person
            person_data = {
                '_id': person.id,
                'name': person.name,
                'normalized_name': person.normalized_name,
                'birth_year': person.birth_year,
                'death_year': person.death_year,
                'birth_place': person.birth_place,
                'bio': person.bio,
                'website': person.website,
                'created_at': person.created_at.isoformat(),
                'updated_at': person.updated_at.isoformat()
            }
            
            success = self.storage.store_node('person', person.id, person_data)
            if success:
                print(f"✅ [REPO] New person created: {person.name} (ID: {person.id})")
                return person
            else:
                raise Exception(f"Failed to create person {person_name}")
                
        except Exception as e:
            print(f"❌ [REPO] Error finding/creating person {person_name}: {e}")
            import traceback
            traceback.print_exc()
            raise

    def _data_to_book(self, data: Dict[str, Any]) -> Book:
        """Convert Redis data to Book domain model."""
        # Handle nested objects
        contributors = []
        
        # Handle new contributor structure
        if 'contributors' in data and data['contributors']:
            for contrib_data in data['contributors']:
                contrib_data_copy = contrib_data.copy()
                # Handle datetime fields
                if 'created_at' in contrib_data_copy and isinstance(contrib_data_copy['created_at'], str):
                    try:
                        contrib_data_copy['created_at'] = datetime.fromisoformat(contrib_data_copy['created_at'])
                    except:
                        contrib_data_copy['created_at'] = datetime.utcnow()
                        
                # Handle person data within contribution
                person_data = contrib_data_copy.pop('person', {})
                if 'created_at' in person_data and isinstance(person_data['created_at'], str):
                    try:
                        person_data['created_at'] = datetime.fromisoformat(person_data['created_at'])
                    except:
                        person_data['created_at'] = datetime.utcnow()
                        
                person = Person(**person_data)
                contrib_data_copy['person'] = person
                
                # Handle contribution_type
                if 'contribution_type' in contrib_data_copy and isinstance(contrib_data_copy['contribution_type'], str):
                    contrib_data_copy['contribution_type'] = ContributionType(contrib_data_copy['contribution_type'])
                    
                contributors.append(BookContribution(**contrib_data_copy))
        
        # Handle legacy authors structure for backward compatibility
        elif 'authors' in data and data['authors']:
            for author_data in data['authors']:
                # Convert legacy author to Person and BookContribution
                person_data = {
                    'id': author_data.get('id', str(uuid.uuid4())),
                    'name': author_data.get('name', ''),
                    'bio': author_data.get('bio', ''),
                    'image_url': author_data.get('image_url', ''),
                    'created_at': datetime.utcnow()
                }
                
                if 'created_at' in author_data and isinstance(author_data['created_at'], str):
                    try:
                        person_data['created_at'] = datetime.fromisoformat(author_data['created_at'])
                    except:
                        person_data['created_at'] = datetime.utcnow()
                        
                person = Person(**person_data)
                contribution = BookContribution(
                    person=person,
                    contribution_type=ContributionType.AUTHORED,
                    created_at=person_data['created_at']
                )
                contributors.append(contribution)
                
        publisher = None
        if 'publisher' in data and data['publisher']:
            publisher_data = data['publisher'].copy()
            # Handle datetime field for publisher
            if 'created_at' in publisher_data and isinstance(publisher_data['created_at'], str):
                try:
                    publisher_data['created_at'] = datetime.fromisoformat(publisher_data['created_at'])
                except:
                    publisher_data['created_at'] = datetime.utcnow()
            publisher = Publisher(**publisher_data)
            
        series = None
        if 'series' in data and data['series']:
            series = Series(**data['series'])
            
        categories = []
        if 'categories' in data and data['categories']:
            for category_data in data['categories']:
                categories.append(Category(**category_data))
        
        # Remove nested objects and metadata fields from data for Book creation
        book_data = data.copy()
        book_data.pop('authors', None)  # Remove legacy authors
        book_data.pop('contributors', None)  # Remove contributors (will be set after)
        book_data.pop('publisher', None)
        book_data.pop('series', None)
        book_data.pop('categories', None)
        # Remove Redis metadata fields
        book_data.pop('_type', None)
        book_data.pop('_id', None)
        book_data.pop('_created_at', None)
        book_data.pop('_updated_at', None)
        
        # Handle datetime fields
        if 'published_date' in book_data and isinstance(book_data['published_date'], str):
            try:
                # Convert to date object (not datetime) for published_date
                parsed_date = datetime.fromisoformat(book_data['published_date'])
                book_data['published_date'] = parsed_date.date()
            except:
                book_data['published_date'] = None
                
        if 'created_at' in book_data and isinstance(book_data['created_at'], str):
            try:
                book_data['created_at'] = datetime.fromisoformat(book_data['created_at'])
            except:
                book_data['created_at'] = datetime.utcnow()
                
        if 'updated_at' in book_data and isinstance(book_data['updated_at'], str):
            try:
                book_data['updated_at'] = datetime.fromisoformat(book_data['updated_at'])
            except:
                book_data['updated_at'] = datetime.utcnow()
        
        book = Book(**book_data)
        book.contributors = contributors
        book.publisher = publisher
        book.series = series
        book.categories = categories
        
        return book
    
    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculate simple text similarity (Jaccard similarity)."""
        if not text1 or not text2:
            return 0.0
            
        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())
        
        intersection = words1 & words2
        union = words1 | words2
        
        if not union:
            return 0.0
            
        return len(intersection) / len(union)


class RedisUserRepository(UserRepository):
    """Redis-based implementation of UserRepository."""
    
    def __init__(self, storage: RedisGraphStorage):
        self.storage = storage
        
    async def create(self, user: User) -> User:
        """Create a new user."""
        if not user.id:
            user.id = str(uuid.uuid4())
            
        user_data = asdict(user)
        # Serialize datetime objects for JSON storage
        user_data = _serialize_for_json(user_data)
        
        success = self.storage.store_node('user', user.id, user_data)
        if not success:
            raise Exception(f"Failed to create user {user.id}")
            
        return user
    
    async def get_by_id(self, user_id: str) -> Optional[User]:
        """Get a user by ID."""
        user_data = self.storage.get_node('user', user_id)
        if not user_data:
            return None
            
        return self._data_to_user(user_data)
    
    async def get_by_username(self, username: str) -> Optional[User]:
        """Get a user by username."""
        search_results = self.storage.search_nodes('user', {'username': username})
        if search_results:
            return self._data_to_user(search_results[0])
        return None
    
    async def get_by_email(self, email: str) -> Optional[User]:
        """Get a user by email."""
        search_results = self.storage.search_nodes('user', {'email': email})
        if search_results:
            return self._data_to_user(search_results[0])
        return None
    
    async def update(self, user: User) -> User:
        """Update an existing user."""
        if not user.id:
            raise ValueError("User must have an ID to update")
            
        user_data = asdict(user)
        # Serialize datetime objects for JSON storage
        user_data = _serialize_for_json(user_data)
        
        success = self.storage.update_node('user', user.id, user_data)
        if not success:
            raise Exception(f"Failed to update user {user.id}")
            
        return user
    
    async def delete(self, user_id: str) -> bool:
        """Delete a user."""
        return self.storage.delete_node('user', user_id)
    
    async def list_all(self, limit: int = 100, offset: int = 0) -> List[User]:
        """List all users (admin only)."""
        user_data_list = self.storage.find_nodes_by_type('user', limit, offset)
        return [self._data_to_user(data) for data in user_data_list]
    
    async def get_all(self) -> List[User]:
        """Get all users (alias for list_all with no limits)."""
        user_data_list = self.storage.find_nodes_by_type('user', limit=1000, offset=0)
        return [self._data_to_user(data) for data in user_data_list]
    
    def _data_to_user(self, data: Dict[str, Any]) -> User:
        """Convert Redis data to User domain model."""
        # Handle datetime fields
        user_data = data.copy()
        # Remove Redis metadata fields
        user_data.pop('_type', None)
        user_data.pop('_id', None)
        user_data.pop('_created_at', None)
        user_data.pop('_updated_at', None)
        
        if 'created_at' in user_data and isinstance(user_data['created_at'], str):
            try:
                user_data['created_at'] = datetime.fromisoformat(user_data['created_at'])
            except:
                user_data['created_at'] = datetime.utcnow()
                
        if 'last_login' in user_data and isinstance(user_data['last_login'], str):
            try:
                user_data['last_login'] = datetime.fromisoformat(user_data['last_login'])
            except:
                user_data['last_login'] = None
        
        return User(**user_data)


class RedisAuthorRepository(AuthorRepository):
    """Redis-based implementation of AuthorRepository."""
    
    def __init__(self, storage: RedisGraphStorage):
        self.storage = storage
        
    async def create(self, author: Author) -> Author:
        """Create a new author."""
        if not author.id:
            author.id = str(uuid.uuid4())
            
        author_data = asdict(author)
        # Serialize datetime objects for JSON storage
        author_data = _serialize_for_json(author_data)
        success = self.storage.store_node('author', author.id, author_data)
        if not success:
            raise Exception(f"Failed to create author {author.id}")
            
        return author
    
    async def get_by_id(self, author_id: str) -> Optional[Author]:
        """Get an author by ID."""
        author_data = self.storage.get_node('author', author_id)
        if not author_data:
            return None
            
        return self._data_to_author(author_data)
    
    async def find_by_name(self, name: str) -> List[Author]:
        """Find authors by name (fuzzy matching)."""
        search_results = self.storage.search_nodes('author', {'name': name})
        return [self._data_to_author(data) for data in search_results]
    
    async def update(self, author: Author) -> Author:
        """Update an existing author."""
        if not author.id:
            raise ValueError("Author must have an ID to update")
            
        author_data = _serialize_for_json(asdict(author))
        success = self.storage.update_node('author', author.id, author_data)
        if not success:
            raise Exception(f"Failed to update author {author.id}")
            
        return author
    
    async def get_collaborators(self, author_id: str) -> List[Author]:
        """Get authors who have collaborated with this author."""
        # Get all COLLABORATED_WITH relationships
        relationships = self.storage.get_relationships('author', author_id, 'COLLABORATED_WITH')
        
        collaborators = []
        for rel in relationships:
            if rel['to_type'] == 'author':
                collaborator_data = self.storage.get_node('author', rel['to_id'])
                if collaborator_data:
                    collaborators.append(self._data_to_author(collaborator_data))
                    
        return collaborators
    
    def _data_to_author(self, data: Dict[str, Any]) -> Author:
        """Convert Redis data to Author domain model."""
        author_data = data.copy()
        # Remove Redis metadata fields
        author_data.pop('_type', None)
        author_data.pop('_id', None)
        author_data.pop('_created_at', None)
        author_data.pop('_updated_at', None)
        
        if 'created_at' in author_data and isinstance(author_data['created_at'], str):
            try:
                author_data['created_at'] = datetime.fromisoformat(author_data['created_at'])
            except:
                author_data['created_at'] = datetime.utcnow()
        
        return Author(**author_data)


class RedisUserBookRepository(UserBookRepository):
    """Redis-based implementation of UserBookRepository."""
    
    def __init__(self, storage: RedisGraphStorage):
        self.storage = storage
        
    async def create_relationship(self, relationship: UserBookRelationship) -> UserBookRelationship:
        """Create a user-book relationship."""
        # Store as a graph relationship with properties
        relationship_data = {
            'reading_status': relationship.reading_status.value,
            'ownership_status': relationship.ownership_status.value,
            'date_added': relationship.date_added.isoformat(),
            'start_date': relationship.start_date.isoformat() if relationship.start_date else None,
            'finish_date': relationship.finish_date.isoformat() if relationship.finish_date else None,
            'user_rating': relationship.user_rating,
            'personal_notes': relationship.personal_notes,
            'media_type': relationship.media_type.value if hasattr(relationship, 'media_type') else 'physical',
            'locations': relationship.locations,
            'user_tags': relationship.user_tags,
            'source': relationship.source,
            'custom_metadata': relationship.custom_metadata,  # ✅ ADD THIS LINE
            'created_at': relationship.created_at.isoformat(),
            'updated_at': relationship.updated_at.isoformat()
        }
        
        print(f"🔍 [REPO] Storing relationship with custom_metadata: {relationship.custom_metadata}")
        
        # Create relationship in Redis graph
        success = self.storage.create_relationship(
            from_type='user',
            from_id=relationship.user_id,
            relationship='owns',
            to_type='book',
            to_id=relationship.book_id,
            properties=relationship_data
        )
        
        if success:
            print(f"✅ [REPO] Relationship stored successfully")
            return relationship
        else:
            raise Exception(f"Failed to create user-book relationship for user {relationship.user_id} and book {relationship.book_id}")
    
    async def get_relationship(self, user_id: str, book_id: str) -> Optional[UserBookRelationship]:
        """Get a specific user-book relationship."""
        relationships = self.storage.get_relationships('user', user_id, 'owns')
        
        for rel in relationships:
            if rel['to_id'] == book_id:
                return self._data_to_relationship(user_id, book_id, rel['properties'])
        
        return None
    
    async def get_user_library(self, user_id: str, filters: Dict[str, Any] = None) -> List[UserBookRelationship]:
        """Get a user's library with optional filters."""
        relationships = self.storage.get_relationships('user', user_id, 'owns')
        
        user_books = []
        for rel in relationships:
            book_id = rel['to_id']
            relationship = self._data_to_relationship(user_id, book_id, rel['properties'])
            if relationship:
                # Apply filters if provided
                if filters:
                    if not self._matches_filters(relationship, filters):
                        continue
                user_books.append(relationship)
        
        return user_books
    
    async def update_relationship(self, relationship: UserBookRelationship) -> UserBookRelationship:
        """Update a user-book relationship."""
        # Delete old relationship
        await self.delete_relationship(relationship.user_id, relationship.book_id)
        
        # Create new relationship with updated data
        return await self.create_relationship(relationship)
    
    async def delete_relationship(self, user_id: str, book_id: str) -> bool:
        """Delete a user-book relationship."""
        return self.storage.delete_relationship('user', user_id, 'owns', 'book', book_id)
    
    async def get_book_owners(self, book_id: str) -> List[str]:
        """Get all user IDs who own a specific book."""
        # Get reverse relationships
        relationships = self.storage.get_relationships('book', book_id)
        
        user_ids = []
        for rel in relationships:
            if rel['relationship'] == 'owns' and rel.get('from_type') == 'user':
                user_ids.append(rel['from_id'])
        
        return user_ids
    
    async def get_community_stats(self, user_id: str) -> Dict[str, Any]:
        """Get community statistics visible to the user based on privacy settings."""
        # Placeholder implementation - can be enhanced later
        return {
            'total_books_in_community': 0,
            'shared_books': 0,
            'reading_goals_met': 0
        }
    
    def _data_to_relationship(self, user_id: str, book_id: str, data: Dict[str, Any]) -> UserBookRelationship:
        """Convert stored data to UserBookRelationship object."""
        from datetime import datetime
        
        # Parse dates
        date_added = datetime.fromisoformat(data['date_added']) if data.get('date_added') else datetime.utcnow()
        start_date = datetime.fromisoformat(data['start_date']) if data.get('start_date') else None
        finish_date = datetime.fromisoformat(data['finish_date']) if data.get('finish_date') else None
        created_at = datetime.fromisoformat(data['created_at']) if data.get('created_at') else datetime.utcnow()
        updated_at = datetime.fromisoformat(data['updated_at']) if data.get('updated_at') else datetime.utcnow()
        
        # Parse enums
        reading_status = ReadingStatus(data.get('reading_status', 'plan_to_read'))
        ownership_status = OwnershipStatus(data.get('ownership_status', 'owned'))
        
        # Get custom metadata
        custom_metadata = data.get('custom_metadata', {})
        print(f"🔍 [REPO] Loading relationship custom_metadata from storage: {custom_metadata}")
        
        return UserBookRelationship(
            user_id=user_id,
            book_id=book_id,
            reading_status=reading_status,
            ownership_status=ownership_status,
            date_added=date_added,
            start_date=start_date,
            finish_date=finish_date,
            user_rating=data.get('user_rating'),
            personal_notes=data.get('personal_notes'),
            locations=data.get('locations', []),
            user_tags=data.get('user_tags', []),
            source=data.get('source', 'manual'),
            custom_metadata=custom_metadata,  # ✅ ADD THIS LINE
            created_at=created_at,
            updated_at=updated_at
        )
    
    def _matches_filters(self, relationship: UserBookRelationship, filters: Dict[str, Any]) -> bool:
        """Check if relationship matches the provided filters."""
        for key, value in filters.items():
            if key == 'reading_status':
                if relationship.reading_status.value != value:
                    return False
            elif key == 'ownership_status':
                if relationship.ownership_status.value != value:
                    return False
            # Add more filter criteria as needed
        
        return True


class RedisCustomFieldRepository(CustomFieldRepository):
    """Redis-based implementation of CustomFieldRepository."""
    
    def __init__(self, storage: RedisGraphStorage):
        self.storage = storage
    
    async def create(self, field_def: CustomFieldDefinition) -> CustomFieldDefinition:
        """Create a new custom field definition."""
        if not field_def.id:
            field_def.id = str(uuid.uuid4())
        
        print(f"🔍 [CUSTOM_FIELD_REPO] Creating field: {field_def.name}")
        print(f"   📊 Field properties: is_global={field_def.is_global}, is_shareable={field_def.is_shareable}, created_by={field_def.created_by_user_id}")
        
        field_data = asdict(field_def)
        field_data = _serialize_for_json(field_data)
        
        success = self.storage.store_node('custom_field', field_def.id, field_data)
        if not success:
            raise Exception(f"Failed to create custom field {field_def.id}")
        
        print(f"✅ [CUSTOM_FIELD_REPO] Field {field_def.name} created successfully with ID {field_def.id}")
        return field_def
    
    async def get_by_id(self, field_id: str) -> Optional[CustomFieldDefinition]:
        """Get a custom field definition by ID."""
        field_data = self.storage.get_node('custom_field', field_id)
        if not field_data:
            return None
        
        return self._data_to_custom_field(field_data)
    
    async def get_by_user(self, user_id: str) -> List[CustomFieldDefinition]:
        """Get all custom field definitions created by a user."""
        print(f"🔍 [CUSTOM_FIELD_REPO] Getting fields for user {user_id}")
        search_results = self.storage.search_nodes('custom_field', {'created_by_user_id': user_id})
        fields = [self._data_to_custom_field(data) for data in search_results]
        print(f"📋 [CUSTOM_FIELD_REPO] Found {len(fields)} user-specific fields:")
        for field in fields:
            print(f"   👤 {field.name} (is_global: {field.is_global}, is_shareable: {field.is_shareable}, created_by: {field.created_by_user_id})")
        return fields
    
    async def get_shareable(self, exclude_user_id: Optional[str] = None) -> List[CustomFieldDefinition]:
        """Get all shareable custom field definitions."""
        print(f"🔍 [CUSTOM_FIELD_REPO] Getting shareable fields (exclude_user_id: {exclude_user_id})")
        search_results = self.storage.search_nodes('custom_field', {'is_shareable': True})
        fields = [self._data_to_custom_field(data) for data in search_results]
        print(f"📋 [CUSTOM_FIELD_REPO] Found {len(fields)} shareable fields before filtering:")
        for field in fields:
            print(f"   🌐 {field.name} (is_global: {field.is_global}, is_shareable: {field.is_shareable}, created_by: {field.created_by_user_id})")
        
        if exclude_user_id:
            original_count = len(fields)
            fields = [f for f in fields if f.created_by_user_id != exclude_user_id]
            print(f"📋 [CUSTOM_FIELD_REPO] After excluding user {exclude_user_id}: {len(fields)} fields (removed {original_count - len(fields)})")
        
        sorted_fields = sorted(fields, key=lambda x: x.usage_count, reverse=True)
        return sorted_fields
    
    async def search(self, query: str, user_id: Optional[str] = None) -> List[CustomFieldDefinition]:
        """Search custom field definitions."""
        # For now, simple text matching - could be enhanced with fuzzy search
        all_fields = []
        
        if user_id:
            user_fields = await self.get_by_user(user_id)
            all_fields.extend(user_fields)
        
        shareable_fields = await self.get_shareable(exclude_user_id=user_id)
        all_fields.extend(shareable_fields)
        
        query_lower = query.lower()
        matching_fields = [
            field for field in all_fields
            if query_lower in field.name.lower() or 
               query_lower in field.display_name.lower() or
               (field.description and query_lower in field.description.lower())
        ]
        
        return matching_fields
    
    async def update(self, field_def: CustomFieldDefinition) -> CustomFieldDefinition:
        """Update an existing custom field definition."""
        field_def.updated_at = datetime.utcnow()
        field_data = asdict(field_def)
        field_data = _serialize_for_json(field_data)
        
        success = self.storage.store_node('custom_field', field_def.id, field_data)
        if not success:
            raise Exception(f"Failed to update custom field {field_def.id}")
        
        return field_def
    
    async def delete(self, field_id: str) -> bool:
        """Delete a custom field definition."""
        return self.storage.delete_node('custom_field', field_id)
    
    async def increment_usage(self, field_id: str) -> None:
        """Increment usage count for a field definition."""
        field_def = await self.get_by_id(field_id)
        if field_def:
            field_def.usage_count += 1
            await self.update(field_def)
    
    async def get_popular(self, limit: int = 20) -> List[CustomFieldDefinition]:
        """Get most popular shareable custom field definitions."""
        shareable_fields = await self.get_shareable()
        return sorted(shareable_fields, key=lambda x: x.usage_count, reverse=True)[:limit]
    
    async def get_all(self) -> List[CustomFieldDefinition]:
        """Get all custom field definitions."""
        all_data = self.storage.find_nodes_by_type('custom_field', limit=1000)  # Large limit to get all
        return [self._data_to_custom_field(data) for data in all_data]
    
    def _data_to_custom_field(self, data: Dict[str, Any]) -> CustomFieldDefinition:
        """Convert stored data to CustomFieldDefinition object."""
        # Parse datetime fields
        created_at = datetime.fromisoformat(data['created_at']) if data.get('created_at') else datetime.utcnow()
        updated_at = datetime.fromisoformat(data['updated_at']) if data.get('updated_at') else datetime.utcnow()
        
        # Parse enum
        field_type = CustomFieldType(data.get('field_type', 'text'))
        
        return CustomFieldDefinition(
            id=data.get('id'),
            name=data.get('name', ''),
            display_name=data.get('display_name', ''),
            field_type=field_type,
            description=data.get('description'),
            created_by_user_id=data.get('created_by_user_id', ''),
            is_shareable=data.get('is_shareable', False),
            is_global=data.get('is_global', False),
            default_value=data.get('default_value'),
            placeholder_text=data.get('placeholder_text'),
            help_text=data.get('help_text'),
            predefined_options=data.get('predefined_options', []),
            allow_custom_options=data.get('allow_custom_options', True),
            rating_min=data.get('rating_min', 1),
            rating_max=data.get('rating_max', 5),
            rating_labels=data.get('rating_labels', {}),
            usage_count=data.get('usage_count', 0),
            created_at=created_at,
            updated_at=updated_at
        )


class RedisImportMappingRepository(ImportMappingRepository):
    """Redis-based implementation of ImportMappingRepository."""
    
    def __init__(self, storage: RedisGraphStorage):
        self.storage = storage
    
    async def create(self, template: ImportMappingTemplate) -> ImportMappingTemplate:
        """Create a new import mapping template."""
        if not template.id:
            template.id = str(uuid.uuid4())
        
        template_data = asdict(template)
        template_data = _serialize_for_json(template_data)
        
        success = self.storage.store_node('import_mapping', template.id, template_data)
        if not success:
            raise Exception(f"Failed to create import mapping template {template.id}")
        
        return template
    
    async def get_by_id(self, template_id: str) -> Optional[ImportMappingTemplate]:
        """Get an import mapping template by ID."""
        template_data = self.storage.get_node('import_mapping', template_id)
        if not template_data:
            return None
        
        return self._data_to_import_mapping(template_data)
    
    async def get_by_user(self, user_id: str) -> List[ImportMappingTemplate]:
        """Get all import mapping templates for a user (includes system defaults)."""
        # Get user's own templates
        user_search_results = self.storage.search_nodes('import_mapping', {'user_id': user_id})
        user_templates = [self._data_to_import_mapping(data) for data in user_search_results]
        
        # Get system default templates
        system_search_results = self.storage.search_nodes('import_mapping', {'user_id': '__system__'})
        system_templates = [self._data_to_import_mapping(data) for data in system_search_results]
        
        # Return system templates first, then user templates
        return system_templates + user_templates
    
    async def detect_template(self, headers: List[str], user_id: str) -> Optional[ImportMappingTemplate]:
        """Detect matching template based on CSV headers."""
        # Get all templates (user + system)
        all_templates = await self.get_by_user(user_id)
        
        # Find template with best header match
        best_template = None
        best_match_score = 0
        
        for template in all_templates:
            if not template.sample_headers:
                continue
            
            # Calculate match score based on header overlap
            header_set = set(headers)
            sample_set = set(template.sample_headers)
            
            # Intersection over union
            intersection = len(header_set & sample_set)
            union = len(header_set | sample_set)
            
            if union > 0:
                match_score = intersection / union
                if match_score > best_match_score and match_score > 0.7:  # 70% similarity threshold
                    best_match_score = match_score
                    best_template = template
        
        return best_template
    
    async def get_all(self) -> List[ImportMappingTemplate]:
        """Get all import mapping templates."""
        # Get all templates by searching with empty criteria
        all_template_data = self.storage.search_nodes('import_mapping', {})
        return [self._data_to_import_mapping(data) for data in all_template_data]

    async def update(self, template: ImportMappingTemplate) -> ImportMappingTemplate:
        """Update an existing import mapping template."""
        template.updated_at = datetime.utcnow()
        template_data = asdict(template)
        template_data = _serialize_for_json(template_data)
        
        success = self.storage.store_node('import_mapping', template.id, template_data)
        if not success:
            raise Exception(f"Failed to update import mapping template {template.id}")
        
        return template
    
    async def delete(self, template_id: str) -> bool:
        """Delete an import mapping template."""
        return self.storage.delete_node('import_mapping', template_id)
    
    async def increment_usage(self, template_id: str) -> None:
        """Increment usage count and update last used timestamp."""
        template = await self.get_by_id(template_id)
        if template:
            template.times_used += 1
            template.last_used = datetime.utcnow()
            await self.update(template)
    
    def _data_to_import_mapping(self, data: Dict[str, Any]) -> ImportMappingTemplate:
        """Convert stored data to ImportMappingTemplate object."""
        # Parse datetime fields
        created_at = datetime.fromisoformat(data['created_at']) if data.get('created_at') else datetime.utcnow()
        updated_at = datetime.fromisoformat(data['updated_at']) if data.get('updated_at') else datetime.utcnow()
        last_used = None
        if data.get('last_used'):
            last_used = datetime.fromisoformat(data['last_used'])
        
        return ImportMappingTemplate(
            id=data.get('id'),
            user_id=data.get('user_id', ''),
            name=data.get('name', ''),
            description=data.get('description'),
            source_type=data.get('source_type', ''),
            sample_headers=data.get('sample_headers', []),
            field_mappings=data.get('field_mappings', {}),
            times_used=data.get('times_used', 0),
            last_used=last_used,
            created_at=created_at,
            updated_at=updated_at
        )


# Add base repository for categories
class CategoryRepository:
    """Abstract base repository for categories/genres."""
    
    async def create(self, category: Category) -> Category:
        """Create a new category."""
        raise NotImplementedError
    
    async def get_by_id(self, category_id: str) -> Optional[Category]:
        """Get a category by ID."""
        raise NotImplementedError
    
    async def get_by_name(self, name: str) -> Optional[Category]:
        """Get a category by exact name."""
        raise NotImplementedError
    
    async def search_by_name(self, query: str) -> List[Category]:
        """Search categories by name pattern."""
        raise NotImplementedError
    
    async def get_all(self) -> List[Category]:
        """Get all categories."""
        raise NotImplementedError
    
    async def get_root_categories(self) -> List[Category]:
        """Get categories with no parent (root level)."""
        raise NotImplementedError
    
    async def get_children(self, parent_id: str) -> List[Category]:
        """Get direct children of a category."""
        raise NotImplementedError
    
    async def update(self, category: Category) -> Category:
        """Update an existing category."""
        raise NotImplementedError
    
    async def delete(self, category_id: str) -> bool:
        """Delete a category."""
        raise NotImplementedError
    
    async def find_or_create(self, name: str, parent_id: Optional[str] = None) -> Category:
        """Find existing category by name or create new one."""
        raise NotImplementedError


class RedisCategoryRepository(CategoryRepository):
    """Redis-based implementation of CategoryRepository."""
    
    def __init__(self, storage: RedisGraphStorage):
        self.storage = storage
    
    async def create(self, category: Category) -> Category:
        """Create a new category."""
        if not category.id:
            category.id = str(uuid.uuid4())
        
        category.updated_at = datetime.utcnow()
        
        # Prepare data for storage
        category_data = {
            '_id': category.id,
            'name': category.name,
            'normalized_name': category.normalized_name,
            'parent_id': category.parent_id,
            'description': category.description,
            'level': category.level,
            'color': category.color,
            'icon': category.icon,
            'aliases': category.aliases,
            'book_count': category.book_count,  # Include book count
            'created_at': category.created_at.isoformat(),
            'updated_at': category.updated_at.isoformat()
        }
        
        # Store the category node
        success = self.storage.store_node('category', category.id, category_data)
        if not success:
            raise Exception(f"Failed to create category: {category.name}")
        
        # Create parent-child relationship if parent exists
        if category.parent_id:
            self.storage.create_relationship(
                'category', category.parent_id, 'HAS_CHILD', 'category', category.id
            )
        
        print(f"✅ [CATEGORY_REPO] Created category: {category.name} (ID: {category.id})")
        return category
    
    async def get_by_id(self, category_id: str) -> Optional[Category]:
        """Get a category by ID."""
        try:
            category_data = self.storage.get_node('category', category_id)
            if not category_data:
                return None
            
            return self._data_to_category(category_data)
        except Exception as e:
            print(f"❌ [CATEGORY_REPO] Error getting category by ID {category_id}: {e}")
            return None
    
    async def get_by_name(self, name: str) -> Optional[Category]:
        """Get a category by exact name."""
        try:
            normalized_name = Category._normalize_name(name)
            all_categories = self.storage.find_nodes_by_type('category')
            
            for category_data in all_categories:
                if category_data.get('normalized_name') == normalized_name:
                    return self._data_to_category(category_data)
            
            return None
        except Exception as e:
            print(f"❌ [CATEGORY_REPO] Error getting category by name {name}: {e}")
            return None
    
    async def search_by_name(self, query: str) -> List[Category]:
        """Search categories by name pattern."""
        try:
            normalized_query = query.lower()
            all_categories = self.storage.find_nodes_by_type('category')
            
            matching_categories = []
            for category_data in all_categories:
                category_name = category_data.get('name', '').lower()
                normalized_name = category_data.get('normalized_name', '')
                aliases = category_data.get('aliases', [])
                
                # Check name, normalized name, and aliases
                if (normalized_query in category_name or 
                    normalized_query in normalized_name or
                    any(normalized_query in alias.lower() for alias in aliases)):
                    matching_categories.append(self._data_to_category(category_data))
            
            # Sort by relevance (exact match first, then starts with, then contains)
            def sort_key(category):
                name_lower = category.name.lower()
                if name_lower == normalized_query:
                    return 0  # Exact match
                elif name_lower.startswith(normalized_query):
                    return 1  # Starts with
                else:
                    return 2  # Contains
            
            matching_categories.sort(key=sort_key)
            return matching_categories[:50]  # Limit results
            
        except Exception as e:
            print(f"❌ [CATEGORY_REPO] Error searching categories: {e}")
            return []
    
    async def get_all(self) -> List[Category]:
        """Get all categories."""
        try:
            all_category_data = self.storage.find_nodes_by_type('category')
            categories = []
            
            for category_data in all_category_data:
                category = self._data_to_category(category_data)
                if category:
                    categories.append(category)
            
            # Sort by level first, then by name
            categories.sort(key=lambda c: (c.level, c.name.lower()))
            return categories
            
        except Exception as e:
            print(f"❌ [CATEGORY_REPO] Error getting all categories: {e}")
            return []
    
    async def get_root_categories(self) -> List[Category]:
        """Get categories with no parent (root level)."""
        try:
            all_categories = await self.get_all()
            root_categories = [cat for cat in all_categories if cat.parent_id is None]
            root_categories.sort(key=lambda c: c.name.lower())
            return root_categories
        except Exception as e:
            print(f"❌ [CATEGORY_REPO] Error getting root categories: {e}")
            return []
    
    async def get_children(self, parent_id: str) -> List[Category]:
        """Get direct children of a category."""
        try:
            # Get relationships where parent has children
            relationships = self.storage.get_relationships('category', parent_id, 'HAS_CHILD')
            
            children = []
            for rel in relationships:
                if rel.get('to_type') == 'category':
                    child_id = rel.get('to_id')
                    child = await self.get_by_id(child_id)
                    if child:
                        children.append(child)
            
            children.sort(key=lambda c: c.name.lower())
            return children
            
        except Exception as e:
            print(f"❌ [CATEGORY_REPO] Error getting children for {parent_id}: {e}")
            return []
    
    async def update(self, category: Category) -> Category:
        """Update an existing category."""
        try:
            category.updated_at = datetime.utcnow()
            
            # Get existing data to preserve relationships
            existing_data = self.storage.get_node('category', category.id)
            if not existing_data:
                raise Exception(f"Category {category.id} not found")
            
            # Prepare updated data
            category_data = {
                '_id': category.id,
                'name': category.name,
                'normalized_name': category.normalized_name,
                'parent_id': category.parent_id,
                'description': category.description,
                'level': category.level,
                'color': category.color,
                'icon': category.icon,
                'aliases': category.aliases,
                'book_count': existing_data.get('book_count', category.book_count),  # Preserve existing count or use new
                'created_at': existing_data.get('created_at', category.created_at.isoformat()),
                'updated_at': category.updated_at.isoformat()
            }
            
            # Update the node
            success = self.storage.store_node('category', category.id, category_data)
            if not success:
                raise Exception(f"Failed to update category: {category.name}")
            
            # Handle parent relationship changes
            old_parent_id = existing_data.get('parent_id')
            new_parent_id = category.parent_id
            
            if old_parent_id != new_parent_id:
                # Remove old parent relationship
                if old_parent_id:
                    self.storage.delete_relationship('category', old_parent_id, 'HAS_CHILD', 'category', category.id)
                
                # Add new parent relationship
                if new_parent_id:
                    self.storage.create_relationship('category', new_parent_id, 'HAS_CHILD', 'category', category.id)
            
            print(f"✅ [CATEGORY_REPO] Updated category: {category.name} (ID: {category.id})")
            return category
            
        except Exception as e:
            print(f"❌ [CATEGORY_REPO] Error updating category {category.id}: {e}")
            raise
    
    async def delete(self, category_id: str) -> bool:
        """Delete a category."""
        try:
            # Check if category has children
            children = await self.get_children(category_id)
            if children:
                raise Exception(f"Cannot delete category with {len(children)} children. Move or delete children first.")
            
            # Check if category is used by any books
            book_relationships = self.storage.get_relationships('book', None, 'HAS_CATEGORY')
            used_by_books = [rel for rel in book_relationships if rel.get('to_id') == category_id]
            
            if used_by_books:
                raise Exception(f"Cannot delete category used by {len(used_by_books)} books. Remove category from books first.")
            
            # Get category data for parent relationship cleanup
            category_data = self.storage.get_node('category', category_id)
            if not category_data:
                return False
            
            # Remove parent relationship if exists
            parent_id = category_data.get('parent_id')
            if parent_id:
                self.storage.delete_relationship('category', parent_id, 'HAS_CHILD', 'category', category_id)
            
            # Delete the category node
            success = self.storage.delete_node('category', category_id)
            
            if success:
                print(f"✅ [CATEGORY_REPO] Deleted category: {category_id}")
            
            return success
            
        except Exception as e:
            print(f"❌ [CATEGORY_REPO] Error deleting category {category_id}: {e}")
            return False
    
    async def find_or_create(self, name: str, parent_id: Optional[str] = None) -> Category:
        """Find existing category by name or create new one."""
        try:
            # First try to find by exact name
            existing = await self.get_by_name(name)
            if existing:
                print(f"✅ [CATEGORY_REPO] Found existing category: {name} (ID: {existing.id})")
                return existing
            
            # Create new category
            level = 0
            if parent_id:
                parent = await self.get_by_id(parent_id)
                if parent:
                    level = parent.level + 1
            
            new_category = Category(
                id=str(uuid.uuid4()),
                name=name.strip(),
                parent_id=parent_id,
                level=level,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            created_category = await self.create(new_category)
            print(f"✅ [CATEGORY_REPO] Created new category: {name} (ID: {created_category.id})")
            return created_category
            
        except Exception as e:
            print(f"❌ [CATEGORY_REPO] Error in find_or_create for {name}: {e}")
            raise
    
    def _data_to_category(self, data: Dict[str, Any]) -> Optional[Category]:
        """Convert Redis data to Category domain model."""
        try:
            # Handle datetime fields
            created_at = datetime.utcnow()
            updated_at = datetime.utcnow()
            
            if 'created_at' in data and isinstance(data['created_at'], str):
                try:
                    created_at = datetime.fromisoformat(data['created_at'])
                except:
                    pass
            
            if 'updated_at' in data and isinstance(data['updated_at'], str):
                try:
                    updated_at = datetime.fromisoformat(data['updated_at'])
                except:
                    pass
            
            # Handle aliases
            aliases = data.get('aliases', [])
            if isinstance(aliases, str):
                aliases = [aliases]
            elif not isinstance(aliases, list):
                aliases = []
            
            category = Category(
                id=data.get('_id'),
                name=data.get('name', ''),
                normalized_name=data.get('normalized_name', ''),
                parent_id=data.get('parent_id'),
                description=data.get('description'),
                level=data.get('level', 0),
                color=data.get('color'),
                icon=data.get('icon'),
                aliases=aliases,
                book_count=data.get('book_count', 0),  # Include book count
                created_at=created_at,
                updated_at=updated_at
            )
            
            return category
            
        except Exception as e:
            print(f"❌ [CATEGORY_REPO] Error converting data to category: {e}")
            return None
    
    async def build_hierarchy_for_categories(self, categories: List[Category]) -> List[Category]:
        """Build parent-child relationships for a list of categories."""
        try:
            # Create lookup maps
            category_map = {cat.id: cat for cat in categories}
            
            # Build relationships
            for category in categories:
                # Reset relationships
                category.parent = None
                category.children = []
                
                # Set parent reference
                if category.parent_id and category.parent_id in category_map:
                    category.parent = category_map[category.parent_id]
                
                # Set children references
                for other_category in categories:
                    if other_category.parent_id == category.id:
                        category.children.append(other_category)
                
                # Sort children by name
                category.children.sort(key=lambda c: c.name.lower())
            
            return categories
            
        except Exception as e:
            print(f"❌ [CATEGORY_REPO] Error building hierarchy: {e}")
            return categories
    
    async def get_category_usage_stats(self, category_id: str, user_id: Optional[str] = None) -> Dict[str, int]:
        """Get usage statistics for a category."""
        try:
            stats = {
                'total_books': 0,
                'user_books': 0,
                'children_count': 0,
                'descendants_count': 0
            }
            
            # Count books with this category
            book_relationships = self.storage.get_relationships('book', None, 'HAS_CATEGORY')
            category_books = [rel for rel in book_relationships if rel.get('to_id') == category_id]
            stats['total_books'] = len(category_books)
            
            # Count user's books with this category (if user specified)
            if user_id:
                # This would require cross-referencing with user-book relationships
                # For now, we'll implement a basic version
                stats['user_books'] = 0  # TODO: Implement user-specific count
            
            # Count direct children
            children = await self.get_children(category_id)
            stats['children_count'] = len(children)
            
            # Count all descendants recursively
            def count_descendants(cat_id):
                children = self.storage.get_relationships('category', cat_id, 'HAS_CHILD')
                count = len(children)
                for child_rel in children:
                    child_id = child_rel.get('to_id')
                    if child_id:
                        count += count_descendants(child_id)
                return count
            
            stats['descendants_count'] = count_descendants(category_id)
            
            return stats
            
        except Exception as e:
            print(f"❌ [CATEGORY_REPO] Error getting usage stats for {category_id}: {e}")
            return {'total_books': 0, 'user_books': 0, 'children_count': 0, 'descendants_count': 0}

    async def increment_book_count(self, category_id: str) -> bool:
        """Increment the book count for a category."""
        try:
            # Get current category data
            category_data = self.storage.get_node('category', category_id)
            if not category_data:
                print(f"❌ [CATEGORY_REPO] Category {category_id} not found for book count increment")
                return False
            
            # Increment book_count (default to 0 if not present)
            current_count = category_data.get('book_count', 0)
            new_count = current_count + 1
            
            # Update the category data
            category_data['book_count'] = new_count
            category_data['updated_at'] = datetime.utcnow().isoformat()
            
            # Store updated data
            success = self.storage.store_node('category', category_id, category_data)
            if success:
                print(f"📊 [CATEGORY_REPO] Incremented book count for category {category_id}: {current_count} -> {new_count}")
            else:
                print(f"❌ [CATEGORY_REPO] Failed to update book count for category {category_id}")
            
            return success
            
        except Exception as e:
            print(f"❌ [CATEGORY_REPO] Error incrementing book count for {category_id}: {e}")
            return False

    async def recalculate_book_count(self, category_id: str) -> bool:
        """Recalculate the book count for a category by counting actual relationships."""
        try:
            # Count actual HAS_CATEGORY relationships pointing to this category
            pattern = f"rel:book:*:HAS_CATEGORY"
            keys = self.storage.redis.keys(pattern)
            count = 0
            
            for key in keys:
                rel_strings = self.storage.redis.smembers(key)
                for rel_string in rel_strings:
                    try:
                        rel_data = json.loads(rel_string)
                        if rel_data.get('to_id') == category_id:
                            count += 1
                    except json.JSONDecodeError:
                        continue
            
            # Update category with correct count
            category_data = self.storage.get_node('category', category_id)
            if category_data:
                category_data['book_count'] = count
                category_data['updated_at'] = datetime.utcnow().isoformat()
                success = self.storage.store_node('category', category_id, category_data)
                
                if success:
                    print(f"📊 [CATEGORY_REPO] Recalculated book count for category {category_id}: {count}")
                    return True
            
            return False
            
        except Exception as e:
            print(f"❌ [CATEGORY_REPO] Error recalculating book count for {category_id}: {e}")
            return False
