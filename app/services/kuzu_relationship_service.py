"""
Kuzu Relationship Service

Handles user-book relationships, custom metadata, and ownership tracking using Kuzu.
Focused responsibility: User-book relationship management and metadata.
"""

import json
import traceback
from typing import List, Optional, Dict, Any
from datetime import datetime, date

from ..domain.models import Book, UserBookRelationship, ReadingStatus, OwnershipStatus, Person, BookContribution, ContributionType
from ..infrastructure.kuzu_repositories import KuzuUserRepository
from ..infrastructure.kuzu_graph import get_graph_storage
from .kuzu_async_helper import run_async
from .kuzu_book_service import KuzuBookService
from ..debug_system import debug_log


class KuzuRelationshipService:
    """Service for user-book relationship and metadata management."""
    
    def __init__(self):
        self.graph_storage = get_graph_storage()
        self.user_repo = KuzuUserRepository()
        self.book_service = KuzuBookService()
    
    def _load_contributors_for_book(self, book: Book) -> None:
        """Load contributors for a book from the database."""
        try:
            if not book.id:
                return  # Cannot load contributors without book ID
                
            # Query contributors directly using the graph storage
            query = """
            MATCH (p:Person)-[rel:AUTHORED]->(b:Book {id: $book_id})
            RETURN p.name as name, p.id as id, rel.role as role, rel.order_index as order_index
            ORDER BY rel.order_index ASC
            """
            
            results = self.graph_storage.query(query, {"book_id": book.id})
            
            import logging
            logger = logging.getLogger(__name__)
            
            contributors = []
            for result in results:
                if result.get('col_0'):  # name
                    # Create Person object
                    person = Person(
                        id=result.get('col_1') or '',
                        name=result.get('col_0') or '',
                        normalized_name=(result.get('col_0') or '').strip().lower()
                    )
                    
                    # Map role string to ContributionType enum
                    role_str = (result.get('col_2') or 'authored').lower()
                    try:
                        contribution_type = ContributionType(role_str)
                    except ValueError:
                        # Default to AUTHORED if role is not recognized
                        contribution_type = ContributionType.AUTHORED
                    
                    # Create BookContribution object  
                    contribution = BookContribution(
                        person_id=person.id or '',
                        book_id=book.id,
                        contribution_type=contribution_type,
                        order=result.get('col_3', 0),
                        person=person
                    )
                    
                    contributors.append(contribution)
            
            book.contributors = contributors
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to load contributors for book {book.id}: {e}")
            # Initialize empty list on error
            book.contributors = []

    def _create_enriched_book(self, book_data: Dict[str, Any], relationship_data: Dict[str, Any], locations_data: Optional[List[Dict[str, Any]]] = None) -> Book:
        """Create an enriched Book object with user-specific attributes."""
        # Convert book data to Book object using the book service
        book = self.book_service._dict_to_book(book_data)
        
        # Load contributors directly here to avoid async issues
        self._load_contributors_for_book(book)
        
        # Add user-specific attributes dynamically using setattr
        setattr(book, 'reading_status', relationship_data.get('reading_status', 'plan_to_read'))
        setattr(book, 'ownership_status', relationship_data.get('ownership_status', 'owned'))
        setattr(book, 'start_date', relationship_data.get('start_date'))
        setattr(book, 'finish_date', relationship_data.get('finish_date'))
        setattr(book, 'user_rating', relationship_data.get('user_rating'))
        setattr(book, 'personal_notes', relationship_data.get('personal_notes'))
        setattr(book, 'review', relationship_data.get('user_review'))  # Map user_review back to review
        setattr(book, 'date_added', relationship_data.get('date_added'))
        setattr(book, 'want_to_read', relationship_data.get('reading_status') == 'plan_to_read')
        setattr(book, 'library_only', relationship_data.get('reading_status') == 'library_only')
        
        # Handle location information
        locations = locations_data if locations_data else []
        location_id = None
        
        # If we have locations from STORED_AT relationships, use them
        if locations:
            location_id = locations[0].get('id')  # Use first location for backward compatibility
            debug_log(f"Using STORED_AT locations: {locations}", "RELATIONSHIP")
        else:
            # Fall back to old location_id field for backward compatibility
            location_id = relationship_data.get('location_id')
            if location_id:
                # Look up the actual location name
                location_name = location_id  # Default fallback
                try:
                    from app.infrastructure.kuzu_graph import get_kuzu_connection
                    from app.location_service import LocationService
                    
                    kuzu_connection = get_kuzu_connection()
                    location_service = LocationService(kuzu_connection.connect())
                    location = location_service.get_location(location_id)
                    if location:
                        location_name = location.name
                        debug_log(f"Resolved legacy location {location_id} to name '{location_name}'", "RELATIONSHIP")
                    else:
                        debug_log(f"Legacy location {location_id} not found, using ID as name", "RELATIONSHIP")
                except Exception as e:
                    debug_log(f"Error resolving legacy location name for {location_id}: {e}", "RELATIONSHIP")
                
                # Create locations list from legacy data
                locations = [{'id': location_id, 'name': location_name}]
        
        # Set location attributes on book
        setattr(book, 'location_id', location_id)
        setattr(book, 'locations', locations)
        
        # Convert date strings back to date objects if needed
        start_date = getattr(book, 'start_date', None)
        if isinstance(start_date, str):
            try:
                setattr(book, 'start_date', datetime.fromisoformat(start_date).date())
            except:
                setattr(book, 'start_date', None)
        
        finish_date = getattr(book, 'finish_date', None)
        if isinstance(finish_date, str):
            try:
                setattr(book, 'finish_date', datetime.fromisoformat(finish_date).date())
            except:
                setattr(book, 'finish_date', None)
        
        return book
    
    async def get_books_for_user(self, user_id: str, limit: int = 50, offset: int = 0) -> List[Book]:
        """Get all books for a user with relationship data and locations (common library model)."""
        try:
            # Common library model: Get ALL books with optional user overlay data
            # No longer requires OWNS relationships since all books are in the common library
            query = """
            MATCH (b:Book)
            OPTIONAL MATCH (u:User {id: $user_id})-[owns:OWNS]->(b)
            OPTIONAL MATCH (b)-[stored:STORED_AT]->(l:Location)
            WHERE stored.user_id = $user_id OR stored IS NULL
            RETURN b, owns, COLLECT(DISTINCT {id: l.id, name: l.name}) as locations
            SKIP $offset LIMIT $limit
            """
            
            results = self.graph_storage.query(query, {
                "user_id": user_id,
                "offset": offset,
                "limit": limit
            })
            
            books = []
            for result in results:
                if 'col_0' in result and 'col_1' in result:
                    book_data = result['col_0']
                    relationship_data = result['col_1'] or {}
                    locations_data = result.get('col_2', []) or []
                    
                    # Filter out null/empty locations
                    valid_locations = [loc for loc in locations_data if loc.get('id')]
                    
                    book = self._create_enriched_book(book_data, relationship_data, valid_locations)
                    books.append(book)
            
            return books
            
        except Exception as e:
            traceback.print_exc()
            return []
    
    async def get_book_by_id_for_user(self, book_id: str, user_id: str) -> Optional[Book]:
        """Get a specific book for a user with relationship data (common library model)."""
        try:
            # Common library model: Get ANY book with optional user overlay data
            # No longer requires OWNS relationships since all books are in the common library
            query = """
            MATCH (b:Book {id: $book_id})
            OPTIONAL MATCH (u:User {id: $user_id})-[owns:OWNS]->(b)
            OPTIONAL MATCH (b)-[stored:STORED_AT]->(l:Location)
            WHERE stored.user_id = $user_id OR stored IS NULL
            RETURN b, owns, COLLECT(DISTINCT {id: l.id, name: l.name}) as locations
            """
            
            results = self.graph_storage.query(query, {
                "user_id": user_id,
                "book_id": book_id
            })
            
            if not results:
                return None
                
            result = results[0]
            if 'col_0' not in result or 'col_1' not in result:
                return None
                
            book_data = result['col_0']
            relationship_data = result['col_1']
            locations_data = result.get('col_2', []) or []
            
            # Filter out null/empty locations
            valid_locations = [loc for loc in locations_data if loc.get('id')]
            
            # Create enriched book with user-specific attributes
            book = self._create_enriched_book(book_data, relationship_data, valid_locations)
            
            # Load custom metadata for this user-book combination
            custom_metadata = self._load_custom_metadata(relationship_data)
            setattr(book, 'custom_metadata', custom_metadata)
                    
            return book
            
        except Exception as e:
            traceback.print_exc()
            return None
    
    def _load_custom_metadata(self, relationship_data: Dict[str, Any]) -> Dict[str, Any]:
        """Load custom metadata from relationship data."""
        try:
            custom_metadata = {}
            
            # Extract custom metadata from the OWNS relationship's custom_metadata JSON field
            owns_metadata = relationship_data or {}
            if 'custom_metadata' in owns_metadata and owns_metadata['custom_metadata']:
                try:
                    # The custom_metadata field contains a JSON string or dict
                    metadata_value = owns_metadata['custom_metadata']
                    if isinstance(metadata_value, str):
                        custom_metadata = json.loads(metadata_value)
                    elif isinstance(metadata_value, dict):
                        custom_metadata = metadata_value
                    else:
                        custom_metadata = {}
                except (json.JSONDecodeError, TypeError) as e:
                    custom_metadata = {}
            
            return custom_metadata
            
        except Exception as e:
            return {}
    
    def add_book_to_user_library_sync(self, user_id: str, book_id: str, 
                                      reading_status: str = "library_only",
                                      ownership_status: str = "owned",
                                      locations: Optional[List[str]] = None,
                                      custom_metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Add a book to user's library with custom metadata support (sync version)."""
        try:
            # Convert string status to enum if needed
            if isinstance(reading_status, str):
                reading_status_map = {
                    'read': ReadingStatus.READ,
                    'reading': ReadingStatus.READING, 
                    'plan_to_read': ReadingStatus.PLAN_TO_READ,
                    'on_hold': ReadingStatus.ON_HOLD,
                    'did_not_finish': ReadingStatus.DNF,
                    'dnf': ReadingStatus.DNF,
                    'library_only': ReadingStatus.LIBRARY_ONLY
                }
                reading_status_enum = reading_status_map.get(reading_status, ReadingStatus.LIBRARY_ONLY)
            else:
                reading_status_enum = reading_status

            if isinstance(ownership_status, str):
                ownership_status_map = {
                    'owned': OwnershipStatus.OWNED,
                    'borrowed': OwnershipStatus.BORROWED,
                    'wishlist': OwnershipStatus.WISHLIST
                }
                ownership_status_enum = ownership_status_map.get(ownership_status, OwnershipStatus.OWNED)
            else:
                ownership_status_enum = ownership_status

            # Create OWNS relationship with custom metadata
            rel_data = {
                'reading_status': reading_status_enum.value if hasattr(reading_status_enum, 'value') else str(reading_status_enum),
                'ownership_status': ownership_status_enum.value if hasattr(ownership_status_enum, 'value') else str(ownership_status_enum),
                'date_added': datetime.utcnow().isoformat()
            }
            
            # Add locations if provided
            if locations:
                # Use the first location as location_id (OWNS schema has location_id, not locations)
                if len(locations) > 0:
                    rel_data['location_id'] = locations[0]
            
            # Add custom metadata as JSON
            if custom_metadata:
                rel_data['custom_metadata'] = json.dumps(custom_metadata)
            
            # Create the relationship
            success = self.graph_storage.create_relationship(
                'User', user_id, 'OWNS', 'Book', book_id, rel_data
            )
            
            if success:
                if custom_metadata:
                    pass  # Custom metadata would be handled separately
            
            return success
            
        except Exception as e:
            traceback.print_exc()
            return False
    
    async def update_user_book_relationship(self, user_id: str, book_id: str, updates: Dict[str, Any]) -> bool:
        """Update the relationship between a user and book."""
        try:
            
            # Get the current relationship - check if any OWNS relationship exists
            query = """
            MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book {id: $book_id})
            RETURN owns
            LIMIT 1
            """
            
            results = self.graph_storage.query(query, {
                "user_id": user_id,
                "book_id": book_id
            })
            
            if results:
                pass  # Process results
            
            if not results or 'result' not in results[0]:
                # Try to create the relationship first with the updates included
                
                # Extract base properties for relationship creation
                reading_status = updates.get('reading_status', 'library_only')
                ownership_status = updates.get('ownership_status', 'owned')
                custom_metadata = updates.get('custom_metadata')
                
                # Handle location updates - convert location_id to locations list
                locations = []
                if 'location_id' in updates and updates['location_id']:
                    locations = [updates['location_id']]
                elif 'locations' in updates and updates['locations']:
                    locations = updates['locations']
                
                success = self.add_book_to_user_library_sync(
                    user_id=user_id,
                    book_id=book_id,
                    reading_status=reading_status,
                    ownership_status=ownership_status,
                    locations=locations,
                    custom_metadata=custom_metadata
                )
                
                if not success:
                    return False
                
                return True
            
            current_rel = results[0]['result']
            
            # Handle field mapping for compatibility
            field_mapping = {
                'review': 'user_review'  # Map 'review' to 'user_review' in database
            }
            
            # Handle custom metadata specially and apply field mapping
            processed_updates = {}
            for key, value in updates.items():
                # Apply field mapping if needed
                db_field = field_mapping.get(key, key)
                
                if key == 'custom_metadata' and isinstance(value, dict):
                    processed_updates[db_field] = json.dumps(value)
                else:
                    processed_updates[db_field] = value
            
            # Always update the updated_at timestamp
            # KuzuDB requires datetime objects for TIMESTAMP fields, not strings
            processed_updates['updated_at'] = datetime.utcnow()
            
            # Build dynamic SET clause for only the fields being updated
            set_clauses = []
            params = {"user_id": user_id, "book_id": book_id}
            
            for key, value in processed_updates.items():
                param_name = f"new_{key}"
                set_clauses.append(f"owns.{key} = ${param_name}")
                params[param_name] = value
            
            # Update the relationship with individual property updates
            update_query = f"""
            MATCH (u:User {{id: $user_id}})-[owns:OWNS]->(b:Book {{id: $book_id}})
            SET {', '.join(set_clauses)}
            """
            
            print(f"🔧 [UPDATE_RELATIONSHIP] Executing query: {update_query}")
            print(f"🔧 [UPDATE_RELATIONSHIP] With params: {params}")
            
            self.graph_storage.query(update_query, params)
            
            return True
            
        except Exception as e:
            traceback.print_exc()
            return False
    
    async def remove_book_from_user_library(self, user_id: str, book_id: str) -> bool:
        """Remove a book from user's library (delete OWNS relationship)."""
        try:
            print(f"🗑️ [REMOVE_FROM_LIBRARY] Removing book {book_id} from user {user_id} library")
            
            # Delete user-book relationship using Kuzu query
            delete_rel_query = """
            MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book {id: $book_id})
            DELETE owns
            """
            
            self.graph_storage.query(delete_rel_query, {
                "user_id": user_id,
                "book_id": book_id
            })
            
            return True
            
        except Exception as e:
            traceback.print_exc()
            return False
    
    async def get_all_books_with_user_overlay(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all books with user-specific overlay data."""
        try:
            # Use the existing get_books_for_user method to get enriched books
            books = await self.get_books_for_user(user_id, limit=10000)  # Get all books
            
            # Convert Book objects to dictionaries
            book_dicts = []
            for book in books:
                if hasattr(book, '__dict__'):
                    book_dict = book.__dict__.copy()
                else:
                    # Fallback for non-standard book objects
                    book_dict = {
                        'id': getattr(book, 'id', ''),
                        'title': getattr(book, 'title', ''),
                        'uid': getattr(book, 'id', ''),  # Add uid for compatibility
                    }
                
                # Ensure uid is available (some templates expect this)
                if 'id' in book_dict and 'uid' not in book_dict:
                    book_dict['uid'] = book_dict['id']
                
                book_dicts.append(book_dict)
            
            return book_dicts
            
        except Exception as e:
            traceback.print_exc()
            return []
    
    def get_book_by_uid_sync(self, uid: str, user_id: str) -> Optional[Book]:
        """Get a book by UID with user overlay data - sync wrapper."""
        # Enhanced query to also get location information via STORED_AT relationships
        query = """
        MATCH (b:Book {id: $book_id})
        OPTIONAL MATCH (u:User {id: $user_id})-[owns:OWNS]->(b)
        OPTIONAL MATCH (b)-[stored:STORED_AT]->(l:Location)
        WHERE stored.user_id = $user_id OR stored IS NULL
        RETURN b, owns, COLLECT(DISTINCT {id: l.id, name: l.name}) as locations
        """
        
        results = self.graph_storage.query(query, {
            "user_id": user_id,
            "book_id": uid
        })
        
        if not results:
            return None
            
        result = results[0]
        if 'col_0' not in result:
            return None
            
        book_data = result['col_0']
        relationship_data = result.get('col_1', {}) or {}
        locations_data = result.get('col_2', []) or []
        
        # Filter out null/empty locations
        valid_locations = [loc for loc in locations_data if loc.get('id')]
        
        # Create enriched book with user-specific attributes
        book = self._create_enriched_book(book_data, relationship_data, valid_locations)
        
        # Load custom metadata for this user-book combination
        custom_metadata = self._load_custom_metadata(relationship_data)
        setattr(book, 'custom_metadata', custom_metadata)
        
        return book
    
    def get_user_book_sync(self, user_id: str, book_id: str) -> Optional[Dict[str, Any]]:
        """Get user's book by book ID - sync wrapper."""
        result = self.get_book_by_uid_sync(book_id, user_id)
        if result:
            # Convert Book object to dictionary if needed
            if hasattr(result, '__dict__'):
                return result.__dict__
            elif isinstance(result, dict):
                return result
            else:
                # Try to convert to dict
                try:
                    return vars(result)
                except:
                    # Fallback - create basic dict
                    return {
                        'id': getattr(result, 'id', book_id),
                        'title': getattr(result, 'title', 'Unknown'),
                        'custom_metadata': getattr(result, 'custom_metadata', {})
                    }
        return None
    
    async def get_recently_added_want_to_read_books(self, user_id: str, limit: int = 5) -> List[Book]:
        """Get books recently added to the user's want-to-read list."""
        try:
            query = """
            MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
            WHERE owns.reading_status = 'plan_to_read'
            OPTIONAL MATCH (b)-[stored:STORED_AT]->(l:Location)
            WHERE stored.user_id = $user_id OR stored IS NULL
            RETURN b, owns, COLLECT(DISTINCT {id: l.id, name: l.name}) as locations
            ORDER BY owns.date_added DESC
            LIMIT $limit
            """
            
            results = self.graph_storage.query(query, {
                "user_id": user_id,
                "limit": limit
            })
            
            books = []
            for result in results:
                if 'col_0' in result and 'col_1' in result:
                    book_data = result['col_0']
                    relationship_data = result['col_1'] or {}
                    locations_data = result.get('col_2', []) or []
                    
                    # Filter out null/empty locations
                    valid_locations = [loc for loc in locations_data if loc.get('id')]
                    
                    book = self._create_enriched_book(book_data, relationship_data, valid_locations)
                    books.append(book)
            
            return books
            
        except Exception as e:
            traceback.print_exc()
            return []

    # Sync wrappers for backward compatibility
    def get_books_for_user_sync(self, user_id: str, limit: int = 50, offset: int = 0) -> List[Book]:
        """Sync wrapper for get_books_for_user."""
        return run_async(self.get_books_for_user(user_id, limit, offset))
    
    def get_book_by_id_for_user_sync(self, book_id: str, user_id: str) -> Optional[Book]:
        """Sync wrapper for get_book_by_id_for_user."""
        return run_async(self.get_book_by_id_for_user(book_id, user_id))
    
    def update_user_book_relationship_sync(self, user_id: str, book_id: str, updates: Dict[str, Any]) -> bool:
        """Sync wrapper for update_user_book_relationship."""
        return run_async(self.update_user_book_relationship(user_id, book_id, updates))
    
    def remove_book_from_user_library_sync(self, user_id: str, book_id: str) -> bool:
        """Sync wrapper for remove_book_from_user_library."""
        return run_async(self.remove_book_from_user_library(user_id, book_id))
    
    def get_all_books_with_user_overlay_sync(self, user_id: str) -> List[Dict[str, Any]]:
        """Sync wrapper for get_all_books_with_user_overlay."""
        return run_async(self.get_all_books_with_user_overlay(user_id))
    
    def get_recently_added_want_to_read_books_sync(self, user_id: str, limit: int = 5) -> List[Book]:
        """Sync wrapper for get_recently_added_want_to_read_books."""
        return run_async(self.get_recently_added_want_to_read_books(user_id, limit))
