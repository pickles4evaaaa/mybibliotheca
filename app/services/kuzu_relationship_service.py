"""
Kuzu Relationship Service

Handles user-book relationships, custom metadata, and ownership tracking using Kuzu.
Focused responsibility: User-book relationship management and metadata.

This service has been migrated to use the SafeKuzuManager pattern for
improved thread safety and connection management.
"""

import json
import traceback
from typing import List, Optional, Dict, Any
from datetime import datetime, date

from ..domain.models import Book, UserBookRelationship, ReadingStatus, OwnershipStatus, Person, BookContribution, ContributionType
from ..infrastructure.kuzu_repositories import KuzuUserRepository
from ..infrastructure.kuzu_graph import safe_execute_kuzu_query, safe_get_kuzu_connection
from .kuzu_async_helper import run_async
from .kuzu_book_service import KuzuBookService
from ..debug_system import debug_log
import logging

logger = logging.getLogger(__name__)


def _convert_query_result_to_list(result) -> List[Dict[str, Any]]:
    """
    Convert KuzuDB QueryResult to list of dictionaries (matching old graph_storage.query format).
    
    Args:
        result: QueryResult object from KuzuDB
        
    Returns:
        List of dictionaries representing rows
    """
    if result is None:
        return []
    
    rows = []
    try:
        # Check if result has the iterator interface
        if hasattr(result, 'has_next') and hasattr(result, 'get_next'):
            while result.has_next():
                row = result.get_next()
                # Convert row to dict
                if len(row) == 1:
                    # Single column result
                    rows.append({'result': row[0]})
                else:
                    # Multiple columns - create dict with column names
                    row_dict = {}
                    for i, value in enumerate(row):
                        row_dict[f'col_{i}'] = value
                    rows.append(row_dict)
        else:
            # Fallback: if it's already a list or other format
            if isinstance(result, list):
                return result
            elif isinstance(result, dict):
                return [result]
            else:
                # Try to convert to string representation
                rows.append({'result': str(result)})
    except Exception as e:
        logger.warning(f"Error converting query result: {e}")
        # Return empty list if conversion fails
        return []
    
    return rows


class KuzuRelationshipService:
    """
    Service for user-book relationship and metadata management with thread-safe operations.
    
    This service has been migrated to use the SafeKuzuManager pattern for
    improved thread safety and connection management.
    """
    
    def __init__(self, user_id: Optional[str] = None):
        """
        Initialize relationship service with thread-safe database access.
        
        Args:
            user_id: User identifier for tracking and isolation
        """
        self.user_id = user_id or "relationship_service"
        self.user_repo = KuzuUserRepository()
        self.book_service = KuzuBookService(user_id)
    
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
            
            result = safe_execute_kuzu_query(query, {"book_id": book.id})
            results = _convert_query_result_to_list(result)
            
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

    def _create_enriched_book(self, book_data: Dict[str, Any], relationship_data: Dict[str, Any], locations_data: Optional[List[Dict[str, Any]]] = None, personal_meta: Optional[Dict[str, Any]] = None) -> Book:
        """Create an enriched Book object with user-specific attributes.

        Supports data coming from either legacy OWNS relationship or fallback
        HAS_PERSONAL_METADATA relationship when OWNS is absent (universal library mode)
        so that personal_notes / review continue to persist.
        """
        # Convert book data to Book object using the book service
        book = self.book_service._dict_to_book(book_data)
        
        # Load contributors directly here to avoid async issues
        self._load_contributors_for_book(book)
        
        # Add user-specific attributes dynamically using setattr
        combined = {}
        combined.update(relationship_data or {})
        # Personal metadata (HAS_PERSONAL_METADATA) stores json blob in personal_custom_fields; flatten known fields
        if personal_meta:
            # Accept direct fields if present
            for key in ['personal_notes', 'user_review', 'user_rating', 'reading_status', 'ownership_status']:
                if key in personal_meta and personal_meta[key] not in (None, ''):
                    combined[key] = personal_meta[key]
            # Extract from personal_custom_fields JSON if exists
            custom_blob = personal_meta.get('personal_custom_fields') if isinstance(personal_meta, dict) else None
            if custom_blob and isinstance(custom_blob, (str, dict)):
                try:
                    import json
                    if isinstance(custom_blob, str):
                        custom_blob = json.loads(custom_blob)
                    for k, v in custom_blob.items():
                        if k in ['personal_notes', 'user_review'] and v:
                            combined[k] = v
                except Exception:
                    pass
        setattr(book, 'reading_status', combined.get('reading_status', 'plan_to_read'))
        setattr(book, 'ownership_status', combined.get('ownership_status', 'owned'))
        setattr(book, 'start_date', combined.get('start_date'))
        setattr(book, 'finish_date', combined.get('finish_date'))
        setattr(book, 'user_rating', combined.get('user_rating'))
        setattr(book, 'personal_notes', combined.get('personal_notes'))
        setattr(book, 'review', combined.get('user_review') or combined.get('review'))  # Map user_review back to review
        setattr(book, 'date_added', combined.get('date_added'))
        setattr(book, 'want_to_read', relationship_data.get('reading_status') == 'plan_to_read')
        setattr(book, 'library_only', relationship_data.get('reading_status') == 'library_only')
        
        # Handle location information
        locations = [loc for loc in (locations_data or []) if loc and loc.get('id') and loc.get('name')]
        location_id = None
        
        debug_log(f"Processing locations for book: filtered_locations={locations}, relationship_data location_id={relationship_data.get('location_id')}", "RELATIONSHIP")
        
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
                    from app.utils.safe_kuzu_manager import safe_get_connection
                    from app.location_service import LocationService
                    
                    location_service = LocationService()
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
        
        debug_log(f"Final book location attributes: location_id={location_id}, locations={locations}", "RELATIONSHIP")
        
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
        """Get all books for a user with relationship data and locations (universal library model)."""
        try:
            # Universal library model: Get ALL books with their STORED_AT locations
            # No OWNS relationships - books are universal and stored at locations
            query = """
            MATCH (b:Book)
            OPTIONAL MATCH (b)-[stored:STORED_AT]->(l:Location)
            RETURN b, COLLECT(DISTINCT CASE WHEN l.id IS NOT NULL AND l.name IS NOT NULL THEN {id: l.id, name: l.name} ELSE NULL END) as locations
            SKIP $offset LIMIT $limit
            """
            
            result = safe_execute_kuzu_query(query, {
                "offset": offset,
                "limit": limit
            })
            results = _convert_query_result_to_list(result)
            
            books = []
            for result in results:
                if 'col_0' in result:
                    book_data = result['col_0']
                    locations_data = result.get('col_1', []) or []
                    
                    # Filter out null/empty locations
                    valid_locations = [loc for loc in locations_data if loc and loc.get('id') and loc.get('name')]
                    
                    # No relationship data in universal library - books don't belong to users
                    relationship_data = {}
                    
                    book = self._create_enriched_book(book_data, relationship_data, valid_locations)
                    books.append(book)
            
            return books
            
        except Exception as e:
            traceback.print_exc()
            return []
    
    async def get_book_by_id_for_user(self, book_id: str, user_id: str) -> Optional[Book]:
        """Get a specific book for a user with relationship data (universal library model)."""
        try:
            # Universal library model: Get ANY book with its STORED_AT locations
            # No OWNS relationships - books are universal and stored at locations
            query = """
            MATCH (b:Book {id: $book_id})
            OPTIONAL MATCH (b)-[stored:STORED_AT]->(l:Location)
            RETURN b, COLLECT(DISTINCT CASE WHEN l.id IS NOT NULL AND l.name IS NOT NULL THEN {id: l.id, name: l.name} ELSE NULL END) as locations
            """
            
            result = safe_execute_kuzu_query(query, {
                "book_id": book_id
            })
            results = _convert_query_result_to_list(result)
            
            if not results:
                return None
                
            result = results[0]
            if 'col_0' not in result:
                return None
                
            book_data = result['col_0']
            locations_data = result.get('col_1', []) or []
            
            # Filter out null/empty locations
            valid_locations = [loc for loc in locations_data if loc and loc.get('id') and loc.get('name')]
            
            # No relationship data in universal library - books don't belong to users
            relationship_data = {}
            
            # Create enriched book with user-specific attributes
            book = self._create_enriched_book(book_data, relationship_data, valid_locations)
            
            # No custom metadata in universal library (books don't belong to users)
            setattr(book, 'custom_metadata', {})
                    
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
        """Add a book to user's library with custom metadata support (sync version).
        
        NOTE: In universal library mode, this method is deprecated.
        Books are shared and don't belong to individual users.
        Use location assignment instead via LocationService.
        """
        # Universal library mode - books don't belong to users
        # This method is deprecated but kept for backward compatibility
        logger.warning(f"[UNIVERSAL_LIBRARY] add_book_to_user_library_sync called but deprecated in universal library mode")
        return True  # Always return success to avoid breaking existing code
    
    async def update_user_book_relationship(self, user_id: str, book_id: str, updates: Dict[str, Any]) -> bool:
        """Persist personal (user-specific) fields.

        Even in "universal library" mode we still allow a single user overlay
        using the OWNS relationship so that personal notes & reviews persist.
        If OWNS does not yet exist it will be created.
        """
        try:
            print(f"🔍 [DEBUG] update_user_book_relationship called: user_id={user_id}, book_id={book_id}, updates={updates}")
            if not updates:
                return True
            # Map review key to user_review for storage
            storage_updates = {}
            for k, v in updates.items():
                if k == 'review':
                    storage_updates['user_review'] = v
                else:
                    storage_updates[k] = v
            # Always bump updated_at
            storage_updates['updated_at'] = datetime.utcnow()
            # Prepare SET clauses
            set_parts = []
            # Provide a creation timestamp param for ON CREATE to avoid unsupported DB functions
            params = {'user_id': user_id, 'book_id': book_id, 'date_added': datetime.utcnow()}
            for key, value in storage_updates.items():
                set_parts.append(f"owns.{key} = ${key}")
                params[key] = value
            set_clause = ", ".join(set_parts)
            query = f"""
            MATCH (u:User {{id: $user_id}}), (b:Book {{id: $book_id}})
            MERGE (u)-[owns:OWNS]->(b)
            ON CREATE SET owns.date_added = $date_added
            SET {set_clause}
            RETURN owns
            """
            safe_execute_kuzu_query(query, params)
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
            
            safe_execute_kuzu_query(delete_rel_query, {
                "user_id": user_id,
                "book_id": book_id
            })
            
            return True
            
        except Exception as e:
            traceback.print_exc()
            return False
    
    async def get_all_books_with_user_overlay(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all books with user-specific overlay data (universal library model)."""
        try:
            # Universal library - get ALL books with their STORED_AT locations
            # No OWNS relationships - books are universal and stored at locations
            query = """
            MATCH (b:Book)
            OPTIONAL MATCH (b)-[stored:STORED_AT]->(l:Location)
            RETURN b, COLLECT(DISTINCT CASE WHEN l.id IS NOT NULL AND l.name IS NOT NULL THEN {id: l.id, name: l.name} ELSE NULL END) as locations
            """
            
            result = safe_execute_kuzu_query(query, {})
            results = _convert_query_result_to_list(result)
            
            logger.info(f"[RELATIONSHIP_SERVICE] Raw query returned {len(results)} results for universal library")
            
            # Convert all books to dictionaries with user overlay
            book_dicts = []
            for i, result_row in enumerate(results):
                try:
                    if 'col_0' not in result_row:
                        logger.warning(f"[RELATIONSHIP_SERVICE] Row {i} missing col_0 (book data)")
                        continue
                        
                    book_data = result_row['col_0']
                    locations_data = result_row.get('col_1', []) or []
                    
                    # No relationship data in universal library - books don't belong to users
                    relationship_data = {}
                    
                    # Create enriched book object
                    book = self._create_enriched_book(book_data, relationship_data, locations_data)
                    
                    # Convert to dictionary
                    if hasattr(book, '__dict__'):
                        book_dict = book.__dict__.copy()
                    else:
                        # Fallback for non-standard book objects
                        book_dict = {
                            'id': getattr(book, 'id', ''),
                            'title': getattr(book, 'title', ''),
                            'uid': getattr(book, 'id', ''),
                        }
                    
                    # Ensure uid is available (some templates expect this)
                    if 'id' in book_dict and 'uid' not in book_dict:
                        book_dict['uid'] = book_dict['id']
                    
                    book_dicts.append(book_dict)
                    
                except Exception as e:
                    logger.error(f"[RELATIONSHIP_SERVICE] Error processing book {i}: {e}")
                    continue
            
            logger.info(f"[RELATIONSHIP_SERVICE] Successfully processed {len(book_dicts)} books for universal library")
            return book_dicts
            
        except Exception as e:
            logger.error(f"[RELATIONSHIP_SERVICE] Error in get_all_books_with_user_overlay: {e}")
            traceback.print_exc()
            return []
    
    def get_book_by_uid_sync(self, uid: str, user_id: str) -> Optional[Book]:
        """Get a book by UID with user overlay data.

        Attempts to pull personal data from OWNS relationship if present; falls
        back to HAS_PERSONAL_METADATA if available. Works in both legacy and
        universal modes.
        """
        query = """
        MATCH (b:Book {id: $book_id})
        OPTIONAL MATCH (b)-[stored:STORED_AT]->(l:Location)
        OPTIONAL MATCH (u:User {id: $user_id})-[owns:OWNS]->(b)
        OPTIONAL MATCH (u)-[pm:HAS_PERSONAL_METADATA]->(b)
        RETURN b,
               COLLECT(DISTINCT CASE WHEN l.id IS NOT NULL AND l.name IS NOT NULL THEN {id: l.id, name: l.name} ELSE NULL END) as locations,
               owns, pm
        """

        raw = safe_execute_kuzu_query(query, {"book_id": uid, "user_id": user_id})
        results = _convert_query_result_to_list(raw)
        if not results:
            return None
        row = results[0]
        if 'col_0' not in row:
            return None
        book_data = row['col_0']
        locations_data = row.get('col_1', []) or []
        relationship_data = row.get('col_2') or {}
        personal_meta = row.get('col_3') or {}
        valid_locations = [loc for loc in locations_data if loc and loc.get('id') and loc.get('name')]
        debug_log(f"Book {uid} locations from STORED_AT: {valid_locations}", "RELATIONSHIP")
        book = self._create_enriched_book(book_data, relationship_data, valid_locations, personal_meta)
        setattr(book, 'custom_metadata', {})
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
        """Get books recently added to the user's want-to-read list.
        
        NOTE: In universal library mode, this method is deprecated.
        Books are shared and don't have user-specific reading statuses.
        """
        # Universal library mode - books don't have user-specific reading statuses
        # This method is deprecated but kept for backward compatibility
        logger.warning(f"[UNIVERSAL_LIBRARY] get_recently_added_want_to_read_books called but deprecated in universal library mode")
        return []  # Return empty list to avoid breaking existing code

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
