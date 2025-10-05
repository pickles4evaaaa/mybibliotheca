"""
Kuzu Relationship Service

Handles user-book relationships, custom metadata, and ownership tracking using Kuzu.
Focused responsibility: User-book relationship management and metadata.

This service has been migrated to use the SafeKuzuManager pattern for
improved thread safety and connection management.
"""

import json
import traceback
import os
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timezone

from ..domain.models import Book, UserBookRelationship, ReadingStatus, OwnershipStatus, Person, BookContribution, ContributionType
from ..infrastructure.kuzu_repositories import KuzuUserRepository
from ..infrastructure.kuzu_graph import safe_execute_kuzu_query, safe_get_kuzu_connection
from .kuzu_async_helper import run_async
from .kuzu_book_service import KuzuBookService
from ..debug_system import debug_log
import logging

logger = logging.getLogger(__name__)

def _safe_get_row_value(row: Any, index: int) -> Any:
    """Safely extract a value from a KuzuDB row at the given index."""
    if isinstance(row, list):
        return row[index] if index < len(row) else None
    elif isinstance(row, dict):
        keys = list(row.keys())
        return row[keys[index]] if index < len(keys) else None
    else:
        try:
            return row[index]  # type: ignore
        except (IndexError, KeyError, TypeError):
            return None


def _convert_query_result_to_list(result) -> List[Dict[str, Any]]:
    """
    Convert KuzuDB QueryResult to list of dictionaries (matching old graph_storage.query format).
    """
    rows = []
    try:
        # Check if result has the iterator interface
        if hasattr(result, 'has_next') and hasattr(result, 'get_next'):
            while result.has_next():
                row = result.get_next()
                # Convert row to dict
                if len(row) == 1:
                    # Single column result
                    rows.append({'result': _safe_get_row_value(row, 0)})
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
        combined: Dict[str, Any] = {}
        # Legacy OWNS data (reading_status, ownership_status, user_rating, etc.)
        combined.update(relationship_data or {})
        # Personal metadata (HAS_PERSONAL_METADATA) stores json blob in personal_custom_fields; flatten known fields
        if personal_meta:
            # Accept direct fields if present
            for key in ['personal_notes', 'user_review', 'user_rating', 'reading_status', 'ownership_status', 'start_date', 'finish_date']:
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
                        # Promote known keys; keep others as custom_*
                        if k in ['personal_notes', 'user_review', 'reading_status', 'ownership_status', 'user_rating', 'start_date', 'finish_date', 'progress_ms', 'last_listened_at', 'progress_percentage'] and v not in (None, ''):
                            combined[k] = v
                except Exception:
                    pass
        # If key personal fields still missing, consult personal_metadata_service directly (ensures coverage when OWNS absent and pm not returned)
        try:
            if not any(k in combined for k in ('personal_notes', 'user_review', 'reading_status', 'ownership_status', 'user_rating', 'start_date', 'finish_date')):
                from .personal_metadata_service import personal_metadata_service
                pm = personal_metadata_service.get_personal_metadata(self.user_id, str(getattr(book, 'id', '')))
                for k in ['personal_notes', 'user_review', 'reading_status', 'ownership_status', 'user_rating', 'start_date', 'finish_date']:
                    v = pm.get(k)
                    if v not in (None, ''):
                        combined[k] = v
        except Exception:
            pass
        # Apply resolved personal fields without forcing defaults
        setattr(book, 'reading_status', combined.get('reading_status'))
        setattr(book, 'ownership_status', combined.get('ownership_status', 'owned'))
        setattr(book, 'start_date', combined.get('start_date'))
        setattr(book, 'finish_date', combined.get('finish_date'))
        setattr(book, 'user_rating', combined.get('user_rating'))
        # Optional progress fields for UI hints
        setattr(book, 'progress_ms', combined.get('progress_ms'))
        setattr(book, 'last_listened_at', combined.get('last_listened_at'))
        # Expose progress percentage if present
        if 'progress_percentage' in combined:
            setattr(book, 'progress_percentage', combined.get('progress_percentage'))
        setattr(book, 'personal_notes', combined.get('personal_notes'))
        setattr(book, 'review', combined.get('user_review') or combined.get('review'))  # Map user_review back to review
        # In universal library mode, "date added" is a global property of the Book (creation time),
        # not user-specific. Prefer book.created_at, fall back to any legacy relationship date if present.
        try:
            book_created = getattr(book, 'created_at', None)
        except Exception:
            book_created = None
        setattr(book, 'date_added', book_created or combined.get('date_added'))

        # Compute convenience booleans from the resolved reading_status
        rs_val = (combined.get('reading_status')
                  if combined.get('reading_status') is not None
                  else (relationship_data.get('reading_status') if isinstance(relationship_data, dict) else None))
        setattr(book, 'want_to_read', rs_val == 'plan_to_read')
        setattr(book, 'library_only', rs_val == 'library_only')
        
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
        
        # media_type comes from Book node only - no personal metadata involvement
        # Let the Book's native media_type property show through without any override
        
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

    async def get_recently_added_books(self, limit: int = 10) -> List[Book]:
        """Get the most recently created books (global scope)."""
        try:
            query = """
            MATCH (b:Book)
            OPTIONAL MATCH (b)-[stored:STORED_AT]->(l:Location)
            WITH b,
                 COLLECT(DISTINCT CASE WHEN l.id IS NOT NULL AND l.name IS NOT NULL THEN {id: l.id, name: l.name} ELSE NULL END) AS locations
            ORDER BY
                 CASE
                     WHEN b.created_at IS NOT NULL THEN b.created_at
                     ELSE b.updated_at
                 END DESC,
                 b.updated_at DESC,
                 lower(COALESCE(b.normalized_title, b.title, '')) ASC
            LIMIT $limit
            RETURN b, locations
            """

            result = safe_execute_kuzu_query(query, {
                "limit": limit
            })
            rows = _convert_query_result_to_list(result)

            books: List[Book] = []
            for row in rows:
                book_data = row.get('col_0')
                if not book_data:
                    continue
                locations_data = row.get('col_1', []) or []
                valid_locations = [loc for loc in locations_data if loc and loc.get('id') and loc.get('name')]
                relationship_data: Dict[str, Any] = {}
                book = self._create_enriched_book(book_data, relationship_data, valid_locations)
                books.append(book)

            return books

        except Exception:
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
        """Persist personal (user-specific) fields using HAS_PERSONAL_METADATA.

        Fully replaces legacy OWNS usage. Accepts legacy field names (review -> user_review)
        and stores everything via personal_metadata_service.
        """
        try:
            if not updates:
                return True
            from .personal_metadata_service import personal_metadata_service

            # Map legacy keys
            mapped: Dict[str, Any] = {}
            for k, v in updates.items():
                if k == 'review':
                    mapped['user_review'] = v
                else:
                    mapped[k] = v

            # Extract standard fields we persist explicitly (others go into personal_custom_fields JSON)
            direct_fields = {k: mapped.pop(k) for k in list(mapped.keys()) if k in {
                'personal_notes', 'user_review', 'start_date', 'finish_date'
            }}

            # Convert date strings to datetime if needed
            def _parse_date(val):
                if isinstance(val, datetime):
                    return val
                if isinstance(val, date):
                    return datetime(val.year, val.month, val.day)
                if isinstance(val, str):
                    try:
                        return datetime.fromisoformat(val)
                    except Exception:
                        return None
                return None

            start_dt = _parse_date(direct_fields.get('start_date')) if 'start_date' in direct_fields else None
            finish_dt = _parse_date(direct_fields.get('finish_date')) if 'finish_date' in direct_fields else None
            if 'start_date' in direct_fields:
                direct_fields.pop('start_date', None)
            if 'finish_date' in direct_fields:
                direct_fields.pop('finish_date', None)

            # Remaining mapped keys (reading_status, ownership_status, user_rating, etc.) -> custom blob
            custom_updates = mapped.copy()

            # Merge any additional custom fields (if user supplied arbitrary keys)
            existing = personal_metadata_service.get_personal_metadata(user_id, book_id)
            existing_blob = {}
            # existing already returns flattened; build blob excluding core fields
            for k, v in existing.items():
                if k not in ('personal_notes', 'user_review', 'start_date', 'finish_date'):
                    existing_blob[k] = v
            existing_blob.update(custom_updates)
            custom_updates = existing_blob

            personal_metadata_service.update_personal_metadata(
                user_id,
                book_id,
                personal_notes=direct_fields.get('personal_notes'),
                user_review=direct_fields.get('user_review'),
                start_date=start_dt,
                finish_date=finish_dt,
                custom_updates=custom_updates,
                merge=True,
            )
            return True
        except Exception:
            traceback.print_exc()
            return False
    
    async def remove_book_from_user_library(self, user_id: str, book_id: str) -> bool:
        """Remove user-specific metadata for a book (OWNS deprecated).

        In universal library mode we generally keep the book; this becomes a metadata clear.
        """
        try:
            from .personal_metadata_service import personal_metadata_service
            # Overwrite with empty metadata (could also delete relationship)
            safe_execute_kuzu_query(
                f"""
                MATCH (u:User {{id: $user_id}})-[r:HAS_PERSONAL_METADATA]->(b:Book {{id: $book_id}})
                DELETE r
                """ , {"user_id": user_id, "book_id": book_id})
            # Optionally also drop legacy OWNS if still present (cleanup phase)
            try:
                safe_execute_kuzu_query("MATCH (u:User {id: $uid})-[o:OWNS]->(b:Book {id: $bid}) DELETE o", {"uid": user_id, "bid": book_id})
            except Exception:
                pass
            return True
        except Exception:
            traceback.print_exc()
            return False
    
    async def get_all_books_with_user_overlay(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all books with user-specific overlay data (universal library model)."""
        try:
            # Universal library base: get ALL books with their STORED_AT locations
            # Enhance with user overlay: personal metadata (OWNS fully deprecated)
            query = """
            MATCH (b:Book)
            OPTIONAL MATCH (b)-[stored:STORED_AT]->(l:Location)
            WITH b,
                COLLECT(DISTINCT CASE WHEN l.id IS NOT NULL AND l.name IS NOT NULL THEN {id: l.id, name: l.name} ELSE NULL END) AS locations
            OPTIONAL MATCH (u:User {id: $user_id})-[pm:HAS_PERSONAL_METADATA]->(b)
            RETURN b, locations, pm
            """

            result = safe_execute_kuzu_query(query, {"user_id": user_id})
            results = _convert_query_result_to_list(result)

            logger.info(f"[RELATIONSHIP_SERVICE] Raw query returned {len(results)} results with optional user overlay")

            # Convert all books to dictionaries with user overlay
            book_dicts = []
            for i, result_row in enumerate(results):
                try:
                    if 'col_0' not in result_row:
                        logger.warning(f"[RELATIONSHIP_SERVICE] Row {i} missing col_0 (book data)")
                        continue

                    book_data = result_row['col_0']
                    locations_data = result_row.get('col_1', []) or []
                    # No legacy relationship data anymore
                    relationship_data = {}
                    personal_meta = result_row.get('col_2') or {}

                    # Create enriched book object (filters locations internally)
                    book = self._create_enriched_book(book_data, relationship_data, locations_data, personal_meta)

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

            logger.info(f"[RELATIONSHIP_SERVICE] Successfully processed {len(book_dicts)} books with personal metadata overlay")
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
        OPTIONAL MATCH (u:User {id: $user_id})-[pm:HAS_PERSONAL_METADATA]->(b)
        RETURN b,
               COLLECT(DISTINCT CASE WHEN l.id IS NOT NULL AND l.name IS NOT NULL THEN {id: l.id, name: l.name} ELSE NULL END) as locations,
               pm
        """
        raw = safe_execute_kuzu_query(query, {"book_id": uid, "user_id": user_id})
        results = _convert_query_result_to_list(raw)
        logger.debug(f"[RELATIONSHIP_SERVICE][BOOK_LOOKUP] Raw rows for id={uid}: {results}")
        if not results:
            logger.debug(f"[RELATIONSHIP_SERVICE][BOOK_LOOKUP] No results for book id={uid} user={user_id} db_path={os.getenv('KUZU_DB_PATH')}")
            return None
        row = results[0]
        # Normal expected key pattern is col_0 / col_1 / col_2. Fallbacks handle alternate shapes.
        if 'col_0' not in row:
            # Attempt graceful recovery: pick first value-like entry
            possible_keys = [k for k in row.keys() if k.startswith('col_')]
            if possible_keys:
                possible_keys.sort()
                book_data = row[possible_keys[0]]
                logger.debug(f"[RELATIONSHIP_SERVICE][BOOK_LOOKUP] Missing col_0 key; recovered using {possible_keys[0]} for book id={uid}")
            elif 'result' in row:
                book_data = row['result']
                logger.debug(f"[RELATIONSHIP_SERVICE][BOOK_LOOKUP] Used 'result' key fallback for book id={uid}")
            else:
                logger.debug(f"[RELATIONSHIP_SERVICE][BOOK_LOOKUP] Row lacked expected keys for book id={uid}: keys={list(row.keys())}")
                return None
        else:
            book_data = row['col_0']
        locations_data = row.get('col_1', []) or []
        relationship_data = {}
        personal_meta = row.get('col_2') or {}
        valid_locations = [loc for loc in locations_data if loc and loc.get('id') and loc.get('name')]
        debug_log(f"Book {uid} locations from STORED_AT: {valid_locations}", "RELATIONSHIP")
        # Ensure minimal dict shape if book_data is a bare id or missing expected keys
        if isinstance(book_data, str):
            book_data = { 'id': book_data }
        if isinstance(book_data, dict) and 'title' not in book_data:
            # Fallback fetch for core fields if only id present
            try:
                fallback_q = "MATCH (b:Book {id: $bid}) RETURN b.title, b.normalized_title, b.created_at, b.updated_at"
                fb_raw = safe_execute_kuzu_query(fallback_q, {"bid": uid})
                fb_rows = _convert_query_result_to_list(fb_raw)
                if fb_rows:
                    fr = fb_rows[0]
                    # Map col indices defensively
                    if isinstance(fr, dict):
                        title = fr.get('col_0') or fr.get('title') or ''
                        norm = fr.get('col_1') or (title.lower() if title else '')
                        book_data.setdefault('title', title)
                        book_data.setdefault('normalized_title', norm)
            except Exception as _fb_err:
                logger.debug(f"[RELATIONSHIP_SERVICE][BOOK_LOOKUP] Fallback field fetch failed for id={uid}: {_fb_err}")
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
    
    def get_recently_added_books_sync(self, limit: int = 10) -> List[Book]:
        """Sync wrapper for get_recently_added_books."""
        return run_async(self.get_recently_added_books(limit))

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

    # ---------------- Pagination helpers for library -----------------
    async def get_books_with_user_overlay_paginated(self, user_id: str, limit: int, offset: int, sort: str = 'title_asc') -> List[Dict[str, Any]]:
        """Fetch a page of books with personal overlay.

        Supports basic sorting by title asc/desc. More complex sorts (author) are handled client-side.
        """
        try:
            sort_clause = 'ORDER BY lower(b.normalized_title) ASC'
            if sort == 'title_desc':
                sort_clause = 'ORDER BY lower(b.normalized_title) DESC'
            # Fallback to title if normalized_title missing
            query = f"""
            MATCH (b:Book)
            OPTIONAL MATCH (b)-[stored:STORED_AT]->(l:Location)
            OPTIONAL MATCH (u:User {{id: $user_id}})-[pm:HAS_PERSONAL_METADATA]->(b)
            WITH b,
                 COLLECT(DISTINCT CASE WHEN l.id IS NOT NULL AND l.name IS NOT NULL THEN {{id: l.id, name: l.name}} ELSE NULL END) as locations,
                 pm
            {sort_clause}
            SKIP $offset LIMIT $limit
            RETURN b, locations, pm
            """
            result = safe_execute_kuzu_query(query, {"user_id": user_id, "offset": offset, "limit": limit})
            rows = _convert_query_result_to_list(result)
            books: List[Dict[str, Any]] = []
            for row in rows:
                try:
                    book_data = row.get('col_0')
                    if not book_data:
                        # Skip rows without a book payload
                        continue
                    if isinstance(book_data, str):
                        book_data = {'id': book_data}
                    locations_data = row.get('col_1', []) or []
                    pm = row.get('col_2') or {}
                    book = self._create_enriched_book(book_data, {}, locations_data, pm)
                    books.append(book.__dict__.copy() if hasattr(book, '__dict__') else {
                        'id': getattr(book, 'id', ''),
                        'uid': getattr(book, 'id', ''),
                        'title': getattr(book, 'title', '')
                    })
                except Exception:
                    continue
            return books
        except Exception as e:
            logger.error(f"[RELATIONSHIP_SERVICE] get_books_with_user_overlay_paginated error: {e}")
            return []

    def get_books_with_user_overlay_paginated_sync(self, user_id: str, limit: int, offset: int, sort: str = 'title_asc') -> List[Dict[str, Any]]:
        return run_async(self.get_books_with_user_overlay_paginated(user_id, limit, offset, sort))

    def get_total_book_count_sync(self) -> int:
        try:
            res = safe_execute_kuzu_query("MATCH (b:Book) RETURN COUNT(b) as c", {})
            rows = _convert_query_result_to_list(res)
            if rows:
                first = rows[0]
                val = first.get('c') or first.get('col_0') or first.get('result')
                return int(val) if isinstance(val, (int, float, str)) else 0
        except Exception as e:
            logger.warning(f"[RELATIONSHIP_SERVICE] total count error: {e}")
        return 0

    def get_library_status_counts_sync(self, user_id: str) -> Dict[str, int]:
        """Compute global counts aligned with library filters from user overlay data.

        Uses get_all_books_with_user_overlay_sync so that books without a personal
        record remain with empty reading_status (user has not set a personal status).
        """
        counts: Dict[str, int] = {
            'read': 0,
            'currently_reading': 0,
            'on_hold': 0,
            'plan_to_read': 0,
            'wishlist': 0,
        }
        try:
            books = self.get_all_books_with_user_overlay_sync(user_id)
            for book in books or []:
                # Support both dicts and objects
                rs = (book.get('reading_status') if isinstance(book, dict) else getattr(book, 'reading_status', None))
                owner = (book.get('ownership_status') if isinstance(book, dict) else getattr(book, 'ownership_status', None))
                rs = rs.strip().lower() if isinstance(rs, str) else rs
                owner = owner.strip().lower() if isinstance(owner, str) else owner

                # Normalize common synonyms/variants
                if rs in (None, '', 'unknown', 'library_only'):
                    rs = ''  # Empty/default: no personal status set
                elif rs in ('reading', 'currently reading'):
                    rs = 'currently_reading'
                elif rs in ('onhold', 'on-hold', 'paused'):
                    rs = 'on_hold'
                elif rs in ('finished', 'complete', 'completed'):
                    rs = 'read'
                elif rs in ('want_to_read', 'wishlist_reading'):
                    rs = 'plan_to_read'

                if rs == 'read':
                    counts['read'] += 1
                elif rs in ('reading', 'currently_reading'):
                    counts['currently_reading'] += 1
                elif rs == 'on_hold':
                    counts['on_hold'] += 1
                elif rs == 'plan_to_read':
                    counts['plan_to_read'] += 1

                if owner == 'wishlist':
                    counts['wishlist'] += 1
            # No fallback: empty/default statuses are not counted under plan_to_read
        except Exception as e:
            logger.error(f"[RELATIONSHIP_SERVICE] get_library_status_counts_sync error: {e}")
        return counts
