"""
Kuzu Relationship Service

Handles user-book relationships, custom metadata, and ownership tracking using Kuzu.
Focused responsibility: User-book relationship management and metadata.
"""

import json
import traceback
from typing import List, Optional, Dict, Any
from datetime import datetime, date

from ..domain.models import Book, UserBookRelationship, ReadingStatus, OwnershipStatus
from ..infrastructure.kuzu_repositories import KuzuUserRepository
from ..infrastructure.kuzu_graph import get_graph_storage
from .kuzu_async_helper import run_async
from .kuzu_book_service import KuzuBookService


class KuzuRelationshipService:
    """Service for user-book relationship and metadata management."""
    
    def __init__(self):
        self.graph_storage = get_graph_storage()
        self.user_repo = KuzuUserRepository()
        self.book_service = KuzuBookService()
    
    def _create_enriched_book(self, book_data: Dict[str, Any], relationship_data: Dict[str, Any]) -> Book:
        """Create an enriched Book object with user-specific attributes."""
        # Convert book data to Book object using the book service
        book = self.book_service._dict_to_book(book_data)
        
        # Add user-specific attributes dynamically using setattr
        setattr(book, 'reading_status', relationship_data.get('reading_status', 'plan_to_read'))
        setattr(book, 'ownership_status', relationship_data.get('ownership_status', 'owned'))
        setattr(book, 'start_date', relationship_data.get('start_date'))
        setattr(book, 'finish_date', relationship_data.get('finish_date'))
        setattr(book, 'user_rating', relationship_data.get('user_rating'))
        setattr(book, 'personal_notes', relationship_data.get('personal_notes'))
        setattr(book, 'date_added', relationship_data.get('date_added'))
        setattr(book, 'want_to_read', relationship_data.get('reading_status') == 'plan_to_read')
        setattr(book, 'library_only', relationship_data.get('reading_status') == 'library_only')
        
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
        """Get all books for a user with relationship data."""
        try:
            # Use Kuzu's query method to get user's books via OWNS relationships
            query = """
            MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book)
            RETURN b, owns
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
                    book = self._create_enriched_book(book_data, relationship_data)
                    books.append(book)
            
            return books
            
        except Exception as e:
            print(f"❌ [GET_BOOKS_FOR_USER] Error getting books for user {user_id}: {e}")
            traceback.print_exc()
            return []
    
    async def get_book_by_id_for_user(self, book_id: str, user_id: str) -> Optional[Book]:
        """Get a specific book for a user with relationship data."""
        try:
            # Use Kuzu's query method to get the book with user relationship data
            query = """
            MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book {id: $book_id})
            RETURN b, owns
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
            
            # Create enriched book with user-specific attributes
            book = self._create_enriched_book(book_data, relationship_data)
            
            # Load custom metadata for this user-book combination
            custom_metadata = self._load_custom_metadata(relationship_data)
            setattr(book, 'custom_metadata', custom_metadata)
                    
            return book
            
        except Exception as e:
            print(f"❌ [GET_BOOK_FOR_USER] Error getting book {book_id} for user {user_id}: {e}")
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
                    print(f"🔍 [LOAD_CUSTOM_META] Loaded from OWNS.custom_metadata: {custom_metadata}")
                except (json.JSONDecodeError, TypeError) as e:
                    print(f"❌ Error parsing custom_metadata JSON: {e}")
                    custom_metadata = {}
            
            return custom_metadata
            
        except Exception as e:
            print(f"❌ [LOAD_CUSTOM_META] Error loading custom metadata: {e}")
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
                'user_id': str(user_id),
                'book_id': str(book_id),
                'reading_status': reading_status_enum.value if hasattr(reading_status_enum, 'value') else str(reading_status_enum),
                'ownership_status': ownership_status_enum.value if hasattr(ownership_status_enum, 'value') else str(ownership_status_enum),
                'date_added': datetime.utcnow().isoformat()
            }
            
            # Add locations if provided
            if locations:
                rel_data['locations'] = json.dumps(locations)  # Store as JSON string
                if len(locations) > 0:
                    rel_data['primary_location_id'] = locations[0]
            
            # Add custom metadata as JSON
            if custom_metadata:
                rel_data['custom_metadata'] = json.dumps(custom_metadata)
            
            # Create the relationship
            success = self.graph_storage.create_relationship(
                'User', user_id, 'OWNS', 'Book', book_id, rel_data
            )
            
            if success:
                print(f"✅ [ADD_TO_LIBRARY] Added book {book_id} to user {user_id} library with status {reading_status}")
                if custom_metadata:
                    print(f"✅ [ADD_TO_LIBRARY] Attached custom metadata: {custom_metadata}")
            
            return success
            
        except Exception as e:
            print(f"❌ [ADD_TO_LIBRARY] Error adding book to user library: {e}")
            traceback.print_exc()
            return False
    
    async def update_user_book_relationship(self, user_id: str, book_id: str, updates: Dict[str, Any]) -> bool:
        """Update the relationship between a user and book."""
        try:
            print(f"🔗 [UPDATE_RELATIONSHIP] Updating user {user_id} - book {book_id} with: {updates}")
            
            # Get the current relationship
            query = """
            MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book {id: $book_id})
            RETURN owns
            """
            
            results = self.graph_storage.query(query, {
                "user_id": user_id,
                "book_id": book_id
            })
            
            if not results or 'col_0' not in results[0]:
                print(f"❌ [UPDATE_RELATIONSHIP] No relationship found between user {user_id} and book {book_id}")
                return False
            
            current_rel = results[0]['col_0']
            
            # Merge updates with current data
            updated_rel = {**current_rel, **updates}
            
            # Handle custom metadata specially
            if 'custom_metadata' in updates:
                if isinstance(updates['custom_metadata'], dict):
                    updated_rel['custom_metadata'] = json.dumps(updates['custom_metadata'])
                else:
                    updated_rel['custom_metadata'] = updates['custom_metadata']
            
            # Update the relationship
            update_query = """
            MATCH (u:User {id: $user_id})-[owns:OWNS]->(b:Book {id: $book_id})
            SET owns = $rel_data
            """
            
            self.graph_storage.query(update_query, {
                "user_id": user_id,
                "book_id": book_id,
                "rel_data": updated_rel
            })
            
            print(f"✅ [UPDATE_RELATIONSHIP] Successfully updated relationship")
            return True
            
        except Exception as e:
            print(f"❌ [UPDATE_RELATIONSHIP] Error updating relationship: {e}")
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
            
            print(f"✅ [REMOVE_FROM_LIBRARY] Successfully removed book from library")
            return True
            
        except Exception as e:
            print(f"❌ [REMOVE_FROM_LIBRARY] Error removing book from library: {e}")
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
            print(f"❌ [GET_ALL_WITH_OVERLAY] Error getting all books with user overlay for user {user_id}: {e}")
            traceback.print_exc()
            return []
    
    def get_book_by_uid_sync(self, uid: str, user_id: str) -> Optional[Book]:
        """Get a book by UID with user overlay data - sync wrapper."""
        # Use similar query to get_all_books_with_user_overlay but for single book
        query = """
        MATCH (b:Book {id: $book_id})
        OPTIONAL MATCH (u:User {id: $user_id})-[owns:OWNS]->(b)
        RETURN b, owns
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
        
        # Create enriched book with user-specific attributes
        book = self._create_enriched_book(book_data, relationship_data)
        
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
