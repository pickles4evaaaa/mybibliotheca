"""
Kuzu Person Service

Handles author/contributor/person management using Kuzu.
Focused responsibility: Person entity management and author relationships.

This service has been migrated to use the SafeKuzuManager pattern for
improved thread safety and connection management.
"""

import traceback
from typing import List, Optional, Dict, Any

from ..infrastructure.kuzu_repositories import KuzuPersonRepository
from ..infrastructure.kuzu_graph import safe_execute_kuzu_query, safe_get_kuzu_connection
from .kuzu_async_helper import run_async
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


class KuzuPersonService:
    """
    Service for person/author management operations with thread-safe operations.
    
    This service has been migrated to use the SafeKuzuManager pattern for
    improved thread safety and connection management.
    """
    
    def __init__(self, user_id: Optional[str] = None):
        """
        Initialize person service with thread-safe database access.
        
        Args:
            user_id: User identifier for tracking and isolation
        """
        self.user_id = user_id or "person_service"
        self.person_repo = KuzuPersonRepository()
    
    async def list_all_persons(self) -> List[Dict[str, Any]]:
        """Get all persons."""
        try:
            # The method exists in KuzuBookRepository, not KuzuPersonRepository
            from ..infrastructure.kuzu_repositories import KuzuBookRepository
            book_repo = KuzuBookRepository()
            return await book_repo.get_all_persons()
        except Exception as e:
            return []
    
    async def get_person_by_id(self, person_id: str) -> Optional[Dict[str, Any]]:
        """Get a person by ID."""
        try:
            return await self.person_repo.get_by_id(person_id)
        except Exception as e:
            return None
    
    def get_person_by_id_sync(self, person_id: str) -> Optional[Dict[str, Any]]:
        """Get a person by ID (sync version with detailed debugging)."""
        try:
            # First, let's see what persons exist in the database
            debug_query = "MATCH (p:Person) RETURN p.id, p.name LIMIT 10"
            debug_raw_result = safe_execute_kuzu_query(
                query=debug_query,
                params={},
                user_id=self.user_id,
                operation="debug_list_persons"
            )
            debug_results = _convert_query_result_to_list(debug_raw_result)
            
            query = """
            MATCH (p:Person {id: $person_id})
            RETURN p
            LIMIT 1
            """
            
            # Use safe query execution and convert result
            raw_result = safe_execute_kuzu_query(
                query=query,
                params={"person_id": person_id},
                user_id=self.user_id,
                operation="get_person_by_id_sync"
            )
            
            results = _convert_query_result_to_list(raw_result)
            
            
            if results and len(results) > 0:
                
                # Try different possible key formats
                if 'result' in results[0]:
                    person_data = dict(results[0]['result'])
                    return person_data
                elif 'col_0' in results[0]:
                    person_data = dict(results[0]['col_0'])
                    return person_data
                elif 'p' in results[0]:
                    person_data = dict(results[0]['p'])
                    return person_data
                else:
                    return None
            else:
                return None
            
            return None
            
        except Exception as e:
            return None
    
    async def create_person(self, person_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a new person."""
        try:
            # Use the person repository to create (it's a sync method)
            created_person = self.person_repo.create(person_data)
            
            if created_person:
                pass  # Person created successfully
            
            return created_person
            
        except Exception as e:
            traceback.print_exc()
            return None
    
    async def update_person(self, person_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update a person's information."""
        try:
            # Update the person node in Kuzu using safe query execution
            print(f"🔧 [PERSON_SERVICE] About to update person_id={person_id}, updates={updates}")
            
            set_clauses = []
            params = {"person_id": person_id}
            
            for key, value in updates.items():
                set_clauses.append(f"p.{key} = ${key}")
                params[key] = value
            
            update_query = f"""
            MATCH (p:Person {{id: $person_id}})
            SET {', '.join(set_clauses)}
            RETURN p
            """
            
            # Use safe query execution
            raw_result = safe_execute_kuzu_query(
                query=update_query,
                params=params,
                user_id=self.user_id,
                operation="update_person"
            )
            
            results = _convert_query_result_to_list(raw_result)
            print(f"🔧 [PERSON_SERVICE] Update query returned: {len(results) > 0} results")
            
            if results:
                print(f"🔧 [PERSON_SERVICE] Success, fetching updated person data...")
                # Return updated person data
                updated_person = await self.get_person_by_id(person_id)
                print(f"🔧 [PERSON_SERVICE] Fetched updated person: {updated_person is not None}")
                return updated_person
            else:
                print(f"🔧 [PERSON_SERVICE] No results from update, returning None")
                return None
                
        except Exception as e:
            print(f"🔧 [PERSON_SERVICE] Exception in update_person: {type(e).__name__}: {str(e)}")
            traceback.print_exc()
            return None
    
    async def delete_person(self, person_id: str) -> bool:
        """Delete a person (if not referenced by any books)."""
        try:
            # Check if person is referenced by any books
            check_query = """
            MATCH (p:Person {id: $person_id})<-[:AUTHORED]-(b:Book)
            RETURN COUNT(b) as book_count
            """
            
            # Use safe query execution and convert result
            check_raw_result = safe_execute_kuzu_query(
                query=check_query,
                params={"person_id": person_id},
                user_id=self.user_id,
                operation="check_person_book_references"
            )
            
            results = _convert_query_result_to_list(check_raw_result)
            book_count = 0
            
            if results and 'col_0' in results[0]:
                book_count = results[0]['col_0']
            
            if book_count > 0:
                return False
            
            # Safe to delete - no book references
            delete_query = """
            MATCH (p:Person {id: $person_id})
            DELETE p
            """
            
            # Use safe query execution
            safe_execute_kuzu_query(
                query=delete_query,
                params={"person_id": person_id},
                user_id=self.user_id,
                operation="delete_person"
            )
            return True
            
        except Exception as e:
            traceback.print_exc()
            return False
    
    async def find_person_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Find a person by name (case-insensitive)."""
        try:
            query = """
            MATCH (p:Person)
            WHERE toLower(p.name) = toLower($name)
            RETURN p
            LIMIT 1
            """
            
            # Use safe query execution and convert result
            raw_result = safe_execute_kuzu_query(
                query=query,
                params={"name": name},
                user_id=self.user_id,
                operation="find_person_by_name"
            )
            
            results = _convert_query_result_to_list(raw_result)
            
            if results and 'col_0' in results[0]:
                return results[0]['col_0']
            
            return None
            
        except Exception as e:
            return None
    
    async def get_books_by_person(self, person_id: str) -> List[Dict[str, Any]]:
        """Get all books associated with a person (as author, illustrator, etc.)."""
        try:
            query = """
            MATCH (p:Person {id: $person_id})-[r:AUTHORED]->(b:Book)
            RETURN b, COALESCE(r.role, 'authored') as relationship_type
            ORDER BY b.title ASC
            """
            
            # Use safe query execution and convert result
            raw_result = safe_execute_kuzu_query(
                query=query,
                params={"person_id": person_id},
                user_id=self.user_id,
                operation="get_books_by_person"
            )
            
            results = _convert_query_result_to_list(raw_result)
            
            books = []
            for result in results:
                if 'col_0' in result:
                    book_data = dict(result['col_0'])
                    # Add the relationship type
                    book_data['relationship_type'] = result.get('col_1', 'authored')
                    # Ensure uid is available as alias for id
                    if 'id' in book_data:
                        book_data['uid'] = book_data['id']
                    books.append(book_data)
            
            return books
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return []
    
    async def get_contribution_type_counts(self) -> Dict[str, int]:
        """Get counts of people by contribution type."""
        try:
            
            query = """
            MATCH (p:Person)-[r:AUTHORED]->(b:Book)
            WITH p, COALESCE(r.role, 'authored') as role
            RETURN role, COUNT(DISTINCT p) as person_count
            ORDER BY role
            """
            
            # Use safe query execution and convert result
            raw_result = safe_execute_kuzu_query(
                query=query,
                params={},
                user_id=self.user_id,
                operation="get_contribution_type_counts"
            )
            
            results = _convert_query_result_to_list(raw_result)
            
            counts = {}
            for result in results:
                role = result.get('col_0', 'authored')
                count = result.get('col_1', 0)
                counts[role] = count
            
            return counts
            
        except Exception as e:
            return {}

    # Sync wrappers for backward compatibility
    def list_all_persons_sync(self) -> List[Dict[str, Any]]:
        """Get all persons (sync version)."""
        return run_async(self.list_all_persons())
    
    def create_person_sync(self, person_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a new person (sync version)."""
        return run_async(self.create_person(person_data))
    
    def update_person_sync(self, person_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update a person's information (sync version)."""
        return run_async(self.update_person(person_id, updates))
    
    def delete_person_sync(self, person_id: str) -> bool:
        """Delete a person (sync version)."""
        return run_async(self.delete_person(person_id))
    
    def find_person_by_name_sync(self, name: str) -> Optional[Dict[str, Any]]:
        """Find a person by name (sync version)."""
        return run_async(self.find_person_by_name(name))
    
    def get_books_by_person_sync(self, person_id: str) -> List[Dict[str, Any]]:
        """Get all books associated with a person (sync version)."""
        return run_async(self.get_books_by_person(person_id))
    
    def get_contribution_type_counts_sync(self) -> Dict[str, int]:
        """Get counts of people by contribution type (sync version)."""
        return run_async(self.get_contribution_type_counts())
