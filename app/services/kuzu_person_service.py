"""
Kuzu Person Service

Handles author/contributor/person management using Kuzu.
Focused responsibility: Person entity management and author relationships.
"""

import traceback
from typing import List, Optional, Dict, Any

from ..infrastructure.kuzu_repositories import KuzuPersonRepository
from ..infrastructure.kuzu_graph import get_graph_storage, get_kuzu_database
from .kuzu_async_helper import run_async


class KuzuPersonService:
    """Service for person/author management operations."""
    
    def __init__(self):
        self.graph_storage = get_graph_storage()
        self.person_repo = KuzuPersonRepository()
    
    async def list_all_persons(self) -> List[Dict[str, Any]]:
        """Get all persons."""
        try:
            # The method exists in KuzuBookRepository, not KuzuPersonRepository
            from ..infrastructure.kuzu_repositories import KuzuBookRepository
            book_repo = KuzuBookRepository()
            return await book_repo.get_all_persons()
        except Exception as e:
            print(f"❌ [LIST_PERSONS] Error getting all persons: {e}")
            return []
    
    async def get_person_by_id(self, person_id: str) -> Optional[Dict[str, Any]]:
        """Get a person by ID."""
        try:
            return await self.person_repo.get_by_id(person_id)
        except Exception as e:
            print(f"❌ [GET_PERSON] Error getting person by ID {person_id}: {e}")
            return None
    
    def get_person_by_id_sync(self, person_id: str) -> Optional[Dict[str, Any]]:
        """Get a person by ID (sync version with detailed debugging)."""
        try:
            db = get_kuzu_database()
            
            print(f"🔍 [GET_PERSON_SYNC] Looking for person_id: '{person_id}'")
            
            # First, let's see what persons exist in the database
            debug_query = "MATCH (p:Person) RETURN p.id, p.name LIMIT 10"
            debug_results = db.query(debug_query)
            print(f"🔍 [GET_PERSON_SYNC] All persons in DB: {debug_results}")
            
            query = """
            MATCH (p:Person {id: $person_id})
            RETURN p
            LIMIT 1
            """
            
            print(f"🔍 [GET_PERSON_SYNC] Executing query: {query}")
            print(f"🔍 [GET_PERSON_SYNC] Query parameters: {{'person_id': person_id}}")
            
            results = db.query(query, {"person_id": person_id})
            
            print(f"🔍 [GET_PERSON_SYNC] Raw query results: {results}")
            print(f"🔍 [GET_PERSON_SYNC] Results type: {type(results)}")
            print(f"🔍 [GET_PERSON_SYNC] Results length: {len(results) if results else 'None'}")
            
            if results and len(results) > 0:
                print(f"🔍 [GET_PERSON_SYNC] First result: {results[0]}")
                print(f"🔍 [GET_PERSON_SYNC] First result keys: {list(results[0].keys()) if isinstance(results[0], dict) else 'Not a dict'}")
                
                # Try different possible key formats
                if 'result' in results[0]:
                    person_data = dict(results[0]['result'])
                    print(f"🔍 [GET_PERSON_SYNC] Person data from 'result': {person_data}")
                    return person_data
                elif 'col_0' in results[0]:
                    person_data = dict(results[0]['col_0'])
                    print(f"🔍 [GET_PERSON_SYNC] Person data from 'col_0': {person_data}")
                    return person_data
                elif 'p' in results[0]:
                    person_data = dict(results[0]['p'])
                    print(f"🔍 [GET_PERSON_SYNC] Person data from 'p': {person_data}")
                    return person_data
                else:
                    print(f"🔍 [GET_PERSON_SYNC] No expected key found in results[0]. Available keys: {list(results[0].keys())}")
            else:
                print(f"🔍 [GET_PERSON_SYNC] No results found")
            
            return None
            
        except Exception as e:
            print(f"❌ [GET_PERSON_SYNC] Error getting person by ID {person_id}: {e}")
            print(f"❌ [GET_PERSON_SYNC] Traceback: {traceback.format_exc()}")
            return None
    
    async def create_person(self, person_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a new person."""
        try:
            print(f"👤 [CREATE_PERSON] Creating person: {person_data.get('name', 'Unknown')}")
            
            # Use the person repository to create (it's a sync method)
            created_person = self.person_repo.create(person_data)
            
            if created_person:
                print(f"✅ [CREATE_PERSON] Successfully created person: {created_person.get('id')}")
            
            return created_person
            
        except Exception as e:
            print(f"❌ [CREATE_PERSON] Error creating person: {e}")
            traceback.print_exc()
            return None
    
    async def update_person(self, person_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update a person's information."""
        try:
            print(f"👤 [UPDATE_PERSON] Updating person {person_id} with: {updates}")
            
            # Update the person node in Kuzu
            success = self.graph_storage.update_node('Person', person_id, updates)
            
            if success:
                print(f"✅ [UPDATE_PERSON] Successfully updated person {person_id}")
                # Return updated person data
                return await self.get_person_by_id(person_id)
            else:
                print(f"❌ [UPDATE_PERSON] Failed to update person {person_id}")
                return None
                
        except Exception as e:
            print(f"❌ [UPDATE_PERSON] Error updating person {person_id}: {e}")
            traceback.print_exc()
            return None
    
    async def delete_person(self, person_id: str) -> bool:
        """Delete a person (if not referenced by any books)."""
        try:
            print(f"🗑️ [DELETE_PERSON] Deleting person {person_id}")
            
            # Check if person is referenced by any books
            check_query = """
            MATCH (p:Person {id: $person_id})<-[:AUTHORED]-(b:Book)
            RETURN COUNT(b) as book_count
            """
            
            results = self.graph_storage.query(check_query, {"person_id": person_id})
            book_count = 0
            
            if results and 'col_0' in results[0]:
                book_count = results[0]['col_0']
            
            if book_count > 0:
                print(f"⚠️ [DELETE_PERSON] Cannot delete person {person_id}: referenced by {book_count} books")
                return False
            
            # Safe to delete - no book references
            delete_query = """
            MATCH (p:Person {id: $person_id})
            DELETE p
            """
            
            self.graph_storage.query(delete_query, {"person_id": person_id})
            print(f"✅ [DELETE_PERSON] Successfully deleted person {person_id}")
            return True
            
        except Exception as e:
            print(f"❌ [DELETE_PERSON] Error deleting person {person_id}: {e}")
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
            
            results = self.graph_storage.query(query, {"name": name})
            
            if results and 'col_0' in results[0]:
                return results[0]['col_0']
            
            return None
            
        except Exception as e:
            print(f"❌ [FIND_PERSON_BY_NAME] Error finding person by name '{name}': {e}")
            return None
    
    async def get_books_by_person(self, person_id: str) -> List[Dict[str, Any]]:
        """Get all books associated with a person (as author, illustrator, etc.)."""
        try:
            query = """
            MATCH (p:Person {id: $person_id})<-[r:AUTHORED]-(b:Book)
            RETURN b, type(r) as relationship_type
            ORDER BY b.title ASC
            """
            
            results = self.graph_storage.query(query, {"person_id": person_id})
            
            books = []
            for result in results:
                if 'col_0' in result:
                    book_data = dict(result['col_0'])
                    # Add the relationship type
                    book_data['relationship_type'] = result.get('col_1', 'AUTHORED')
                    # Ensure uid is available as alias for id
                    if 'id' in book_data:
                        book_data['uid'] = book_data['id']
                    books.append(book_data)
            
            return books
            
        except Exception as e:
            print(f"❌ [GET_BOOKS_BY_PERSON] Error getting books by person {person_id}: {e}")
            return []
    
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
