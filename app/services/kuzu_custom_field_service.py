"""
Kuzu Custom Field Service

Handles custom metadata fields for books using KuzuDB.
"""

import traceback
import json
from typing import List, Dict, Any, Optional
from datetime import datetime

from ..infrastructure.kuzu_graph import KuzuGraphStorage
from .kuzu_async_helper import run_async


class KuzuCustomFieldService:
    """Service for managing custom metadata fields in KuzuDB."""
    
    def __init__(self):
        from ..infrastructure.kuzu_graph import get_kuzu_connection
        connection = get_kuzu_connection()
        self.graph_storage = KuzuGraphStorage(connection)
        self._ensure_custom_field_tables()
    
    def _ensure_custom_field_tables(self):
        """Ensure custom field tables exist in KuzuDB."""
        try:
            # CustomField table for field definitions
            create_table_query = """
            CREATE NODE TABLE CustomField (
                id STRING,
                name STRING,
                display_name STRING,
                field_type STRING,
                description STRING,
                created_by_user_id STRING,
                is_global BOOLEAN,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                PRIMARY KEY (id)
            )
            """
            try:
                self.graph_storage.query(create_table_query)
                print("📝 [CUSTOM_FIELDS] Created CustomField table")
            except Exception as e:
                if "already exists" in str(e):
                    print("📝 [CUSTOM_FIELDS] CustomField table already exists")
                else:
                    raise e
                
            # Create HAS_PERSONAL_METADATA relationship for user-specific data
            create_personal_rel_query = """
            CREATE REL TABLE HAS_PERSONAL_METADATA (
                FROM User TO Book,
                personal_notes STRING,
                personal_custom_fields STRING,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """
            try:
                self.graph_storage.query(create_personal_rel_query)
                print("📝 [CUSTOM_FIELDS] Created HAS_PERSONAL_METADATA relationship")
            except Exception as e:
                if "already exists" in str(e):
                    print("📝 [CUSTOM_FIELDS] HAS_PERSONAL_METADATA relationship already exists")
                else:
                    raise e
                
            # Create GlobalMetadata node table for storing global custom field data
            create_global_meta_query = """
            CREATE NODE TABLE GlobalMetadata (
                book_id STRING,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                PRIMARY KEY (book_id)
            )
            """
            try:
                self.graph_storage.query(create_global_meta_query)
                print("📝 [CUSTOM_FIELDS] Created GlobalMetadata table")
            except Exception as e:
                if "already exists" in str(e):
                    print("📝 [CUSTOM_FIELDS] GlobalMetadata table already exists")
                else:
                    raise e
                
            # Create HAS_GLOBAL_METADATA relationship for global custom fields
            create_global_rel_query = """
            CREATE REL TABLE HAS_GLOBAL_METADATA (
                FROM Book TO GlobalMetadata,
                global_custom_fields STRING,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            """
            try:
                self.graph_storage.query(create_global_rel_query)
                print("📝 [CUSTOM_FIELDS] Created HAS_GLOBAL_METADATA relationship")
            except Exception as e:
                if "already exists" in str(e):
                    print("📝 [CUSTOM_FIELDS] HAS_GLOBAL_METADATA relationship already exists")
                else:
                    raise e
                
        except Exception as e:
            print(f"❌ [CUSTOM_FIELDS] Error ensuring tables: {e}")
            # Tables might already exist, continue
    
    def get_custom_metadata_for_display(self, custom_metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Convert custom metadata to display format."""
        print(f"🔍 [CUSTOM_FIELDS] DEBUG: get_custom_metadata_for_display called with: {custom_metadata}")
        
        if not custom_metadata:
            print(f"🔍 [CUSTOM_FIELDS] DEBUG: No custom metadata provided, returning empty list")
            return []
        
        display_items = []
        for key, value in custom_metadata.items():
            print(f"🔍 [CUSTOM_FIELDS] DEBUG: Processing field '{key}' with value '{value}' (type: {type(value)})")
            
            if value is not None and value != '':
                # Try to get field definition for better display
                field_def = self._get_field_definition(key)
                print(f"🔍 [CUSTOM_FIELDS] DEBUG: Field definition for '{key}': {field_def}")
                
                display_name = field_def.get('display_name', key.replace('_', ' ').title()) if field_def else key.replace('_', ' ').title()
                field_type = field_def.get('field_type', 'text') if field_def else 'text'
                
                display_item = {
                    'field_name': key,
                    'display_name': display_name,
                    'value': str(value),
                    'display_value': str(value),
                    'field_type': field_type
                }
                
                print(f"🔍 [CUSTOM_FIELDS] DEBUG: Created display item: {display_item}")
                display_items.append(display_item)
            else:
                print(f"🔍 [CUSTOM_FIELDS] DEBUG: Skipping field '{key}' - empty or None value")
        
        print(f"🔍 [CUSTOM_FIELDS] DEBUG: Final display items: {display_items}")
        return display_items
    
    def _get_field_definition(self, field_name: str) -> Optional[Dict[str, Any]]:
        """Get field definition by name."""
        print(f"🔍 [CUSTOM_FIELDS] DEBUG: _get_field_definition called for field: {field_name}")
        
        try:
            query = "MATCH (f:CustomField) WHERE f.name = $name RETURN f.id, f.name, f.display_name, f.field_type, f.description, f.is_global, f.created_at"
            results = self.graph_storage.query(query, {"name": field_name})
            
            print(f"🔍 [CUSTOM_FIELDS] DEBUG: Field definition query results: {results}")
            
            if results and len(results) > 0:
                result = results[0]
                # KuzuDB returns column-based results: col_0=id, col_1=name, col_2=display_name, etc.
                field_definition = {
                    'id': result.get('col_0'),
                    'name': result.get('col_1'), 
                    'display_name': result.get('col_2'),
                    'field_type': result.get('col_3'),
                    'description': result.get('col_4'),
                    'is_global': result.get('col_5'),
                    'created_at': result.get('col_6')
                }
                print(f"🔍 [CUSTOM_FIELDS] DEBUG: Parsed field definition: {field_definition}")
                return field_definition
            
            print(f"🔍 [CUSTOM_FIELDS] DEBUG: No field definition found for '{field_name}'")
            return None
            
        except Exception as e:
            print(f"❌ [CUSTOM_FIELDS] Error getting field definition: {e}")
            return None
    
    def get_user_fields_sync(self, user_id: str) -> List[Dict[str, Any]]:
        """Get custom fields for a user."""
        print(f"📝 [CUSTOM_FIELDS] Getting custom fields for user {user_id}")
        
        try:
            # Get user-specific fields and global fields
            query = """
            MATCH (f:CustomField) 
            WHERE f.created_by_user_id = $user_id OR f.is_global = true
            RETURN f.id, f.name, f.display_name, f.field_type, f.description, f.is_global, f.created_at, f.updated_at
            ORDER BY f.created_at DESC
            """
            
            results = self.graph_storage.query(query, {"user_id": user_id})
            
            fields = []
            for result in results:
                # KuzuDB returns column-based results (col_0, col_1, etc.) instead of named columns
                # Based on the query: f.id, f.name, f.display_name, f.field_type, f.description, f.is_global, f.created_at, f.updated_at
                # Mapping: col_0=id, col_1=name, col_2=display_name, col_3=field_type, col_4=description, col_5=is_global, col_6=created_at, col_7=updated_at
                fields.append({
                    'id': result.get('col_0'),
                    'name': result.get('col_1'),
                    'display_name': result.get('col_2'),
                    'field_type': result.get('col_3', 'text'),
                    'description': result.get('col_4', ''),
                    'is_global': result.get('col_5', False),
                    'created_at': result.get('col_6'),
                    'updated_at': result.get('col_7')
                })
            
            print(f"📝 [CUSTOM_FIELDS] Found {len(fields)} fields for user {user_id}")
            return fields
            
        except Exception as e:
            print(f"❌ [CUSTOM_FIELDS] Error getting user fields: {e}")
            return []
    
    def create_field_sync(self, user_id: str, field_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Create a custom field definition. 
        Global fields affect Book properties, personal fields are stored in relationships."""
        field_name = field_data.get('name', 'unknown_field')
        is_global = field_data.get('is_global', False)  # Default to personal fields
        
        print(f"📝 [CUSTOM_FIELDS] Creating {'global' if is_global else 'personal'} field '{field_name}' for user {user_id}")
        
        try:
            # Check if field already exists with same scope
            if is_global:
                # For global fields, check if any global field with this name exists
                check_query = """
                MATCH (f:CustomField) 
                WHERE f.name = $name AND f.is_global = true 
                RETURN f.id, f.name, f.display_name, f.field_type, f.description, f.is_global, f.created_at
                """
                check_params = {"name": field_name}
            else:
                # For personal fields, check if this user already has a field with this name
                check_query = """
                MATCH (f:CustomField) 
                WHERE f.name = $name AND f.created_by_user_id = $user_id AND f.is_global = false 
                RETURN f.id, f.name, f.display_name, f.field_type, f.description, f.is_global, f.created_at
                """
                check_params = {"name": field_name, "user_id": user_id}
            
            existing_results = self.graph_storage.query(check_query, check_params)
            
            if existing_results and len(existing_results) > 0:
                # Field already exists, return it
                result = existing_results[0]
                created_at = result.get('col_6')
                existing_field = {
                    'id': result.get('col_0'),
                    'name': result.get('col_1'), 
                    'display_name': result.get('col_2'),
                    'field_type': result.get('col_3'),
                    'description': result.get('col_4'),
                    'is_global': result.get('col_5'),
                    'created_at': created_at.isoformat() if created_at and hasattr(created_at, 'isoformat') else None
                }
                print(f"✅ [CUSTOM_FIELDS] Field '{field_name}' already exists with ID {existing_field['id']}")
                return existing_field
            
            # Field doesn't exist, create new one
            field_id = f"field_{datetime.utcnow().timestamp()}"
            current_time = datetime.utcnow()
            
            # Validate field type
            valid_types = ['text', 'textarea', 'number', 'date', 'boolean', 'rating_5', 'rating_10', 'tags']
            field_type = field_data.get('field_type', 'text')
            if field_type not in valid_types:
                field_type = 'text'
            
            # Insert field definition
            query = """
            CREATE (f:CustomField {
                id: $id,
                name: $name,
                display_name: $display_name,
                field_type: $field_type,
                description: $description,
                created_by_user_id: $user_id,
                is_global: $is_global,
                created_at: $created_at,
                updated_at: $updated_at
            })
            RETURN f
            """
            
            params = {
                'id': field_id,
                'name': field_name,
                'display_name': field_data.get('display_name', field_name.replace('_', ' ').title()),
                'field_type': field_type,
                'description': field_data.get('description', ''),
                'user_id': user_id,
                'is_global': is_global,
                'created_at': current_time,
                'updated_at': current_time
            }
            
            result = self.graph_storage.query(query, params)
            
            if result and len(result) > 0:
                created_field = result[0].get('f', {})
                print(f"✅ [CUSTOM_FIELDS] Created {'global' if is_global else 'personal'} field '{field_name}' with ID {field_id}")
                
                # For global fields, we might want to add the property to the Book schema
                if is_global:
                    print(f"📝 [CUSTOM_FIELDS] Note: Global field '{field_name}' will be stored as Book.{field_name} property")
                
                created_at = created_field.get('created_at')
                return {
                    'id': created_field.get('id'),
                    'name': created_field.get('name'),
                    'display_name': created_field.get('display_name'),
                    'field_type': created_field.get('field_type'),
                    'description': created_field.get('description'),
                    'is_global': created_field.get('is_global'),
                    'created_at': created_at.isoformat() if created_at and hasattr(created_at, 'isoformat') else None
                }
            else:
                print(f"❌ [CUSTOM_FIELDS] Failed to create field '{field_name}'")
                return None
                
        except Exception as e:
            print(f"❌ [CUSTOM_FIELDS] Error creating field: {e}")
            traceback.print_exc()
            return None
    
    def save_custom_metadata_sync(self, book_id: str, user_id: str, custom_metadata: Dict[str, Any]) -> bool:
        """Save custom metadata according to the new architecture:
        - Global fields: stored directly on Book node as properties
        - Personal fields: stored in HAS_PERSONAL_METADATA relationship
        """
        print(f"📝 [CUSTOM_FIELDS] Saving custom metadata for book {book_id}, user {user_id}")
        
        try:
            global_metadata = {}
            personal_metadata = {}
            
            # Separate global vs personal fields based on field definitions
            for field_name, field_value in custom_metadata.items():
                if field_value is not None and field_value != '':
                    field_def = self._get_field_definition(field_name)
                    print(f"🔍 [CUSTOM_FIELDS] DEBUG: Field '{field_name}' definition: {field_def}")
                    
                    if field_def and field_def.get('is_global', False):
                        global_metadata[field_name] = field_value
                        print(f"📝 [CUSTOM_FIELDS] Classified '{field_name}' as GLOBAL field")
                    else:
                        personal_metadata[field_name] = field_value
                        print(f"📝 [CUSTOM_FIELDS] Classified '{field_name}' as PERSONAL field")
            
            print(f"📝 [CUSTOM_FIELDS] SUMMARY: {len(global_metadata)} global fields, {len(personal_metadata)} personal fields")
            
            # Save global metadata to a global metadata relationship instead of Book properties
            if global_metadata:
                print(f"📝 [CUSTOM_FIELDS] Saving {len(global_metadata)} global fields to HAS_GLOBAL_METADATA relationship")
                
                # Check if HAS_GLOBAL_METADATA relationship exists for this book
                check_global_query = """
                MATCH (b:Book {id: $book_id})-[r:HAS_GLOBAL_METADATA]->(gm:GlobalMetadata)
                RETURN r.global_custom_fields
                """
                
                existing_global_results = self.graph_storage.query(check_global_query, {
                    "book_id": book_id
                })
                
                # Get existing global metadata and merge
                existing_global_metadata = {}
                if existing_global_results and len(existing_global_results) > 0:
                    result = existing_global_results[0]
                    # Try multiple ways to access the data
                    metadata_json = (result.get('col_0') or 
                                   result.get('r.global_custom_fields') or 
                                   result.get('global_custom_fields') or
                                   result.get('result'))
                    
                    if metadata_json:
                        try:
                            if isinstance(metadata_json, str):
                                existing_global_metadata = json.loads(metadata_json)
                            elif isinstance(metadata_json, dict):
                                existing_global_metadata = metadata_json
                        except (json.JSONDecodeError, TypeError) as e:
                            print(f"❌ [CUSTOM_FIELDS] Error parsing existing global metadata JSON: {e}")
                            existing_global_metadata = {}
                
                # Merge new global metadata with existing
                merged_global_metadata = existing_global_metadata.copy()
                merged_global_metadata.update(global_metadata)
                
                current_time = datetime.utcnow()
                
                if existing_global_results and len(existing_global_results) > 0:
                    # Update existing global metadata
                    update_global_query = """
                    MATCH (b:Book {id: $book_id})-[r:HAS_GLOBAL_METADATA]->(gm:GlobalMetadata)
                    SET r.global_custom_fields = $global_custom_fields, r.updated_at = $updated_at
                    """
                    
                    self.graph_storage.query(update_global_query, {
                        "book_id": book_id,
                        "global_custom_fields": json.dumps(merged_global_metadata),
                        "updated_at": current_time
                    })
                    print(f"✅ [CUSTOM_FIELDS] Updated existing global metadata")
                else:
                    # Create new global metadata relationship
                    # First ensure GlobalMetadata node exists
                    create_global_query = """
                    MERGE (gm:GlobalMetadata {book_id: $book_id})
                    WITH gm
                    MATCH (b:Book {id: $book_id})
                    MERGE (b)-[r:HAS_GLOBAL_METADATA]->(gm)
                    SET r.global_custom_fields = $global_custom_fields,
                        r.created_at = $created_at,
                        r.updated_at = $updated_at
                    """
                    
                    self.graph_storage.query(create_global_query, {
                        "book_id": book_id,
                        "global_custom_fields": json.dumps(merged_global_metadata),
                        "created_at": current_time,
                        "updated_at": current_time
                    })
                    print(f"✅ [CUSTOM_FIELDS] Created new global metadata relationship")
                
                print(f"✅ [CUSTOM_FIELDS] Saved {len(global_metadata)} global fields to HAS_GLOBAL_METADATA")
            
            # Save personal metadata to HAS_PERSONAL_METADATA relationship
            if personal_metadata:
                print(f"📝 [CUSTOM_FIELDS] Saving {len(personal_metadata)} personal fields to HAS_PERSONAL_METADATA")
                print(f"📝 [CUSTOM_FIELDS] DEBUG: personal_metadata = {personal_metadata}")
                
                # First, verify User and Book nodes exist
                verify_nodes_query = """
                MATCH (u:User {id: $user_id}), (b:Book {id: $book_id})
                RETURN u.id AS user_exists, b.id AS book_exists
                """
                
                verify_results = self.graph_storage.query(verify_nodes_query, {
                    "user_id": user_id,
                    "book_id": book_id
                })
                
                print(f"🔍 [CUSTOM_FIELDS] DEBUG: Node verification results = {verify_results}")
                if not verify_results or len(verify_results) == 0:
                    print(f"❌ [CUSTOM_FIELDS] ERROR: User {user_id} or Book {book_id} nodes don't exist!")
                    return False
                
                # Check if HAS_PERSONAL_METADATA relationship exists
                check_query = """
                MATCH (u:User {id: $user_id})-[r:HAS_PERSONAL_METADATA]->(b:Book {id: $book_id})
                RETURN r.personal_custom_fields
                """
                
                existing_results = self.graph_storage.query(check_query, {
                    "user_id": user_id,
                    "book_id": book_id
                })
                
                print(f"🔍 [CUSTOM_FIELDS] DEBUG: check existing_results = {existing_results}")
                print(f"🔍 [CUSTOM_FIELDS] DEBUG: check existing_results type = {type(existing_results)}")
                if existing_results:
                    print(f"🔍 [CUSTOM_FIELDS] DEBUG: check len(existing_results) = {len(existing_results)}")
                    if len(existing_results) > 0:
                        print(f"🔍 [CUSTOM_FIELDS] DEBUG: check existing_results[0] = {existing_results[0]}")
                        print(f"🔍 [CUSTOM_FIELDS] DEBUG: check existing_results[0] keys = {list(existing_results[0].keys()) if isinstance(existing_results[0], dict) else 'Not a dict'}")
                
                # Get existing personal metadata and merge
                existing_personal_metadata = {}
                if existing_results and len(existing_results) > 0:
                    result = existing_results[0]
                    # Try multiple ways to access the data
                    metadata_json = (result.get('col_0') or 
                                   result.get('r.personal_custom_fields') or 
                                   result.get('personal_custom_fields') or
                                   result.get('result'))
                    
                    if metadata_json:
                        try:
                            if isinstance(metadata_json, str):
                                existing_personal_metadata = json.loads(metadata_json)
                            elif isinstance(metadata_json, dict):
                                existing_personal_metadata = metadata_json
                        except (json.JSONDecodeError, TypeError) as e:
                            print(f"❌ [CUSTOM_FIELDS] Error parsing existing personal metadata JSON: {e}")
                            existing_personal_metadata = {}
                
                # Merge new personal metadata with existing
                merged_personal_metadata = existing_personal_metadata.copy()
                merged_personal_metadata.update(personal_metadata)
                
                current_time = datetime.utcnow()
                
                if existing_results and len(existing_results) > 0:
                    # Update existing relationship
                    print(f"🔄 [CUSTOM_FIELDS] DEBUG: Updating existing HAS_PERSONAL_METADATA relationship")
                    update_personal_query = """
                    MATCH (u:User {id: $user_id})-[r:HAS_PERSONAL_METADATA]->(b:Book {id: $book_id})
                    SET r.personal_custom_fields = $personal_custom_fields, r.updated_at = $updated_at
                    """
                    
                    try:
                        update_result = self.graph_storage.query(update_personal_query, {
                            "user_id": user_id,
                            "book_id": book_id,
                            "personal_custom_fields": json.dumps(merged_personal_metadata),
                            "updated_at": current_time
                        })
                        print(f"✅ [CUSTOM_FIELDS] DEBUG: Update result: {update_result}")
                    except Exception as e:
                        print(f"❌ [CUSTOM_FIELDS] DEBUG: Error updating relationship: {e}")
                        raise
                else:
                    # Create new relationship
                    print(f"🆕 [CUSTOM_FIELDS] DEBUG: Creating new HAS_PERSONAL_METADATA relationship")
                    print(f"🆕 [CUSTOM_FIELDS] DEBUG: user_id={user_id}, book_id={book_id}")
                    print(f"🆕 [CUSTOM_FIELDS] DEBUG: merged_personal_metadata={merged_personal_metadata}")
                    
                    create_personal_query = """
                    MATCH (u:User {id: $user_id}), (b:Book {id: $book_id})
                    CREATE (u)-[r:HAS_PERSONAL_METADATA {
                        personal_custom_fields: $personal_custom_fields,
                        created_at: $created_at,
                        updated_at: $updated_at
                    }]->(b)
                    """
                    
                    try:
                        create_result = self.graph_storage.query(create_personal_query, {
                            "user_id": user_id,
                            "book_id": book_id,
                            "personal_custom_fields": json.dumps(merged_personal_metadata),
                            "created_at": current_time,
                            "updated_at": current_time
                        })
                        print(f"✅ [CUSTOM_FIELDS] DEBUG: Create result: {create_result}")
                    except Exception as e:
                        print(f"❌ [CUSTOM_FIELDS] DEBUG: Error creating relationship: {e}")
                        raise
                
                print(f"✅ [CUSTOM_FIELDS] Saved {len(personal_metadata)} personal fields to HAS_PERSONAL_METADATA")
            
            total_saved = len(global_metadata) + len(personal_metadata)
            print(f"✅ [CUSTOM_FIELDS] Successfully saved {total_saved} custom metadata items")
            return True
            
        except Exception as e:
            print(f"❌ [CUSTOM_FIELDS] Error saving custom metadata: {e}")
            traceback.print_exc()
            return False
    
    def get_custom_metadata_sync(self, book_id: str, user_id: str) -> Dict[str, Any]:
        """Get custom metadata for a book from the new architecture:
        - Global fields: from Book node properties
        - Personal fields: from HAS_PERSONAL_METADATA relationship
        """
        print(f"📝 [CUSTOM_FIELDS] Getting custom metadata for book {book_id}, user {user_id}")
        
        try:
            custom_metadata = {}
            
            # Get global custom fields from HAS_GLOBAL_METADATA relationship
            global_query = """
            MATCH (b:Book {id: $book_id})-[r:HAS_GLOBAL_METADATA]->(gm:GlobalMetadata)
            RETURN r.global_custom_fields
            """
            
            global_results = self.graph_storage.query(global_query, {"book_id": book_id})
            
            if global_results and len(global_results) > 0:
                result = global_results[0]
                # KuzuDB returns results with column-based keys, but we need to handle both cases
                metadata_json = (result.get('col_0') or 
                               result.get('r.global_custom_fields') or 
                               result.get('global_custom_fields') or
                               result.get('result'))
                
                print(f"📝 [CUSTOM_FIELDS] Found global metadata: {metadata_json}")
                print(f"🔍 [CUSTOM_FIELDS] DEBUG: global metadata_json type = {type(metadata_json)}")
                print(f"🔍 [CUSTOM_FIELDS] DEBUG: global metadata_json value = {repr(metadata_json)}")
                if metadata_json:
                    try:
                        global_metadata = {}
                        if isinstance(metadata_json, str):
                            global_metadata = json.loads(metadata_json)
                        elif isinstance(metadata_json, dict):
                            global_metadata = metadata_json
                        
                        # Merge global metadata into the result
                        custom_metadata.update(global_metadata)
                        print(f"📝 [CUSTOM_FIELDS] Loaded {len(global_metadata)} global custom fields")
                    except (json.JSONDecodeError, TypeError) as e:
                        print(f"❌ [CUSTOM_FIELDS] Error parsing global custom metadata JSON: {e}")
            else:
                print(f"📝 [CUSTOM_FIELDS] No global custom metadata found")
            
            # Get personal custom fields from HAS_PERSONAL_METADATA relationship
            personal_query = """
            MATCH (u:User {id: $user_id})-[r:HAS_PERSONAL_METADATA]->(b:Book {id: $book_id})
            RETURN r.personal_custom_fields
            """
            
            print(f"📝 [CUSTOM_FIELDS] Checking HAS_PERSONAL_METADATA relationship for personal fields...")
            
            # First, let's check if the relationship exists at all
            exists_query = """
            MATCH (u:User {id: $user_id})-[r:HAS_PERSONAL_METADATA]->(b:Book {id: $book_id})
            RETURN COUNT(r) AS rel_count
            """
            
            exists_results = self.graph_storage.query(exists_query, {
                "book_id": book_id,
                "user_id": user_id
            })
            
            print(f"🔍 [CUSTOM_FIELDS] DEBUG: exists_results = {exists_results}")
            if exists_results and len(exists_results) > 0:
                rel_count = exists_results[0].get('col_0') or exists_results[0].get('rel_count') or exists_results[0].get('result') or 0
                print(f"🔍 [CUSTOM_FIELDS] DEBUG: Found {rel_count} HAS_PERSONAL_METADATA relationships")
            
            personal_results = self.graph_storage.query(personal_query, {
                "book_id": book_id,
                "user_id": user_id
            })
            
            print(f"🔍 [CUSTOM_FIELDS] DEBUG: personal_results = {personal_results}")
            print(f"🔍 [CUSTOM_FIELDS] DEBUG: personal_results type = {type(personal_results)}")
            if personal_results:
                print(f"🔍 [CUSTOM_FIELDS] DEBUG: len(personal_results) = {len(personal_results)}")
                if len(personal_results) > 0:
                    print(f"🔍 [CUSTOM_FIELDS] DEBUG: personal_results[0] = {personal_results[0]}")
                    print(f"🔍 [CUSTOM_FIELDS] DEBUG: personal_results[0] keys = {list(personal_results[0].keys()) if isinstance(personal_results[0], dict) else 'Not a dict'}")
            
            if personal_results and len(personal_results) > 0:
                # KuzuDB returns column-based results - personal_custom_fields is col_0
                result = personal_results[0]
                # Try multiple ways to access the data
                metadata_json = (result.get('col_0') or 
                               result.get('r.personal_custom_fields') or 
                               result.get('personal_custom_fields') or
                               result.get('result'))
                
                print(f"📝 [CUSTOM_FIELDS] Found personal metadata: {metadata_json}")
                print(f"🔍 [CUSTOM_FIELDS] DEBUG: metadata_json type = {type(metadata_json)}")
                print(f"🔍 [CUSTOM_FIELDS] DEBUG: metadata_json value = {repr(metadata_json)}")
                if metadata_json:
                    try:
                        personal_metadata = {}
                        if isinstance(metadata_json, str):
                            personal_metadata = json.loads(metadata_json)
                        elif isinstance(metadata_json, dict):
                            personal_metadata = metadata_json
                        
                        # Merge personal metadata into the result
                        custom_metadata.update(personal_metadata)
                        print(f"📝 [CUSTOM_FIELDS] Loaded {len(personal_metadata)} personal custom fields")
                    except (json.JSONDecodeError, TypeError) as e:
                        print(f"❌ [CUSTOM_FIELDS] Error parsing personal custom metadata JSON: {e}")
            else:
                print(f"📝 [CUSTOM_FIELDS] No personal custom metadata found")
            
            # Fallback: Check old OWNS relationship for migration compatibility
            if not custom_metadata:
                print(f"📝 [CUSTOM_FIELDS] No metadata found in new structure, checking OWNS relationship for backward compatibility...")
                owns_query = """
                MATCH (u:User {id: $user_id})-[r:OWNS]->(b:Book {id: $book_id})
                RETURN r.custom_metadata AS custom_metadata
                """
                
                owns_results = self.graph_storage.query(owns_query, {
                    "book_id": book_id,
                    "user_id": user_id
                })
                
                if owns_results and owns_results[0].get('custom_metadata'):
                    metadata_json = owns_results[0].get('custom_metadata')
                    if metadata_json:
                        try:
                            if isinstance(metadata_json, str):
                                custom_metadata = json.loads(metadata_json)
                            elif isinstance(metadata_json, dict):
                                custom_metadata = metadata_json
                            print(f"📝 [CUSTOM_FIELDS] Found {len(custom_metadata)} metadata items in OWNS relationship (backward compatibility)")
                        except (json.JSONDecodeError, TypeError) as e:
                            print(f"❌ [CUSTOM_FIELDS] Error parsing OWNS custom_metadata JSON: {e}")
            
            print(f"📝 [CUSTOM_FIELDS] Found {len(custom_metadata)} total custom metadata items")
            return custom_metadata
            
        except Exception as e:
            print(f"❌ [CUSTOM_FIELDS] Error getting custom metadata: {e}")
            traceback.print_exc()
            return {}
    
    def get_user_fields_with_calculated_usage_sync(self, user_id: str) -> List[Dict[str, Any]]:
        """Get user fields with usage stats."""
        print(f"📝 [CUSTOM_FIELDS] Getting fields with usage for user {user_id}")
        
        try:
            # First, let's debug what's actually in the database
            debug_query = "MATCH (f:CustomField) RETURN f.id, f.name, f.created_by_user_id, f.is_global LIMIT 10"
            debug_results = self.graph_storage.query(debug_query)
            print(f"📝 [CUSTOM_FIELDS] DEBUG: Found {len(debug_results)} CustomField nodes in total")
            for i, result in enumerate(debug_results):
                print(f"📝 [CUSTOM_FIELDS] DEBUG: Field {i}: {result}")
            
            # Get fields without usage count for now (KuzuDB has issues with COUNT + OPTIONAL MATCH + GROUP BY)
            query = """
            MATCH (f:CustomField) 
            WHERE f.created_by_user_id = $user_id OR f.is_global = true
            RETURN f.id AS field_id, f.name AS field_name, f.display_name AS field_display_name, 
                   f.field_type AS field_type, f.description AS field_description, 
                   f.is_global AS field_is_global, f.created_at AS field_created_at
            ORDER BY f.created_at DESC
            """
            
            print(f"📝 [CUSTOM_FIELDS] Query: {query}")
            print(f"📝 [CUSTOM_FIELDS] Parameters: user_id={user_id}")
            
            results = self.graph_storage.query(query, {"user_id": user_id})
            
            print(f"📝 [CUSTOM_FIELDS] Raw query results: {len(results)} rows")
            for i, result in enumerate(results):
                print(f"📝 [CUSTOM_FIELDS] Row {i}: {result}")
            
            fields = []
            for result in results:
                # KuzuDB returns column-based results (col_0, col_1, etc.) instead of named columns
                # Based on the query: f.id, f.name, f.display_name, f.field_type, f.description, f.is_global, f.created_at
                # Mapping: col_0=id, col_1=name, col_2=display_name, col_3=field_type, col_4=description, col_5=is_global, col_6=created_at
                field_data = {
                    'id': result.get('col_0'),
                    'name': result.get('col_1'),
                    'display_name': result.get('col_2'),
                    'field_type': result.get('col_3', 'text'),
                    'description': result.get('col_4', ''),
                    'is_global': result.get('col_5', False),
                    'created_at': result.get('col_6')
                }
                # Set usage_count to 0 for now since we're not calculating it
                usage_count = 0
                
                if field_data.get('id'):
                    print(f"📝 [CUSTOM_FIELDS] Processing field: {field_data}")
                    fields.append({
                        'id': field_data['id'],
                        'name': field_data['name'],
                        'display_name': field_data['display_name'],
                        'field_type': field_data['field_type'],
                        'description': field_data['description'],
                        'is_global': field_data['is_global'],
                        'usage_count': usage_count,
                        'created_at': field_data['created_at']
                    })
            
            print(f"📝 [CUSTOM_FIELDS] Found {len(fields)} fields with usage for user {user_id}")
            return fields
            
        except Exception as e:
            print(f"❌ [CUSTOM_FIELDS] Error getting fields with usage: {e}")
            return []
    
    def get_shareable_fields_with_calculated_usage_sync(self, exclude_user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get shareable fields with usage stats."""
        print(f"📝 [CUSTOM_FIELDS] Getting shareable fields (excluding user: {exclude_user_id})")
        
        try:
            # Get global fields without usage count for now (KuzuDB has issues with COUNT + OPTIONAL MATCH + GROUP BY)
            if exclude_user_id:
                query = """
                MATCH (f:CustomField) 
                WHERE f.is_global = true AND f.created_by_user_id <> $exclude_user_id
                RETURN f.id AS field_id, f.name AS field_name, f.display_name AS field_display_name, 
                       f.field_type AS field_type, f.description AS field_description, 
                       f.is_global AS field_is_global, f.created_at AS field_created_at
                ORDER BY f.created_at DESC
                """
                params = {"exclude_user_id": exclude_user_id}
            else:
                query = """
                MATCH (f:CustomField) 
                WHERE f.is_global = true
                RETURN f.id AS field_id, f.name AS field_name, f.display_name AS field_display_name, 
                       f.field_type AS field_type, f.description AS field_description, 
                       f.is_global AS field_is_global, f.created_at AS field_created_at
                ORDER BY f.created_at DESC
                """
                params = {}
            
            results = self.graph_storage.query(query, params)
            
            fields = []
            for result in results:
                # KuzuDB returns column-based results (col_0, col_1, etc.) instead of named columns
                # Based on the query: f.id, f.name, f.display_name, f.field_type, f.description, f.is_global, f.created_at
                # Mapping: col_0=id, col_1=name, col_2=display_name, col_3=field_type, col_4=description, col_5=is_global, col_6=created_at
                field_data = {
                    'id': result.get('col_0'),
                    'name': result.get('col_1'),
                    'display_name': result.get('col_2'),
                    'field_type': result.get('col_3', 'text'),
                    'description': result.get('col_4', ''),
                    'is_global': result.get('col_5', False),
                    'created_at': result.get('col_6')
                }
                # Set usage_count to 0 for now since we're not calculating it
                usage_count = 0
                
                if field_data.get('id'):
                    fields.append({
                        'id': field_data['id'],
                        'name': field_data['name'],
                        'display_name': field_data['display_name'],
                        'field_type': field_data['field_type'],
                        'description': field_data['description'],
                        'is_global': field_data['is_global'],
                        'usage_count': usage_count,
                        'created_at': field_data['created_at']
                    })
            
            print(f"📝 [CUSTOM_FIELDS] Found {len(fields)} shareable fields")
            return fields
            
        except Exception as e:
            print(f"❌ [CUSTOM_FIELDS] Error getting shareable fields: {e}")
            return []
    
    def get_field_by_id_sync(self, field_id: str) -> Optional[Dict[str, Any]]:
        """Get a custom field by its ID."""
        print(f"📝 [CUSTOM_FIELDS] Getting field by ID: {field_id}")
        
        try:
            query = """
            MATCH (f:CustomField {id: $field_id})
            RETURN f.id AS field_id, f.name AS field_name, f.display_name AS field_display_name, 
                   f.field_type AS field_type, f.description AS field_description, 
                   f.is_global AS field_is_global, f.created_at AS field_created_at,
                   f.updated_at AS field_updated_at, f.created_by_user_id AS created_by_user_id
            """
            
            results = self.graph_storage.query(query, {"field_id": field_id})
            
            if results and len(results) > 0:
                result = results[0]
                # Column mapping: col_0=id, col_1=name, col_2=display_name, col_3=field_type, 
                # col_4=description, col_5=is_global, col_6=created_at, col_7=updated_at, col_8=created_by_user_id
                field_data = {
                    'id': result.get('col_0'),
                    'name': result.get('col_1'),
                    'display_name': result.get('col_2'),
                    'field_type': result.get('col_3', 'text'),
                    'description': result.get('col_4', ''),
                    'is_global': result.get('col_5', False),
                    'created_at': result.get('col_6'),
                    'updated_at': result.get('col_7'),
                    'created_by_user_id': result.get('col_8'),
                    'usage_count': 0  # Add usage count for template compatibility
                }
                print(f"📝 [CUSTOM_FIELDS] Found field: {field_data}")
                return field_data
            else:
                print(f"📝 [CUSTOM_FIELDS] Field not found: {field_id}")
                return None
                
        except Exception as e:
            print(f"❌ [CUSTOM_FIELDS] Error getting field by ID: {e}")
            return None
    
    def update_field_sync(self, field_id: str, user_id: str, field_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update a custom field definition."""
        print(f"📝 [CUSTOM_FIELDS] Updating field {field_id} for user {user_id}")
        
        try:
            # First check if user owns this field
            field = self.get_field_by_id_sync(field_id)
            if not field:
                print(f"❌ [CUSTOM_FIELDS] Field not found: {field_id}")
                return None
                
            if field.get('created_by_user_id') != user_id:
                print(f"❌ [CUSTOM_FIELDS] User {user_id} does not own field {field_id}")
                return None
            
            current_time = datetime.utcnow()
            
            # Update field definition
            query = """
            MATCH (f:CustomField {id: $field_id})
            SET f.name = $name,
                f.display_name = $display_name,
                f.field_type = $field_type,
                f.description = $description,
                f.is_global = $is_global,
                f.updated_at = $updated_at
            RETURN f.id AS field_id, f.name AS field_name, f.display_name AS field_display_name, 
                   f.field_type AS field_type, f.description AS field_description, 
                   f.is_global AS field_is_global, f.created_at AS field_created_at,
                   f.updated_at AS field_updated_at, f.created_by_user_id AS created_by_user_id
            """
            
            params = {
                'field_id': field_id,
                'name': field_data.get('name', field.get('name', '')),
                'display_name': field_data.get('display_name', field.get('display_name', '')),
                'field_type': field_data.get('field_type', field.get('field_type', 'text')),
                'description': field_data.get('description', field.get('description', '')),
                'is_global': field_data.get('is_global', field.get('is_global', False)),
                'updated_at': current_time
            }
            
            result = self.graph_storage.query(query, params)
            
            if result and len(result) > 0:
                row = result[0]
                print(f"✅ [CUSTOM_FIELDS] Updated field {field_id}")
                # Column mapping: col_0=id, col_1=name, col_2=display_name, col_3=field_type, 
                # col_4=description, col_5=is_global, col_6=created_at, col_7=updated_at, col_8=created_by_user_id
                return {
                    'id': row.get('col_0'),
                    'name': row.get('col_1'),
                    'display_name': row.get('col_2'),
                    'field_type': row.get('col_3'),
                    'description': row.get('col_4'),
                    'is_global': row.get('col_5'),
                    'created_at': row.get('col_6'),
                    'updated_at': row.get('col_7'),
                    'created_by_user_id': row.get('col_8')
                }
            else:
                print(f"❌ [CUSTOM_FIELDS] Failed to update field {field_id}")
                return None
                
        except Exception as e:
            print(f"❌ [CUSTOM_FIELDS] Error updating field: {e}")
            return None
    
    def delete_field_sync(self, field_id: str, user_id: str) -> bool:
        """Delete a custom field and all its values."""
        print(f"📝 [CUSTOM_FIELDS] Deleting field {field_id} for user {user_id}")
        
        try:
            # First, check if user owns this field
            check_query = """
            MATCH (f:CustomField {id: $field_id})
            WHERE f.created_by_user_id = $user_id
            RETURN f
            """
            
            results = self.graph_storage.query(check_query, {
                "field_id": field_id,
                "user_id": user_id
            })
            
            if not results:
                print(f"❌ [CUSTOM_FIELDS] User {user_id} does not own field {field_id}")
                return False
            
            # Step 1: Delete all custom field values that use this field
            delete_values_query = """
            MATCH (cfv:CustomFieldValue)-[:HAS_FIELD]->(f:CustomField {id: $field_id})
            DELETE cfv
            """
            
            self.graph_storage.query(delete_values_query, {"field_id": field_id})
            print(f"📝 [CUSTOM_FIELDS] Deleted field values for field {field_id}")
            
            # Step 2: Delete the field itself
            delete_field_query = """
            MATCH (f:CustomField {id: $field_id})
            DELETE f
            """
            
            self.graph_storage.query(delete_field_query, {"field_id": field_id})
            
            print(f"✅ [CUSTOM_FIELDS] Deleted field {field_id}")
            return True
            
        except Exception as e:
            print(f"❌ [CUSTOM_FIELDS] Error deleting field: {e}")
            return False
        
    def get_available_fields_sync(self, user_id: str, is_global: Optional[bool] = None) -> List[Dict[str, Any]]:
        """Get available fields for a user, optionally filtered by global status."""
        print(f"📝 [CUSTOM_FIELDS] Getting available fields for user {user_id}, is_global={is_global}")
        
        try:
            if is_global is None:
                # Get all fields (user-specific and global)
                query = """
                MATCH (f:CustomField) 
                WHERE f.created_by_user_id = $user_id OR f.is_global = true
                RETURN f.id, f.name, f.display_name, f.field_type, f.description, f.is_global, f.created_at
                ORDER BY f.created_at DESC
                """
                params = {"user_id": user_id}
            elif is_global:
                # Get only global fields
                query = """
                MATCH (f:CustomField) 
                WHERE f.is_global = true
                RETURN f.id, f.name, f.display_name, f.field_type, f.description, f.is_global, f.created_at
                ORDER BY f.created_at DESC
                """
                params = {}
            else:
                # Get only user-specific fields
                query = """
                MATCH (f:CustomField) 
                WHERE f.created_by_user_id = $user_id AND f.is_global = false
                RETURN f.id, f.name, f.display_name, f.field_type, f.description, f.is_global, f.created_at
                ORDER BY f.created_at DESC
                """
                params = {"user_id": user_id}
            
            results = self.graph_storage.query(query, params)
            
            fields = []
            for result in results:
                # KuzuDB returns column-based results (col_0, col_1, etc.) instead of named columns
                # Based on the query: f.id, f.name, f.display_name, f.field_type, f.description, f.is_global, f.created_at
                # Mapping: col_0=id, col_1=name, col_2=display_name, col_3=field_type, col_4=description, col_5=is_global, col_6=created_at
                fields.append({
                    'id': result.get('col_0'),
                    'name': result.get('col_1'),
                    'display_name': result.get('col_2'),
                    'field_type': result.get('col_3', 'text'),
                    'description': result.get('col_4', ''),
                    'is_global': result.get('col_5', False),
                    'created_at': result.get('col_6')
                })
            
            print(f"📝 [CUSTOM_FIELDS] Found {len(fields)} available fields")
            return fields
            
        except Exception as e:
            print(f"❌ [CUSTOM_FIELDS] Error getting available fields: {e}")
            return []
    
    def ensure_custom_fields_exist(self, user_id: str, global_custom_metadata: Dict[str, Any], 
                                 personal_custom_metadata: Dict[str, Any]) -> bool:
        """
        Ensure that custom field definitions exist for all fields in the metadata.
        Creates field definitions if they don't already exist.
        """
        try:
            print(f"🔍 [CUSTOM_FIELDS] Ensuring custom field definitions exist...")
            
            # Process global custom fields
            for field_name, value in global_custom_metadata.items():
                if value is not None and value != '':
                    field_def = self._get_field_definition(field_name)
                    if not field_def:
                        print(f"📝 [CUSTOM_FIELDS] Creating global field definition: {field_name}")
                        field_def = self.create_field_sync(user_id, {
                            'name': field_name,
                            'display_name': field_name.replace('_', ' ').title(),
                            'field_type': 'text',
                            'is_global': True,
                            'description': f'Global custom field for {field_name}'
                        })
                        if not field_def:
                            print(f"❌ [CUSTOM_FIELDS] Failed to create global field: {field_name}")
                            return False
                    else:
                        print(f"✅ [CUSTOM_FIELDS] Global field already exists: {field_name}")
            
            # Process personal custom fields
            for field_name, value in personal_custom_metadata.items():
                if value is not None and value != '':
                    field_def = self._get_field_definition(field_name)
                    if not field_def:
                        print(f"📝 [CUSTOM_FIELDS] Creating personal field definition: {field_name}")
                        field_def = self.create_field_sync(user_id, {
                            'name': field_name,
                            'display_name': field_name.replace('_', ' ').title(),
                            'field_type': 'text',
                            'is_global': False,
                            'description': f'Personal custom field for {field_name}'
                        })
                        if not field_def:
                            print(f"❌ [CUSTOM_FIELDS] Failed to create personal field: {field_name}")
                            return False
                    else:
                        print(f"✅ [CUSTOM_FIELDS] Personal field already exists: {field_name}")
            
            total_fields = len(global_custom_metadata) + len(personal_custom_metadata)
            print(f"✅ [CUSTOM_FIELDS] Custom field definitions ensured for {total_fields} fields")
            return True
            
        except Exception as e:
            print(f"❌ [CUSTOM_FIELDS] Error ensuring custom fields exist: {e}")
            return False
        
    def migrate_custom_metadata_from_owns(self, book_id: str, user_id: str) -> bool:
        """Migrate custom metadata from OWNS relationship to new architecture."""
        print(f"🔄 [MIGRATION] Migrating custom metadata for book {book_id}, user {user_id}")
        
        try:
            # Get metadata from OWNS relationship
            owns_query = """
            MATCH (u:User {id: $user_id})-[r:OWNS]->(b:Book {id: $book_id})
            RETURN r.custom_metadata AS custom_metadata
            """
            
            owns_results = self.graph_storage.query(owns_query, {
                "book_id": book_id,
                "user_id": user_id
            })
            
            if owns_results and owns_results[0].get('custom_metadata'):
                metadata_json = owns_results[0].get('custom_metadata')
                if metadata_json:
                    try:
                        if isinstance(metadata_json, str):
                            old_metadata = json.loads(metadata_json)
                        elif isinstance(metadata_json, dict):
                            old_metadata = metadata_json
                        else:
                            return True  # Nothing to migrate
                        
                        if old_metadata:
                            print(f"🔄 [MIGRATION] Found {len(old_metadata)} metadata items to migrate")
                            
                            # Save using new architecture
                            success = self.save_custom_metadata_sync(book_id, user_id, old_metadata)
                            
                            if success:
                                # Clear the old metadata from OWNS relationship
                                clear_owns_query = """
                                MATCH (u:User {id: $user_id})-[r:OWNS]->(b:Book {id: $book_id})
                                SET r.custom_metadata = NULL
                                """
                                
                                self.graph_storage.query(clear_owns_query, {
                                    "book_id": book_id,
                                    "user_id": user_id
                                })
                                
                                print(f"✅ [MIGRATION] Successfully migrated {len(old_metadata)} metadata items")
                                return True
                            else:
                                print(f"❌ [MIGRATION] Failed to save migrated metadata")
                                return False
                                
                    except (json.JSONDecodeError, TypeError) as e:
                        print(f"❌ [MIGRATION] Error parsing old metadata JSON: {e}")
                        return False
            
            print(f"📝 [MIGRATION] No metadata to migrate")
            return True
            
        except Exception as e:
            print(f"❌ [MIGRATION] Error migrating custom metadata: {e}")
            return False

    def migrate_all_custom_metadata_from_owns(self) -> bool:
        """Migrate all custom metadata from OWNS relationships to new architecture."""
        print(f"🔄 [MIGRATION] Starting migration of all custom metadata from OWNS relationships")
        
        try:
            # Find all OWNS relationships with custom metadata
            migration_query = """
            MATCH (u:User)-[r:OWNS]->(b:Book)
            WHERE r.custom_metadata IS NOT NULL AND r.custom_metadata <> ''
            RETURN u.id AS user_id, b.id AS book_id, r.custom_metadata AS custom_metadata
            """
            
            migration_results = self.graph_storage.query(migration_query)
            
            if not migration_results:
                print(f"📝 [MIGRATION] No OWNS relationships with custom metadata found")
                return True
            
            print(f"🔄 [MIGRATION] Found {len(migration_results)} OWNS relationships with custom metadata to migrate")
            
            migrated_count = 0
            failed_count = 0
            
            for result in migration_results:
                user_id = result.get('col_0')  # u.id
                book_id = result.get('col_1')  # b.id
                
                if user_id and book_id:
                    success = self.migrate_custom_metadata_from_owns(book_id, user_id)
                    if success:
                        migrated_count += 1
                    else:
                        failed_count += 1
                        print(f"❌ [MIGRATION] Failed to migrate metadata for user {user_id}, book {book_id}")
            
            print(f"✅ [MIGRATION] Migration complete: {migrated_count} successful, {failed_count} failed")
            return failed_count == 0
            
        except Exception as e:
            print(f"❌ [MIGRATION] Error during bulk migration: {e}")
            return False

    def cleanup_old_custom_field_nodes(self) -> bool:
        """Clean up old CustomFieldValue nodes that are no longer needed."""
        print(f"🧹 [CLEANUP] Cleaning up old CustomFieldValue nodes")
        
        try:
            # Delete all CustomFieldValue nodes and their relationships
            cleanup_query = """
            MATCH (cfv:CustomFieldValue)
            DETACH DELETE cfv
            """
            
            self.graph_storage.query(cleanup_query)
            print(f"✅ [CLEANUP] Cleaned up old CustomFieldValue nodes")
            return True
            
        except Exception as e:
            print(f"❌ [CLEANUP] Error cleaning up old nodes: {e}")
            return False

