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
            # Try to create CustomField table (will fail if it already exists)
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
                
            # Try to create CustomFieldValue table (will fail if it already exists)
            create_value_table_query = """
            CREATE NODE TABLE CustomFieldValue (
                id STRING,
                field_id STRING,
                book_id STRING,
                user_id STRING,
                value STRING,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                PRIMARY KEY (id)
            )
            """
            try:
                self.graph_storage.query(create_value_table_query)
                print("📝 [CUSTOM_FIELDS] Created CustomFieldValue table")
            except Exception as e:
                if "already exists" in str(e):
                    print("📝 [CUSTOM_FIELDS] CustomFieldValue table already exists")
                else:
                    raise e
                
            # Try to create relationships (will fail if they already exist)
            create_field_rel_query = """
            CREATE REL TABLE HAS_FIELD (FROM CustomFieldValue TO CustomField)
            """
            try:
                self.graph_storage.query(create_field_rel_query)
                print("📝 [CUSTOM_FIELDS] Created HAS_FIELD relationship")
            except Exception as e:
                if "already exists" in str(e):
                    print("📝 [CUSTOM_FIELDS] HAS_FIELD relationship already exists")
                else:
                    raise e
                
            create_book_rel_query = """
            CREATE REL TABLE HAS_CUSTOM_VALUE (FROM Book TO CustomFieldValue)
            """
            try:
                self.graph_storage.query(create_book_rel_query)
                print("📝 [CUSTOM_FIELDS] Created HAS_CUSTOM_VALUE relationship")
            except Exception as e:
                if "already exists" in str(e):
                    print("📝 [CUSTOM_FIELDS] HAS_CUSTOM_VALUE relationship already exists")
                else:
                    raise e
                
        except Exception as e:
            print(f"❌ [CUSTOM_FIELDS] Error ensuring tables: {e}")
            # Tables might already exist, continue
    
    def get_custom_metadata_for_display(self, custom_metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Convert custom metadata to display format."""
        if not custom_metadata:
            return []
        
        display_items = []
        for key, value in custom_metadata.items():
            if value is not None and value != '':
                # Try to get field definition for better display
                field_def = self._get_field_definition(key)
                display_name = field_def.get('display_name', key.replace('_', ' ').title()) if field_def else key.replace('_', ' ').title()
                field_type = field_def.get('field_type', 'text') if field_def else 'text'
                
                display_items.append({
                    'field_name': key,
                    'display_name': display_name,
                    'value': str(value),
                    'field_type': field_type
                })
        
        return display_items
    
    def _get_field_definition(self, field_name: str) -> Optional[Dict[str, Any]]:
        """Get field definition by name."""
        try:
            query = "MATCH (f:CustomField) WHERE f.name = $name RETURN f"
            results = self.graph_storage.query(query, {"name": field_name})
            
            if results and len(results) > 0:
                return results[0].get('f', {})
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
        """Create a custom field definition."""
        field_name = field_data.get('name', 'unknown_field')
        print(f"📝 [CUSTOM_FIELDS] Creating field '{field_name}' for user {user_id}")
        
        try:
            field_id = f"field_{datetime.utcnow().timestamp()}"
            current_time = datetime.utcnow()
            
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
                'display_name': field_data.get('display_name', field_name),
                'field_type': field_data.get('field_type', 'text'),
                'description': field_data.get('description', ''),
                'user_id': user_id,
                'is_global': field_data.get('is_global', True),  # Default to global
                'created_at': current_time,
                'updated_at': current_time
            }
            
            result = self.graph_storage.query(query, params)
            
            if result and len(result) > 0:
                created_field = result[0].get('f', {})
                print(f"✅ [CUSTOM_FIELDS] Created field '{field_name}' with ID {field_id}")
                return {
                    'id': created_field.get('id'),
                    'name': created_field.get('name'),
                    'display_name': created_field.get('display_name'),
                    'field_type': created_field.get('field_type'),
                    'description': created_field.get('description'),
                    'is_global': created_field.get('is_global'),
                    'created_at': created_field.get('created_at').isoformat() if created_field.get('created_at') else None
                }
            else:
                print(f"❌ [CUSTOM_FIELDS] Failed to create field '{field_name}'")
                return None
                
        except Exception as e:
            print(f"❌ [CUSTOM_FIELDS] Error creating field: {e}")
            traceback.print_exc()
            return None
    
    def save_custom_metadata_sync(self, book_id: str, user_id: str, custom_metadata: Dict[str, Any]) -> bool:
        """Save custom metadata for a book."""
        print(f"📝 [CUSTOM_FIELDS] Saving custom metadata for book {book_id}, user {user_id}")
        
        try:
            # First, delete existing custom field values for this book/user
            delete_query = """
            MATCH (b:Book {id: $book_id})-[r:HAS_CUSTOM_VALUE]->(cfv:CustomFieldValue)
            WHERE cfv.user_id = $user_id
            DELETE r, cfv
            """
            
            self.graph_storage.query(delete_query, {
                "book_id": book_id,
                "user_id": user_id
            })
            
            # Save each custom field value
            for field_name, value in custom_metadata.items():
                if value is not None and value != '':
                    # Get or create field definition
                    field_def = self._get_field_definition(field_name)
                    if not field_def:
                        # Create field definition
                        field_def = self.create_field_sync(user_id, {
                            'name': field_name,
                            'display_name': field_name.replace('_', ' ').title(),
                            'field_type': 'text',
                            'is_global': False
                        })
                    
                    if field_def:
                        # Create custom field value
                        value_id = f"cfv_{datetime.utcnow().timestamp()}"
                        current_time = datetime.utcnow()
                        
                        create_value_query = """
                        MATCH (b:Book {id: $book_id}), (f:CustomField {name: $field_name})
                        CREATE (cfv:CustomFieldValue {
                            id: $value_id,
                            field_id: f.id,
                            book_id: $book_id,
                            user_id: $user_id,
                            value: $value,
                            created_at: $created_at,
                            updated_at: $updated_at
                        })
                        CREATE (b)-[:HAS_CUSTOM_VALUE]->(cfv)
                        CREATE (cfv)-[:HAS_FIELD]->(f)
                        """
                        
                        self.graph_storage.query(create_value_query, {
                            'value_id': value_id,
                            'book_id': book_id,
                            'user_id': user_id,
                            'field_name': field_name,
                            'value': str(value),
                            'created_at': current_time,
                            'updated_at': current_time
                        })
            
            print(f"✅ [CUSTOM_FIELDS] Saved {len(custom_metadata)} custom metadata items")
            return True
            
        except Exception as e:
            print(f"❌ [CUSTOM_FIELDS] Error saving custom metadata: {e}")
            traceback.print_exc()
            return False
    
    def get_custom_metadata_sync(self, book_id: str, user_id: str) -> Dict[str, Any]:
        """Get custom metadata for a book."""
        print(f"📝 [CUSTOM_FIELDS] Getting custom metadata for book {book_id}, user {user_id}")
        
        try:
            query = """
            MATCH (b:Book {id: $book_id})-[:HAS_CUSTOM_VALUE]->(cfv:CustomFieldValue)-[:HAS_FIELD]->(f:CustomField)
            WHERE cfv.user_id = $user_id
            RETURN f.name AS field_name, cfv.value AS value, f.display_name AS display_name, f.field_type AS field_type
            """
            
            results = self.graph_storage.query(query, {
                "book_id": book_id,
                "user_id": user_id
            })
            
            custom_metadata = {}
            for result in results:
                field_name = result.get('field_name')
                value = result.get('value')
                if field_name and value:
                    custom_metadata[field_name] = value
            
            print(f"📝 [CUSTOM_FIELDS] Found {len(custom_metadata)} custom metadata items")
            return custom_metadata
            
        except Exception as e:
            print(f"❌ [CUSTOM_FIELDS] Error getting custom metadata: {e}")
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
                RETURN f
                ORDER BY f.created_at DESC
                """
                params = {"user_id": user_id}
            elif is_global:
                # Get only global fields
                query = """
                MATCH (f:CustomField) 
                WHERE f.is_global = true
                RETURN f
                ORDER BY f.created_at DESC
                """
                params = {}
            else:
                # Get only user-specific fields
                query = """
                MATCH (f:CustomField) 
                WHERE f.created_by_user_id = $user_id AND f.is_global = false
                RETURN f
                ORDER BY f.created_at DESC
                """
                params = {"user_id": user_id}
            
            results = self.graph_storage.query(query, params)
            
            fields = []
            for result in results:
                # Try both 'field' and 'f' and also check col_0 for compatibility
                field_data = result.get('field') or result.get('f') or result.get('col_0')
                
                if field_data:
                    fields.append({
                        'id': field_data.get('id'),
                        'name': field_data.get('name'),
                        'display_name': field_data.get('display_name'),
                        'field_type': field_data.get('field_type', 'text'),
                        'description': field_data.get('description', ''),
                        'is_global': field_data.get('is_global', False),
                        'created_at': field_data.get('created_at')
                    })
            
            print(f"📝 [CUSTOM_FIELDS] Found {len(fields)} available fields")
            return fields
            
        except Exception as e:
            print(f"❌ [CUSTOM_FIELDS] Error getting available fields: {e}")
            return []
    
