"""
Kuzu Import Mapping Service

Handles import mapping templates for CSV import functionality using KuzuDB.
"""

import traceback
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

from ..infrastructure.kuzu_graph import safe_execute_kuzu_query
from ..domain.models import ImportMappingTemplate
from .kuzu_async_helper import run_async

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
                    # Single column result - keep both legacy and new keys
                    value = row[0]
                    rows.append({'col_0': value, 'result': value})
                else:
                    # Multi-column result - use col_0, col_1, etc. format for compatibility
                    row_dict = {}
                    for i, value in enumerate(row):
                        row_dict[f'col_{i}'] = value
                    rows.append(row_dict)
        
        return rows
    except Exception as e:
        print(f"Error converting query result to list: {e}")
        return []


def _first_column_payload(row_data: Dict[str, Any]) -> Any:
    """Return the first-column payload from a converted row."""
    if not isinstance(row_data, dict):
        return row_data or {}
    for key in ('col_0', 'result'):
        if key in row_data and row_data[key] is not None:
            return row_data[key]
    if row_data:
        # Fall back to the first available value
        return next(iter(row_data.values()))
    return {}


class KuzuImportMappingService:
    """Service for managing import mapping templates in KuzuDB."""
    
    def __init__(self):
        self._ensure_import_mapping_tables()
    
    def _ensure_import_mapping_tables(self):
        """Ensure import mapping tables exist in KuzuDB."""
        try:
            # Try to create ImportMappingTemplate table (will fail if it already exists)
            create_table_query = """
            CREATE NODE TABLE ImportMappingTemplate (
                id STRING,
                user_id STRING,
                name STRING,
                description STRING,
                source_type STRING,
                sample_headers STRING,
                field_mappings STRING,
                times_used INT64,
                last_used TIMESTAMP,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                PRIMARY KEY (id)
            )
            """
            try:
                safe_execute_kuzu_query(create_table_query)
                logger.debug("Created ImportMappingTemplate table")
            except Exception as e:
                if "already exists" in str(e):
                    logger.debug("ImportMappingTemplate table already exists")
                else:
                    raise e
                
        except Exception as e:
            # Tables might already exist, continue
            pass
    
    def get_template_by_id_sync(self, template_id: str) -> Optional[ImportMappingTemplate]:
        """Get an import mapping template by ID."""
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"📋 [IMPORT_MAPPING] Getting template by ID: {template_id}")
        
        try:
            query = "MATCH (t:ImportMappingTemplate) WHERE t.id = $template_id RETURN t"
            result = safe_execute_kuzu_query(query, {"template_id": template_id})
            results = _convert_query_result_to_list(result)
            
            if results and len(results) > 0:
                template_data = _first_column_payload(results[0])
                return self._dict_to_template(template_data)
            return None
            
        except Exception as e:
            return None
    
    def create_template_sync(self, template: ImportMappingTemplate) -> Optional[ImportMappingTemplate]:
        """Create a new import mapping template."""
        if logger.isEnabledFor(logging.INFO):
            logger.info(f"📋 [IMPORT_MAPPING] Creating template: {template.name}")
        
        try:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("📋 [IMPORT_MAPPING] Step 1: Checking if template already exists")
            
            # Check if template already exists first
            existing_query = """
            MATCH (t:ImportMappingTemplate {id: $id})
            RETURN t
            """
            
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("📋 [IMPORT_MAPPING] Step 2: Executing existing template query")
            existing_result = safe_execute_kuzu_query(existing_query, {"id": template.id})
            existing_results = _convert_query_result_to_list(existing_result)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"📋 [IMPORT_MAPPING] Step 3: Existing results: {existing_results}")
            
            if existing_results and len(existing_results) > 0:
                if logger.isEnabledFor(logging.INFO):
                    logger.info(f"📋 [IMPORT_MAPPING] Template already exists: {template.name}")
                row_payload = _first_column_payload(existing_results[0])
                return self._dict_to_template(row_payload)
            
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("📋 [IMPORT_MAPPING] Step 4: Setting IDs and timestamps")
            # Generate ID if not provided
            if not template.id:
                template.id = f"template_{datetime.now(timezone.utc).timestamp()}"
            
            # Update timestamps
            template.created_at = datetime.now(timezone.utc)
            template.updated_at = datetime.now(timezone.utc)
            
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("📋 [IMPORT_MAPPING] Step 5: Template data before insertion:")
                logger.debug(f"📋 [IMPORT_MAPPING]   ID: {template.id}")
                logger.debug(f"📋 [IMPORT_MAPPING]   User ID: {template.user_id}")
                logger.debug(f"📋 [IMPORT_MAPPING]   Name: {template.name}")
                logger.debug(f"📋 [IMPORT_MAPPING]   Description: {template.description}")
                logger.debug(f"📋 [IMPORT_MAPPING]   Source Type: {template.source_type}")
                logger.debug(f"📋 [IMPORT_MAPPING]   Sample Headers Type: {type(template.sample_headers)}, Length: {len(template.sample_headers) if template.sample_headers else 0}")
                logger.debug(f"📋 [IMPORT_MAPPING]   Field Mappings Type: {type(template.field_mappings)}, Keys: {list(template.field_mappings.keys()) if template.field_mappings else []}")
                logger.debug(f"📋 [IMPORT_MAPPING]   Times Used: {template.times_used}")
                logger.debug(f"📋 [IMPORT_MAPPING]   Last Used: {template.last_used}")
                logger.debug(f"📋 [IMPORT_MAPPING]   Created At: {template.created_at}")
                logger.debug(f"📋 [IMPORT_MAPPING]   Updated At: {template.updated_at}")
            
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("📋 [IMPORT_MAPPING] Step 6: Serializing JSON fields")
            # Serialize JSON fields safely
            try:
                sample_headers_json = json.dumps(template.sample_headers)
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"📋 [IMPORT_MAPPING] Serialized sample_headers successfully (length: {len(sample_headers_json)})")
            except Exception as e:
                sample_headers_json = "[]"
            
            try:
                field_mappings_json = json.dumps(template.field_mappings)
                print(f"📋 [IMPORT_MAPPING] Serialized field_mappings successfully (length: {len(field_mappings_json)})")
            except Exception as e:
                field_mappings_json = "{}"
            
            print(f"📋 [IMPORT_MAPPING] Step 7: Preparing query")
            # Insert template with proper data types
            query = """
            CREATE (t:ImportMappingTemplate {
                id: $id,
                user_id: $user_id,
                name: $name,
                description: $description,
                source_type: $source_type,
                sample_headers: $sample_headers,
                field_mappings: $field_mappings,
                times_used: $times_used,
                last_used: $last_used,
                created_at: $created_at,
                updated_at: $updated_at
            })
            RETURN t
            """
            
            print(f"📋 [IMPORT_MAPPING] Step 8: Preparing parameters")
            params = {
                'id': template.id,
                'user_id': template.user_id,
                'name': template.name,
                'description': template.description,
                'source_type': template.source_type,
                'sample_headers': sample_headers_json,
                'field_mappings': field_mappings_json,
                'times_used': template.times_used,
                'last_used': template.last_used,
                'created_at': template.created_at,
                'updated_at': template.updated_at
            }
            
            print(f"📋 [IMPORT_MAPPING] Step 9: About to execute query")
            print(f"📋 [IMPORT_MAPPING] Query prepared")
            print(f"📋 [IMPORT_MAPPING] Params prepared")
            
            print(f"📋 [IMPORT_MAPPING] Step 10: Executing query now...")
            query_result = safe_execute_kuzu_query(query, params)
            result = _convert_query_result_to_list(query_result)
            print(f"📋 [IMPORT_MAPPING] Step 11: Query executed successfully")
            print(f"📋 [IMPORT_MAPPING] Query result: {result}")
            
            if result and len(result) > 0:
                created_template_data = _first_column_payload(result[0])
                print(f"📋 [IMPORT_MAPPING] Created template data: {created_template_data}")
                
                created_template = self._dict_to_template(created_template_data)
                if created_template:
                    return created_template
                else:
                    return None
            else:
                return None
                
        except Exception as e:
            traceback.print_exc()
            return None
    
    def update_template_sync(self, template: ImportMappingTemplate) -> Optional[ImportMappingTemplate]:
        """Update an existing import mapping template."""
        print(f"📋 [IMPORT_MAPPING] Updating template: {template.name}")
        
        try:
            # Update timestamp
            template.updated_at = datetime.now(timezone.utc)
            
            # Update template with proper data types
            query = """
            MATCH (t:ImportMappingTemplate {id: $id})
            SET t.user_id = $user_id,
                t.name = $name,
                t.description = $description,
                t.source_type = $source_type,
                t.sample_headers = $sample_headers,
                t.field_mappings = $field_mappings,
                t.times_used = $times_used,
                t.last_used = $last_used,
                t.updated_at = $updated_at
            RETURN t
            """
            
            params = {
                'id': template.id,
                'user_id': template.user_id,
                'name': template.name,
                'description': template.description,
                'source_type': template.source_type,
                'sample_headers': json.dumps(template.sample_headers),
                'field_mappings': json.dumps(template.field_mappings),
                'times_used': template.times_used,
                'last_used': template.last_used,
                'updated_at': template.updated_at
            }
            
            query_result = safe_execute_kuzu_query(query, params)
            result = _convert_query_result_to_list(query_result)
            
            if result and len(result) > 0:
                updated_template_data = _first_column_payload(result[0])
                updated_template = self._dict_to_template(updated_template_data)
                return updated_template
            else:
                return None
                
        except Exception as e:
            traceback.print_exc()
            return None
    
    def get_user_templates_sync(self, user_id: str) -> List[ImportMappingTemplate]:
        """Get all import mapping templates for a user."""
        print(f"📋 [IMPORT_MAPPING] Getting templates for user: {user_id}")
        
        try:
            # Get user-specific templates and system templates
            query = """
            MATCH (t:ImportMappingTemplate) 
            WHERE t.user_id = $user_id OR t.user_id = '__system__'
            RETURN t
            ORDER BY t.created_at DESC
            """
            
            query_result = safe_execute_kuzu_query(query, {"user_id": user_id})
            results = _convert_query_result_to_list(query_result)
            
            templates = []
            for result in results:
                template_data = _first_column_payload(result)
                template = self._dict_to_template(template_data)
                if template:
                    templates.append(template)
            
            print(f"📋 [IMPORT_MAPPING] Found {len(templates)} templates for user {user_id}")
            return templates
            
        except Exception as e:
            return []
    
    def detect_template_sync(self, headers: List[str], user_id: str) -> Optional[ImportMappingTemplate]:
        """Detect the best matching template for given headers."""
        print(f"📋 [IMPORT_MAPPING] Detecting template for headers: {headers[:5]}...")
        
        try:
            # Get all available templates for the user
            templates = self.get_user_templates_sync(user_id)
            
            best_match = None
            best_score = 0
            
            for template in templates:
                # Calculate match score based on header similarity
                score = self._calculate_header_match_score(headers, template.sample_headers)
                
                if score > best_score:
                    best_score = score
                    best_match = template
            
            # Only return if we have a reasonable match (at least 50% similarity)
            if best_match and best_score > 0.5:
                print(f"📋 [IMPORT_MAPPING] Detected template: {best_match.name} (score: {best_score:.2f})")
                return best_match
            
            print(f"📋 [IMPORT_MAPPING] No suitable template detected (best score: {best_score:.2f})")
            return None
            
        except Exception as e:
            return None
    
    def _calculate_header_match_score(self, headers1: List[str], headers2: List[str]) -> float:
        """Calculate similarity score between two header lists."""
        if not headers1 or not headers2:
            return 0.0
        
        # Normalize headers (case-insensitive comparison)
        normalized_headers1 = [h.lower().strip() for h in headers1]
        normalized_headers2 = [h.lower().strip() for h in headers2]
        
        # Count matches
        matches = 0
        for header in normalized_headers1:
            if header in normalized_headers2:
                matches += 1
        
        # Calculate score as percentage of matching headers
        total_headers = max(len(normalized_headers1), len(normalized_headers2))
        return matches / total_headers
    
    def delete_template_sync(self, template_id: str, user_id: str) -> bool:
        """Delete an import mapping template."""
        print(f"📋 [IMPORT_MAPPING] Deleting template: {template_id}")
        
        try:
            # First, check if user owns this template (or it's a system template they can modify)
            check_query = """
            MATCH (t:ImportMappingTemplate {id: $template_id})
            WHERE t.user_id = $user_id
            RETURN t
            """
            
            results = safe_execute_kuzu_query(check_query, {
                "template_id": template_id,
                "user_id": user_id
            })
            
            if not results:
                return False
            
            # Delete the template
            delete_query = """
            MATCH (t:ImportMappingTemplate {id: $template_id})
            DELETE t
            """
            
            safe_execute_kuzu_query(delete_query, {"template_id": template_id})
            
            return True
            
        except Exception as e:
            return False
    
    def _dict_to_template(self, data: Dict[str, Any]) -> Optional[ImportMappingTemplate]:
        """Convert dictionary data to ImportMappingTemplate object."""
        try:
            # Safety check for null or corrupted data
            if not data:
                return None
            if not isinstance(data, dict):
                try:
                    if hasattr(data, 'keys') and callable(getattr(data, 'keys')):
                        data = {key: data[key] for key in data.keys()}  # type: ignore[index]
                    elif hasattr(data, '__dict__'):
                        data = dict(data.__dict__)
                except Exception:
                    data = None
                if not isinstance(data, dict):
                    return None
                
            print(f"📋 [IMPORT_MAPPING] Converting dict to template: {data}")
            
            # Handle timestamp conversion from KuzuDB with better error handling
            created_at = data.get('created_at')
            updated_at = data.get('updated_at')
            last_used = data.get('last_used')
            
            print(f"📋 [IMPORT_MAPPING] Raw timestamps - created_at: {created_at} ({type(created_at)}), updated_at: {updated_at} ({type(updated_at)}), last_used: {last_used} ({type(last_used)})")
            
            # KuzuDB might return datetime objects directly or in other formats
            try:
                if created_at is not None and isinstance(created_at, str):
                    created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                elif created_at is not None and not isinstance(created_at, datetime):
                    # Handle other timestamp formats
                    created_at = datetime.now(timezone.utc)
            except Exception as e:
                created_at = datetime.now(timezone.utc)
            
            try:
                if updated_at is not None and isinstance(updated_at, str):
                    updated_at = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
                elif updated_at is not None and not isinstance(updated_at, datetime):
                    # Handle other timestamp formats
                    updated_at = datetime.now(timezone.utc)
            except Exception as e:
                updated_at = datetime.now(timezone.utc)
            
            try:
                if last_used is not None and isinstance(last_used, str):
                    last_used = datetime.fromisoformat(last_used.replace('Z', '+00:00'))
                elif last_used is not None and not isinstance(last_used, datetime):
                    # Handle other timestamp formats
                    last_used = None
            except Exception as e:
                last_used = None
            
            print(f"📋 [IMPORT_MAPPING] Parsed timestamps - created_at: {created_at}, updated_at: {updated_at}, last_used: {last_used}")
            
            # Parse JSON fields with better error handling
            sample_headers = data.get('sample_headers', '[]')
            try:
                if isinstance(sample_headers, str):
                    sample_headers = json.loads(sample_headers)
                elif not isinstance(sample_headers, list):
                    sample_headers = []
            except Exception as e:
                sample_headers = []
            
            field_mappings = data.get('field_mappings', '{}')
            try:
                if isinstance(field_mappings, str):
                    field_mappings = json.loads(field_mappings)
                elif not isinstance(field_mappings, dict):
                    field_mappings = {}
            except Exception as e:
                field_mappings = {}
            
            try:
                times_used = int(data.get('times_used', 0))
            except (ValueError, TypeError) as e:
                times_used = 0
            
            print(f"📋 [IMPORT_MAPPING] Creating ImportMappingTemplate with data...")
            
            # Create template with additional safety checks
            template_data = {
                'id': str(data.get('id', '')),
                'user_id': str(data.get('user_id', '')),
                'name': str(data.get('name', '')),
                'description': str(data.get('description', '') if data.get('description') else ''),
                'source_type': str(data.get('source_type', '')),
                'sample_headers': sample_headers,
                'field_mappings': field_mappings,
                'times_used': times_used,
                'last_used': last_used,
                'created_at': created_at or datetime.now(timezone.utc),
                'updated_at': updated_at or datetime.now(timezone.utc)
            }
            
            template = ImportMappingTemplate(**template_data)
            
            print(f"📋 [IMPORT_MAPPING] Successfully created template: {template.name}")
            return template
            
        except Exception as e:
            traceback.print_exc()
            return None
