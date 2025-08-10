"""
Category service for KuzuDB operations.
"""
import uuid
import traceback
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from app.domain.models import Category
from app.infrastructure.kuzu_repositories import KuzuCategoryRepository
from app.infrastructure.kuzu_graph import safe_execute_kuzu_query, safe_get_kuzu_connection
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


from .kuzu_async_helper import run_async


class KuzuCategoryService:
    """Service for category management and hierarchy operations."""
    
    def __init__(self, user_id: Optional[str] = None):
        """
        Initialize category service with thread-safe database access.
        
        Args:
            user_id: User identifier for tracking and isolation
        """
        self.user_id = user_id or "category_service"
        self.category_repo = KuzuCategoryRepository()
    
    async def list_all_categories(self) -> List[Dict[str, Any]]:
        """Get all categories."""
        try:
            query = """
            MATCH (c:Category)
            RETURN c
            ORDER BY c.name ASC
            """
            
            # Use safe query execution
            # Use safe query execution and convert result
            raw_result = safe_execute_kuzu_query(
                query=query,
                user_id=self.user_id,
                operation="list_all_categories"
            )
            
            results = _convert_query_result_to_list(raw_result)
            
            categories = []
            if results:
                for result in results:
                    if 'result' in result:
                        categories.append(result['result'])
                    elif 'col_0' in result:
                        categories.append(result['col_0'])
                    elif 'c' in result:
                        categories.append(result['c'])
                    else:
                        # Return the first value if it looks like a category
                        for key, value in result.items():
                            if isinstance(value, dict) and 'id' in value and 'name' in value:
                                categories.append(value)
                                break
            
            return categories
            
        except Exception as e:
            return []
    
    async def get_category_by_id(self, category_id: str) -> Optional[Dict[str, Any]]:
        """Get a category by ID."""
        try:
            query = """
            MATCH (c:Category {id: $category_id})
            RETURN c
            """
            
            # Use safe query execution and convert result
            raw_result = safe_execute_kuzu_query(
                query=query,
                params={"category_id": category_id},
                user_id=self.user_id,
                operation="get_category_by_id"
            )
            
            results = _convert_query_result_to_list(raw_result)
            
            if results and len(results) > 0:
                result = results[0]
                # Check different possible result formats
                if 'result' in result:
                    return result['result']
                elif 'col_0' in result:
                    return result['col_0']
                elif 'c' in result:
                    return result['c']
                else:
                    # Return the first value if it looks like a category
                    for key, value in result.items():
                        if isinstance(value, dict) and 'id' in value and 'name' in value:
                            return value
            
            return None
            
        except Exception as e:
            return None
    
    def get_category_by_id_with_hierarchy(self, category_id: str) -> Optional[Category]:
        """Get a category by ID with full hierarchy (sync version)."""
        try:
            return self._build_category_with_hierarchy(category_id)
        except Exception as e:
            return None
    
    def _build_category_with_hierarchy(self, category_id: str) -> Optional[Category]:
        """Build a Category object with proper parent hierarchy."""
        try:
            # Get the category data
            query = """
            MATCH (c:Category {id: $category_id})
            RETURN c
            """
            
            # Use safe query execution and convert result
            raw_result = safe_execute_kuzu_query(
                query=query,
                params={"category_id": category_id},
                user_id=self.user_id,
                operation="_build_category_with_hierarchy"
            )
            
            results = _convert_query_result_to_list(raw_result)
            
            if not results or len(results) == 0:
                return None
            
            result = results[0]
            category_data = None
            
            # Check different possible result formats
            if 'result' in result:
                category_data = result['result']
            elif 'col_0' in result:
                category_data = result['col_0']
            elif 'c' in result:
                category_data = result['c']
            else:
                # Return the first value if it looks like a category
                for key, value in result.items():
                    if isinstance(value, dict) and 'id' in value and 'name' in value:
                        category_data = value
                        break
            
            if not category_data:
                return None
            
            # Create the Category object
            created_at = category_data.get('created_at')
            updated_at = category_data.get('updated_at')
            
            category = Category(
                id=category_data.get('id'),
                name=category_data.get('name', ''),
                normalized_name=category_data.get('normalized_name', ''),
                parent_id=category_data.get('parent_id'),
                description=category_data.get('description'),
                level=category_data.get('level', 0),
                color=category_data.get('color'),
                icon=category_data.get('icon'),
                aliases=category_data.get('aliases', []),
                book_count=category_data.get('book_count', 0),
                user_book_count=category_data.get('user_book_count', 0),
                created_at=created_at if created_at else datetime.now(timezone.utc),
                updated_at=updated_at if updated_at else datetime.now(timezone.utc)
            )
            
            # Recursively build parent hierarchy if parent_id exists
            if category.parent_id:
                parent_category = self._build_category_with_hierarchy(category.parent_id)
                category.parent = parent_category
            
            return category
            
        except Exception as e:
            return None
    
    async def get_child_categories(self, parent_id: str) -> List[Dict[str, Any]]:
        """Get child categories for a parent category."""
        try:
            # Query for categories that have this parent_id
            query = """
            MATCH (c:Category)
            WHERE c.parent_id = $parent_id
            RETURN c
            ORDER BY c.name ASC
            """
            
            # Use safe query execution and convert result
            raw_result = safe_execute_kuzu_query(
                query=query,
                params={"parent_id": parent_id},
                user_id=self.user_id,
                operation="get_child_categories"
            )
            
            results = _convert_query_result_to_list(raw_result)
            
            categories = []
            for result in results:
                # Check different possible result formats
                category_data = None
                if 'result' in result:
                    category_data = result['result']
                elif 'col_0' in result:
                    category_data = result['col_0']
                elif 'c' in result:
                    category_data = result['c']
                else:
                    # Return the first value if it looks like a category
                    for key, value in result.items():
                        if isinstance(value, dict) and 'id' in value and 'name' in value:
                            category_data = value
                            break
                
                if category_data:
                    categories.append(category_data)
            
            return categories
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return []
    
    async def get_books_by_category(self, category_id: str, include_subcategories: bool = False) -> List[Dict[str, Any]]:
        """Get books in a category."""
        try:
            if include_subcategories:
                # Get all descendant categories
                descendant_ids = await self._get_all_descendant_categories(category_id)
                descendant_ids.append(category_id)  # Include the category itself
                
                # Build query for multiple categories
                query = """
                MATCH (b:Book)-[:CATEGORIZED_AS]->(c:Category)
                WHERE c.id IN $category_ids
                RETURN DISTINCT b
                ORDER BY b.title ASC
                """
                
                # Use safe query execution and convert result
                raw_result = safe_execute_kuzu_query(
                    query=query,
                    params={"category_ids": descendant_ids},
                    user_id=self.user_id,
                    operation="get_books_by_category_with_subcategories"
                )
            else:
                # Query for books in this specific category
                query = """
                MATCH (b:Book)-[:CATEGORIZED_AS]->(c:Category {id: $category_id})
                RETURN b
                ORDER BY b.title ASC
                """
                
                # Use safe query execution and convert result
                raw_result = safe_execute_kuzu_query(
                    query=query,
                    params={"category_id": category_id},
                    user_id=self.user_id,
                    operation="get_books_by_category_single"
                )
            
            # Convert results for both cases
            results = _convert_query_result_to_list(raw_result)
            
            books = []
            for result in results:
                book_data = None
                if 'result' in result:
                    book_data = dict(result['result'])
                elif 'col_0' in result:
                    book_data = dict(result['col_0'])
                
                if book_data:
                    # Ensure uid is available as alias for id (for template compatibility)
                    if 'id' in book_data:
                        book_data['uid'] = book_data['id']
                    books.append(book_data)
            
            return books
            
        except Exception as e:
            return []
    
    async def _get_all_descendant_categories(self, category_id: str) -> List[str]:
        """Get all descendant category IDs recursively."""
        try:
            descendant_ids = []
            
            # Get immediate children first
            immediate_children = await self.get_child_categories(category_id)
            
            for child in immediate_children:
                child_id = child.get('id')
                if child_id:
                    descendant_ids.append(child_id)
                    # Recursively get descendants of this child
                    child_descendants = await self._get_all_descendant_categories(child_id)
                    descendant_ids.extend(child_descendants)
            
            print(f"🌳 [GET_DESCENDANTS] Found {len(descendant_ids)} descendant categories for {category_id}")
            return descendant_ids
            
        except Exception as e:
            traceback.print_exc()
            return []
    
    async def create_category(self, category: Category) -> Optional[Category]:
        """Create a new category."""
        try:
            print(f"📁 [CREATE_CATEGORY] Creating category: {category.name}")
            
            # Use the repository to create the category
            created_category = await self.category_repo.create(category)
            
            if created_category:
                return created_category
            else:
                return None
                
        except Exception as e:
            traceback.print_exc()
            return None
    
    async def update_category(self, category: Category) -> Optional[Category]:
        """Update an existing category."""
        try:
            print(f"📁 [UPDATE_CATEGORY] Updating category: {category.name}")
            
            # Use the repository to update the category
            updated_category = await self.category_repo.update(category)
            
            if updated_category:
                return updated_category
            else:
                return None
                
        except Exception as e:
            traceback.print_exc()
            return None
    
    async def delete_category(self, category_id: str) -> bool:
        """Delete a category."""
        try:
            print(f"🗑️ [DELETE_CATEGORY] Deleting category: {category_id}")
            
            # Delete using Kuzu query with DETACH DELETE to remove relationships
            query = """
            MATCH (c:Category {id: $category_id})
            DETACH DELETE c
            """
            
            # Use safe query execution
            safe_execute_kuzu_query(
                query=query,
                params={"category_id": category_id},
                user_id=self.user_id,
                operation="delete_category"
            )
            return True
            
        except Exception as e:
            traceback.print_exc()
            return False
    
    async def merge_categories(self, primary_category_id: str, merge_category_ids: List[str]) -> bool:
        """Merge multiple categories into a primary category."""
        try:
            print(f"🔄 [MERGE_CATEGORIES] Merging {len(merge_category_ids)} categories into {primary_category_id}")
            
            # For each category to merge
            for merge_id in merge_category_ids:
                # Move all books from merge category to primary category
                move_books_query = """
                MATCH (b:Book)-[r:CATEGORIZED_AS]->(merge:Category {id: $merge_id})
                MATCH (primary:Category {id: $primary_id})
                DELETE r
                CREATE (b)-[:CATEGORIZED_AS]->(primary)
                """
                
                # Use safe query execution
                safe_execute_kuzu_query(
                    query=move_books_query,
                    params={
                        "merge_id": merge_id,
                        "primary_id": primary_category_id
                    },
                    user_id=self.user_id,
                    operation="merge_categories_move_books"
                )
                
                # Move all child categories from merge category to primary category
                move_children_query = """
                MATCH (child:Category {parent_id: $merge_id})
                SET child.parent_id = $primary_id
                """
                
                # Use safe query execution
                safe_execute_kuzu_query(
                    query=move_children_query,
                    params={
                        "merge_id": merge_id,
                        "primary_id": primary_category_id
                    },
                    user_id=self.user_id,
                    operation="merge_categories_move_children"
                )
                
                # Delete the merge category
                await self.delete_category(merge_id)
            
            return True
            
        except Exception as e:
            traceback.print_exc()
            return False
    
    async def search_categories(self, query: str, limit: int = 10, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search categories by name or description."""
        try:
            search_query = """
            MATCH (c:Category)
            WHERE toLower(c.name) CONTAINS toLower($query) 
               OR toLower(c.description) CONTAINS toLower($query)
            RETURN c
            ORDER BY c.name ASC
            LIMIT $limit
            """
            
            # Use safe query execution and convert result
            raw_result = safe_execute_kuzu_query(
                query=search_query,
                params={
                    "query": query,
                    "limit": limit
                },
                user_id=self.user_id,
                operation="search_categories"
            )
            
            results = _convert_query_result_to_list(raw_result)
            
            categories = []
            for result in results:
                # Check different possible result formats
                category_data = None
                if 'result' in result:
                    category_data = result['result']
                elif 'col_0' in result:
                    category_data = result['col_0']
                elif 'c' in result:
                    category_data = result['c']
                else:
                    # Return the first value if it looks like a category
                    for key, value in result.items():
                        if isinstance(value, dict) and 'id' in value and 'name' in value:
                            category_data = value
                            break
                
                if category_data:
                    # Add full_path for better search results
                    category_data['full_path'] = category_data.get('name', '')
                    categories.append(category_data)
            
            return categories
            
        except Exception as e:
            return []
    
    async def get_root_categories(self, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get root categories (categories without parent)."""
        try:
            # Check for categories with NULL parent_id OR empty/missing parent_id
            query = """
            MATCH (c:Category)
            WHERE c.parent_id IS NULL OR c.parent_id = ''
            RETURN c
            ORDER BY c.name ASC
            """
            
            # Use safe query execution and convert result
            raw_result = safe_execute_kuzu_query(
                query=query,
                params={},
                user_id=self.user_id,
                operation="get_root_categories"
            )
            
            results = _convert_query_result_to_list(raw_result)
            
            categories = []
            for result in results:
                if 'col_0' in result:
                    categories.append(result['col_0'])
                elif 'result' in result:
                    categories.append(result['result'])
                elif 'c' in result:
                    categories.append(result['c'])
            
            return categories
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return []

    async def get_category_book_counts(self) -> Dict[str, int]:
        """Get book counts for all categories efficiently."""
        try:
            # Query to count books per category
            query = """
            MATCH (b:Book)-[:CATEGORIZED_AS]->(c:Category)
            RETURN c.id as category_id, COUNT(DISTINCT b) as book_count
            """
            
            # Use safe query execution and convert result
            raw_result = safe_execute_kuzu_query(
                query=query,
                params={},
                user_id=self.user_id,
                operation="get_category_book_counts"
            )
            
            results = _convert_query_result_to_list(raw_result)
            
            counts = {}
            for result in results:
                category_id = result.get('category_id')
                book_count = result.get('book_count', 0)
                if category_id:
                    counts[category_id] = book_count
            
            return counts
            
        except Exception as e:
            return {}

    # Sync wrappers for backward compatibility
    def get_category_book_counts_sync(self) -> Dict[str, int]:
        """Get book counts for all categories (sync version)."""
        return run_async(self.get_category_book_counts())
    
    def list_all_categories_sync(self) -> List[Dict[str, Any]]:
        """Get all categories (sync version)."""
        return run_async(self.list_all_categories())
    
    def get_category_by_id_sync(self, category_id: str) -> Optional[Category]:
        """Get a category by ID with full hierarchy (sync version)."""
        return self.get_category_by_id_with_hierarchy(category_id)
    
    def get_child_categories_sync(self, parent_id: str) -> List[Dict[str, Any]]:
        """Get child categories for a parent category (sync version)."""
        return run_async(self.get_child_categories(parent_id))
    
    def get_category_children_sync(self, category_id: str) -> List[Dict[str, Any]]:
        """Get children of a category (sync version)."""
        return run_async(self.get_child_categories(category_id))
    
    def get_books_by_category_sync(self, category_id: str, include_subcategories: bool = False) -> List[Dict[str, Any]]:
        """Get books in a category (sync version)."""
        return run_async(self.get_books_by_category(category_id, include_subcategories))
    
    def create_category_sync(self, category_data: Dict[str, Any]) -> Optional[Category]:
        """Create a new category (sync version)."""
        # Convert dict to Category object if needed
        if isinstance(category_data, dict):
            category = Category(
                id=category_data.get('id'),
                name=category_data.get('name', ''),
                normalized_name=category_data.get('normalized_name', ''),
                description=category_data.get('description'),
                parent_id=category_data.get('parent_id'),
                level=category_data.get('level', 0),
                color=category_data.get('color'),
                icon=category_data.get('icon'),
                aliases=category_data.get('aliases', []),
                book_count=category_data.get('book_count', 0),
                user_book_count=category_data.get('user_book_count', 0),
                created_at=category_data.get('created_at', datetime.now(timezone.utc)),
                updated_at=category_data.get('updated_at', datetime.now(timezone.utc))
            )
        else:
            category = category_data
        
        return run_async(self.create_category(category))
    
    def update_category_sync(self, category: Category) -> Optional[Category]:
        """Update a category (sync version)."""
        return run_async(self.update_category(category))
    
    def delete_category_sync(self, category_id: str) -> bool:
        """Delete a category (sync version)."""
        return run_async(self.delete_category(category_id))
    
    def merge_categories_sync(self, primary_category_id: str, merge_category_ids: List[str]) -> bool:
        """Merge categories (sync version)."""
        return run_async(self.merge_categories(primary_category_id, merge_category_ids))
    
    def search_categories_sync(self, query: str, limit: int = 10, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Search categories (sync version)."""
        return run_async(self.search_categories(query, limit, user_id))
    
    def get_root_categories_sync(self, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get root categories (sync version)."""
        return run_async(self.get_root_categories(user_id))
    
    # ==========================================
    # Sync Wrapper Methods for Compatibility
    # ==========================================
    
    def _get_all_descendant_categories_sync(self, category_id: str) -> List[str]:
        """Sync wrapper for _get_all_descendant_categories."""
        return run_async(self._get_all_descendant_categories(category_id))
    
    def _build_category_with_hierarchy_sync(self, category_data: Dict[str, Any]) -> Dict[str, Any]:
        """Sync wrapper for building category with hierarchy."""
        # This method operates on category data dict, not ID
        try:
            category_id = category_data.get('id')
            if not category_id:
                return category_data
            
            # Build hierarchy using the existing method
            category_obj = self._build_category_with_hierarchy(category_id)
            
            if category_obj:
                # Convert back to dict format
                result = category_data.copy()
                if hasattr(category_obj, 'parent'):
                    result['parent'] = category_obj.parent
                return result
            
            return category_data
            
        except Exception as e:
            return category_data
