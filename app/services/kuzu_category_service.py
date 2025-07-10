"""
Kuzu Category Service

Handles category management and hierarchy operations using Kuzu.
Focused responsibility: Category entity management and hierarchical relationships.
"""

import traceback
from typing import List, Optional, Dict, Any
from datetime import datetime

from ..domain.models import Category
from ..infrastructure.kuzu_graph import get_graph_storage, get_kuzu_database
from .kuzu_async_helper import run_async


class KuzuCategoryService:
    """Service for category management and hierarchy operations."""
    
    def __init__(self):
        self.graph_storage = get_graph_storage()
    
    async def list_all_categories(self) -> List[Dict[str, Any]]:
        """Get all categories."""
        try:
            query = """
            MATCH (c:Category)
            RETURN c
            ORDER BY c.name ASC
            """
            
            results = self.graph_storage.query(query)
            
            categories = []
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
            print(f"❌ [LIST_CATEGORIES] Error getting all categories: {e}")
            return []
    
    async def get_category_by_id(self, category_id: str) -> Optional[Dict[str, Any]]:
        """Get a category by ID."""
        try:
            query = """
            MATCH (c:Category {id: $category_id})
            RETURN c
            """
            
            results = self.graph_storage.query(query, {"category_id": category_id})
            
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
            print(f"❌ [GET_CATEGORY] Error getting category by ID {category_id}: {e}")
            return None
    
    def get_category_by_id_with_hierarchy(self, category_id: str) -> Optional[Category]:
        """Get a category by ID with full hierarchy (sync version)."""
        try:
            return self._build_category_with_hierarchy(category_id)
        except Exception as e:
            print(f"❌ [GET_CATEGORY_HIERARCHY] Error getting category by ID {category_id}: {e}")
            return None
    
    def _build_category_with_hierarchy(self, category_id: str) -> Optional[Category]:
        """Build a Category object with proper parent hierarchy."""
        try:
            # Get the category data
            query = """
            MATCH (c:Category {id: $category_id})
            RETURN c
            """
            
            results = self.graph_storage.query(query, {"category_id": category_id})
            
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
                created_at=created_at if created_at else datetime.utcnow(),
                updated_at=updated_at if updated_at else datetime.utcnow()
            )
            
            # Recursively build parent hierarchy if parent_id exists
            if category.parent_id:
                parent_category = self._build_category_with_hierarchy(category.parent_id)
                category.parent = parent_category
            
            return category
            
        except Exception as e:
            print(f"❌ [BUILD_HIERARCHY] Error building category hierarchy for {category_id}: {e}")
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
            
            results = self.graph_storage.query(query, {"parent_id": parent_id})
            
            categories = []
            for result in results:
                if 'col_0' in result:
                    categories.append(result['col_0'])
            
            return categories
            
        except Exception as e:
            print(f"❌ [GET_CHILDREN] Error getting child categories for {parent_id}: {e}")
            return []
    
    async def get_books_by_category(self, category_id: str, include_subcategories: bool = False) -> List[Dict[str, Any]]:
        """Get books in a category."""
        try:
            db = get_kuzu_database()
            
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
                
                results = db.query(query, {"category_ids": descendant_ids})
            else:
                # Query for books in this specific category
                query = """
                MATCH (b:Book)-[:CATEGORIZED_AS]->(c:Category {id: $category_id})
                RETURN b
                ORDER BY b.title ASC
                """
                
                results = db.query(query, {"category_id": category_id})
            
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
            print(f"❌ [GET_BOOKS_BY_CATEGORY] Error getting books by category {category_id}: {e}")
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
            print(f"❌ [GET_DESCENDANTS] Error getting descendant categories for {category_id}: {e}")
            traceback.print_exc()
            return []
    
    # Sync wrappers for backward compatibility
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
            print(f"❌ [BUILD_HIERARCHY_SYNC] Error building hierarchy: {e}")
            return category_data
