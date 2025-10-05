"""
Kuzu Search Service

Handles search, filtering, and discovery functionality using Kuzu.
Focused responsibility: Search operations and book discovery.

This service has been migrated to use the SafeKuzuManager pattern for
improved thread safety and connection management.
"""

import re
import traceback
from typing import List, Optional, Dict, Any
from datetime import date, timedelta

from ..domain.models import Book

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
from ..infrastructure.kuzu_repositories import KuzuUserRepository
from ..infrastructure.kuzu_graph import safe_execute_kuzu_query, safe_get_kuzu_connection
from .kuzu_async_helper import run_async
from .kuzu_relationship_service import KuzuRelationshipService
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


class KuzuSearchService:
    """
    Service for search and discovery operations with thread-safe operations.
    
    This service has been migrated to use the SafeKuzuManager pattern for
    improved thread safety and connection management.
    """
    
    def __init__(self, user_id: Optional[str] = None):
        """
        Initialize search service with thread-safe database access.
        
        Args:
            user_id: User identifier for tracking and isolation
        """
        self.user_id = user_id or "search_service"
        self.user_repo = KuzuUserRepository()
        self.relationship_service = KuzuRelationshipService()  # This service may not be migrated yet
    
    async def search_books(self, query: str, user_id: str, limit: int = 50) -> List[Book]:
        """Search books for a user."""
        try:
            # Get all user books first
            user_books = await self.relationship_service.get_books_for_user(user_id, limit=1000)  # Get all books for filtering

            tokens = [token.lower() for token in re.split(r"\s+", query.strip()) if token]
            if not tokens:
                # If the query is empty or whitespace, return recent slice of library
                return user_books[:limit]

            def _expand_name_segments(name: str) -> List[str]:
                cleaned = re.sub(r"[\s,]+", " ", name).strip().lower()
                if not cleaned:
                    return []
                segments = [segment for segment in cleaned.split(" ") if segment]
                # Preserve the full name first for contains matches, followed by individual parts
                return [cleaned] + segments

            filtered_books: List[Book] = []

            for book in user_books:
                search_blobs: List[str] = []

                if getattr(book, "title", None):
                    search_blobs.append(book.title.lower())
                    # Enable matching on individual title words
                    search_blobs.extend(word for word in re.split(r"\s+", book.title.lower()) if word)

                subtitle_value = getattr(book, "subtitle", None)
                if subtitle_value:
                    subtitle_lower = str(subtitle_value).lower()
                    search_blobs.append(subtitle_lower)
                    search_blobs.extend(word for word in re.split(r"\s+", subtitle_lower) if word)

                # Include authors/contributors (first or last name)
                if getattr(book, "contributors", None):
                    for contribution in book.contributors:
                        person = getattr(contribution, "person", None)
                        if person and getattr(person, "name", None):
                            search_blobs.extend(_expand_name_segments(person.name))
                        normalized_name = getattr(person, "normalized_name", None) if person else None
                        if normalized_name:
                            search_blobs.extend(_expand_name_segments(normalized_name))

                # Legacy authors property, if contributors missing
                if hasattr(book, "authors") and book.authors:
                    for author in book.authors:
                        if getattr(author, "name", None):
                            search_blobs.extend(_expand_name_segments(author.name))

                # Include series name when available
                series_obj = getattr(book, "series", None)
                if series_obj and getattr(series_obj, "name", None):
                    search_blobs.append(series_obj.name.lower())
                    search_blobs.extend(word for word in re.split(r"\s+", series_obj.name.lower()) if word)

                if not search_blobs:
                    continue

                if all(any(token in blob for blob in search_blobs) for token in tokens):
                    filtered_books.append(book)

                if len(filtered_books) >= limit:
                    break

            return filtered_books[:limit]

        except Exception as e:
            traceback.print_exc()
            return []
    
    async def search_books_global(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Search all books in the system (not user-specific)."""
        try:
            query_lower = query.lower()
            
            # Search books by title
            search_query = """
            MATCH (b:Book)
            WHERE toLower(b.title) CONTAINS $query
            RETURN b
            ORDER BY b.title ASC
            LIMIT $limit
            """
            
            # Use safe query execution and convert result
            raw_result = safe_execute_kuzu_query(
                query=search_query,
                params={
                    "query": query_lower,
                    "limit": limit
                },
                user_id=self.user_id,
                operation="search_books"
            )
            
            results = _convert_query_result_to_list(raw_result)
            
            books = []
            for result in results:
                if 'col_0' in result:
                    book_data = dict(result['col_0'])
                    # Ensure uid is available as alias for id
                    if 'id' in book_data:
                        book_data['uid'] = book_data['id']
                    books.append(book_data)
            
            return books
            
        except Exception as e:
            traceback.print_exc()
            return []
    
    async def get_book_by_isbn_for_user(self, isbn: str, user_id: str) -> Optional[Book]:
        """Get a book by ISBN for a specific user."""
        try:
            books = await self.relationship_service.get_books_for_user(user_id, limit=1000)  # Get enriched books
            for book in books:
                if (hasattr(book, 'isbn13') and book.isbn13 == isbn) or \
                   (hasattr(book, 'isbn10') and book.isbn10 == isbn):
                    return book
            return None
            
        except Exception as e:
            return None
    
    async def get_books_with_sharing_users(self, days_back: int = 30, limit: int = 20) -> List[Book]:
        """Get books from users who share reading activity, finished in the last N days."""
        try:
            # Get all users who share reading activity
            sharing_users = await self.user_repo.get_all()
            sharing_user_ids = []
            
            for user_dict in sharing_users:
                if (user_dict.get('share_reading_activity', False) and 
                    user_dict.get('is_active', True)):
                    sharing_user_ids.append(user_dict.get('id'))
            
            if not sharing_user_ids:
                return []
            
            # Get all books for sharing users using graph queries
            all_books = []
            for user_id in sharing_user_ids:
                books = await self.relationship_service.get_books_for_user(user_id, limit=1000)
                all_books.extend(books)
            
            # Filter for finished books in the specified time range
            cutoff_date = date.today() - timedelta(days=days_back)
            finished_books = [
                book for book in all_books 
                if hasattr(book, 'finish_date') and book.finish_date and book.finish_date >= cutoff_date
            ]
            
            # Sort by finish date descending and limit
            finished_books.sort(key=lambda b: getattr(b, 'finish_date', date.min) or date.min, reverse=True)
            return finished_books[:limit]
            
        except Exception as e:
            traceback.print_exc()
            return []
    
    async def get_currently_reading_shared(self, limit: int = 20) -> List[Book]:
        """Get currently reading books from users who share current reading."""
        try:
            # Get all users who share current reading
            sharing_users = await self.user_repo.get_all()
            sharing_user_ids = []
            
            for user_dict in sharing_users:
                if (user_dict.get('share_current_reading', False) and 
                    user_dict.get('is_active', True)):
                    sharing_user_ids.append(user_dict.get('id'))
            
            if not sharing_user_ids:
                return []
            
            # Get all books for sharing users using graph queries
            all_books = []
            for user_id in sharing_user_ids:
                books = await self.relationship_service.get_books_for_user(user_id, limit=1000)
                all_books.extend(books)
            
            # Filter for currently reading (has start_date but no finish_date)
            currently_reading = [
                book for book in all_books 
                if (hasattr(book, 'start_date') and book.start_date and 
                    (not hasattr(book, 'finish_date') or not book.finish_date))
            ]
            
            # Sort by start date descending and limit
            currently_reading.sort(key=lambda b: getattr(b, 'start_date', date.min) or date.min, reverse=True)
            return currently_reading[:limit]
            
        except Exception as e:
            traceback.print_exc()
            return []
    
    async def get_recommended_books(self, user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recommended books for a user based on their reading history."""
        try:
            # This is a simple recommendation system - can be enhanced later
            # For now, get books from users with similar reading patterns
            
            # Get user's books
            user_books = await self.relationship_service.get_books_for_user(user_id, limit=1000)
            user_book_ids = {book.id for book in user_books}
            
            # Universal library mode: OWNS relationships removed, so collaborative
            # filtering based on shared ownership is no longer applicable.
            # Placeholder: return an empty list (future enhancement: derive similarity
            # from tags/categories/genres + personal metadata reading history).
            return []
            
        except Exception as e:
            traceback.print_exc()
            return []
    
    async def search_by_filters(self, user_id: str, filters: Dict[str, Any], limit: int = 50) -> List[Book]:
        """Search books with advanced filters."""
        try:
            # Get all user books first
            user_books = await self.relationship_service.get_books_for_user(user_id, limit=10000)
            
            filtered_books = []
            
            for book in user_books:
                include_book = True
                
                # Apply filters
                if 'reading_status' in filters:
                    if getattr(book, 'reading_status', None) != filters['reading_status']:
                        include_book = False
                
                if 'language' in filters and include_book:
                    if getattr(book, 'language', None) != filters['language']:
                        include_book = False
                
                if 'year_published' in filters and include_book:
                    year_filter = filters['year_published']
                    book_year = None
                    if hasattr(book, 'published_date') and book.published_date:
                        try:
                            book_year = int(str(book.published_date)[:4])
                        except:
                            pass
                    
                    if isinstance(year_filter, dict):
                        if 'min' in year_filter and (not book_year or book_year < year_filter['min']):
                            include_book = False
                        if 'max' in year_filter and (not book_year or book_year > year_filter['max']):
                            include_book = False
                    elif isinstance(year_filter, int):
                        if book_year != year_filter:
                            include_book = False
                
                if 'has_rating' in filters and include_book:
                    has_rating = getattr(book, 'user_rating', None) is not None
                    if has_rating != filters['has_rating']:
                        include_book = False
                
                if include_book:
                    filtered_books.append(book)
            
            return filtered_books[:limit]
            
        except Exception as e:
            traceback.print_exc()
            return []
    
    # Sync wrappers for backward compatibility
    def search_books_sync(self, query: str, user_id: str, limit: int = 50) -> List[Book]:
        """Sync wrapper for search_books."""
        return run_async(self.search_books(query, user_id, limit))
    
    def search_books_global_sync(self, query: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Sync wrapper for search_books_global."""
        return run_async(self.search_books_global(query, limit))
    
    def get_book_by_isbn_for_user_sync(self, isbn: str, user_id: str) -> Optional[Book]:
        """Sync wrapper for get_book_by_isbn_for_user."""
        return run_async(self.get_book_by_isbn_for_user(isbn, user_id))
    
    def get_books_with_sharing_users_sync(self, days_back: int = 30, limit: int = 20) -> List[Book]:
        """Sync wrapper for get_books_with_sharing_users."""
        return run_async(self.get_books_with_sharing_users(days_back, limit))
    
    def get_currently_reading_shared_sync(self, limit: int = 20) -> List[Book]:
        """Sync wrapper for get_currently_reading_shared."""
        return run_async(self.get_currently_reading_shared(limit))
    
    def get_recommended_books_sync(self, user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Sync wrapper for get_recommended_books."""
        return run_async(self.get_recommended_books(user_id, limit))
    
    def search_by_filters_sync(self, user_id: str, filters: Dict[str, Any], limit: int = 50) -> List[Book]:
        """Sync wrapper for search_by_filters."""
        return run_async(self.search_by_filters(user_id, filters, limit))
