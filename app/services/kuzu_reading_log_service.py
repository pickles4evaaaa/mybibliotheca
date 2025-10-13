"""
Reading Log Service

Handles reading log operations for the Kuzu-based system.
Currently a basic implementation that returns empty results.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta, timezone as dt_timezone
import logging
import uuid

from app.infrastructure.kuzu_graph import safe_execute_kuzu_query
from app.domain.models import ReadingLog

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
                    # Multi-column result - use col_0, col_1, etc. format for compatibility
                    row_dict = {}
                    for i, value in enumerate(row):
                        row_dict[f'col_{i}'] = value
                    rows.append(row_dict)
        
        return rows
    except Exception as e:
        print(f"Error converting query result to list: {e}")
        return []


def _extract_single_value(result: Any, index: int = 0) -> Any:
    """
    Helper function to safely extract a single value from KuzuDB QueryResult objects.
    
    Args:
        result: QueryResult object from KuzuDB
        index: Column index to extract (default: 0)
        
    Returns:
        The extracted value or None if not available
    """
    if not result:
        return None
        
    try:
        if hasattr(result, 'has_next') and result.has_next():
            row = result.get_next()
            if row and len(row) > index:
                return row[index]
    except Exception as e:
        logger.debug(f"Error extracting single value at index {index}: {e}")
        
    return None


def _extract_first_row(result: Any) -> Optional[List[Any]]:
    """
    Helper function to safely extract the first row from KuzuDB QueryResult objects.
    
    Args:
        result: QueryResult object from KuzuDB
        
    Returns:
        List of values from the first row, or None if not available
    """
    if not result:
        return None
        
    try:
        if hasattr(result, 'has_next') and result.has_next():
            return result.get_next()
    except Exception as e:
        logger.debug(f"Error extracting first row: {e}")
        
    return None


class KuzuReadingLogService:
    """Service for reading log operations using Kuzu."""
    
    def __init__(self):
        """Initialize the reading log service."""
        pass  # No longer need graph_storage instance
    
    def get_recent_shared_logs_sync(self, days_back: int = 7, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get recent shared reading logs.
        
        Args:
            days_back: Number of days to look back
            limit: Maximum number of logs to return
            
        Returns:
            List of reading log dictionaries (currently empty as feature is not implemented)
        """
        # TODO: Implement actual reading log retrieval from Kuzu
        logger.debug(f"Reading log system not fully implemented - returning empty list for recent shared logs")
        return []
    
    def get_user_reading_logs_sync(self, user_id: str, days_back: int = 30, limit: Optional[int] = 100) -> List[Dict[str, Any]]:
        """
        Get reading logs for a specific user.
        
        Args:
            user_id: The user ID
            days_back: Number of days to look back
            limit: Maximum number of logs to return
            
        Returns:
            List of reading log dictionaries
        """
        try:
            cutoff_date = date.today() - timedelta(days=days_back)  # Keep as date object
            
            # Query to get both book-specific and bookless reading logs
            limit_clause = ""
            query_params = {
                "user_id": user_id,
                "cutoff_date": cutoff_date
            }

            if limit is not None and limit > 0:
                limit_clause = "LIMIT $limit"
                query_params["limit"] = limit

            query = f"""
            MATCH (u:User {{id: $user_id}})-[:LOGGED]->(rl:ReadingLog)
            WHERE rl.date >= $cutoff_date
            OPTIONAL MATCH (rl)-[:FOR_BOOK]->(b:Book)
            RETURN rl, b
            ORDER BY rl.date DESC, rl.created_at DESC
            {limit_clause}
            """
            
            results = safe_execute_kuzu_query(query, query_params)
            results = _convert_query_result_to_list(results)
            
            logs = []
            for result in results:
                if 'col_0' in result:
                    log_data = result['col_0']
                    book_data = result.get('col_1')  # May be None for bookless logs
                    
                    log_dict = dict(log_data)
                    if book_data:
                        log_dict['book'] = dict(book_data)
                    else:
                        log_dict['book'] = None  # Explicitly set to None for bookless logs
                    logs.append(log_dict)
            
            return logs
            
        except Exception as e:
            logger.error(f"Error getting user reading logs: {e}")
            return []

    def get_user_reading_dates_sync(self, user_id: str, days_back: Optional[int] = None) -> List[date]:
        """Return unique reading-log dates for a user ordered from most recent to oldest."""
        try:
            params: Dict[str, Any] = {"user_id": user_id}
            date_filter = ""

            if days_back is not None:
                params["cutoff_date"] = date.today() - timedelta(days=days_back)
                date_filter = "WHERE rl.date >= $cutoff_date"

            query = f"""
            MATCH (u:User {{id: $user_id}})-[:LOGGED]->(rl:ReadingLog)
            {date_filter}
            RETURN DISTINCT rl.date
            ORDER BY rl.date DESC
            """

            results = safe_execute_kuzu_query(query, params)
            rows = _convert_query_result_to_list(results)

            reading_dates: List[date] = []
            for row in rows:
                raw_value = row.get('col_0') or row.get('result')
                if raw_value is None and row:
                    try:
                        raw_value = next(iter(row.values()))
                    except StopIteration:
                        raw_value = None
                if raw_value is None:
                    continue
                if isinstance(raw_value, date):
                    reading_dates.append(raw_value)
                elif isinstance(raw_value, datetime):
                    reading_dates.append(raw_value.date())
                elif isinstance(raw_value, str):
                    try:
                        reading_dates.append(date.fromisoformat(raw_value.split('T')[0]))
                    except ValueError:
                        continue

            return reading_dates

        except Exception as e:
            logger.error(f"Error getting reading dates for user {user_id}: {e}")
            return []
    
    def create_reading_log_sync(self, reading_log: ReadingLog) -> Optional[Dict[str, Any]]:
        """
        Create a new reading log entry.
        
        Args:
            reading_log: The ReadingLog domain object
            
        Returns:
            The created reading log dictionary or None if creation failed
        """
        try:
            # Generate ID if not provided
            log_id = reading_log.id or str(uuid.uuid4())
            
            # Check if book_id is provided for book-specific logs
            if reading_log.book_id:
                # Check if log already exists for this user, book, and date
                existing_query = """
                MATCH (u:User {id: $user_id})-[:LOGGED]->(rl:ReadingLog)-[:FOR_BOOK]->(b:Book {id: $book_id})
                WHERE rl.date = $log_date
                RETURN rl
                """
                
                existing_results = safe_execute_kuzu_query(existing_query, {
                    'user_id': reading_log.user_id,
                    'book_id': reading_log.book_id,
                    'log_date': reading_log.date if reading_log.date else None
                })
                existing_list = _convert_query_result_to_list(existing_results)
                
                if existing_list:
                    # Update existing reading log
                    existing = existing_list[0].get('result') or existing_list[0].get('col_0') or next(iter(existing_list[0].values()), None)
                    if existing is not None:
                        return self._update_existing_log(existing, reading_log)
            else:
                # For bookless logs, check if a general log exists for this user and date
                existing_query = """
                MATCH (u:User {id: $user_id})-[:LOGGED]->(rl:ReadingLog)
                WHERE rl.date = $log_date AND NOT (rl)-[:FOR_BOOK]->()
                RETURN rl
                """
                
                existing_results = safe_execute_kuzu_query(existing_query, {
                    'user_id': reading_log.user_id,
                    'log_date': reading_log.date if reading_log.date else None
                })
                existing_list = _convert_query_result_to_list(existing_results)
                
                if existing_list:
                    # Update existing bookless reading log
                    existing = existing_list[0].get('result') or existing_list[0].get('col_0') or next(iter(existing_list[0].values()), None)
                    if existing is not None:
                        return self._update_existing_log(existing, reading_log)
            
            # Create new reading log node
            create_log_query = """
            CREATE (rl:ReadingLog {
                id: $log_id,
                date: $log_date,
                pages_read: $pages_read,
                minutes_read: $minutes_read,
                notes: $notes,
                created_at: $created_at,
                updated_at: $updated_at
            })
            RETURN rl
            """
            
            log_result = safe_execute_kuzu_query(create_log_query, {
                "log_id": log_id,
                "log_date": reading_log.date,  # Pass date object directly
                "pages_read": reading_log.pages_read,
                "minutes_read": reading_log.minutes_read,
                "notes": reading_log.notes,
                "created_at": reading_log.created_at,  # Pass datetime object directly
                "updated_at": reading_log.updated_at or reading_log.created_at
            })
            
            if not log_result:
                logger.error("Failed to create reading log node")
                return None
            
            # Debug: Check the structure of log_result
            logger.debug(f"log_result structure: {log_result}")
            logger.debug(f"log_result type: {type(log_result)}")
            log_list = _convert_query_result_to_list(log_result)
            if log_list:
                logger.debug(f"log_list[0]: {log_list[0]}")
                logger.debug(f"log_list[0] keys: {log_list[0].keys() if hasattr(log_list[0], 'keys') else 'No keys method'}")
            
            # Create user relationship (always needed)
            user_rel_query = """
            MATCH (u:User {id: $user_id}), (rl:ReadingLog {id: $log_id})
            CREATE (u)-[:LOGGED]->(rl)
            """
            
            safe_execute_kuzu_query(user_rel_query, {
                "user_id": reading_log.user_id,
                "log_id": log_id
            })
            
            # Create book relationship only if book_id is provided
            if reading_log.book_id:
                book_rel_query = """
                MATCH (rl:ReadingLog {id: $log_id}), (b:Book {id: $book_id})
                CREATE (rl)-[:FOR_BOOK]->(b)
                """
                
                safe_execute_kuzu_query(book_rel_query, {
                    "log_id": log_id,
                    "book_id": reading_log.book_id
                })

                # Auto-set start_date in personal metadata if empty
                try:
                    from .personal_metadata_service import personal_metadata_service
                    if reading_log.date:
                        start_dt = datetime(
                            reading_log.date.year,
                            reading_log.date.month,
                            reading_log.date.day,
                            tzinfo=dt_timezone.utc
                        )
                    else:
                        start_dt = (reading_log.created_at or datetime.now(dt_timezone.utc))
                    personal_metadata_service.ensure_start_date(reading_log.user_id, reading_log.book_id, start_dt)
                except Exception as e:
                    logger.warning(f"Auto-set personal start_date skipped: {e}")
            
            # Return a simple success response instead of trying to access col_0
            created_log = {
                'id': log_id,
                'user_id': reading_log.user_id,
                'book_id': reading_log.book_id,
                'date': reading_log.date.isoformat() if reading_log.date else None,
                'pages_read': reading_log.pages_read,
                'minutes_read': reading_log.minutes_read,
                'notes': reading_log.notes,
                'created_at': reading_log.created_at.isoformat() if reading_log.created_at else None,
                'updated_at': (reading_log.updated_at or reading_log.created_at).isoformat() if reading_log.updated_at or reading_log.created_at else None
            }
            
            logger.info(f"Successfully created reading log {log_id} for user {reading_log.user_id}")
            return created_log
            
        except Exception as e:
            logger.error(f"Error creating reading log: {e}")
            return None
    
    def _update_existing_log(self, existing_log_data: Dict[str, Any], new_log: ReadingLog) -> Optional[Dict[str, Any]]:
        """Update an existing reading log by adding to the values."""
        try:
            existing_log = dict(existing_log_data)
            log_id = existing_log['id']
            
            # Add pages and minutes to existing values
            new_pages = existing_log.get('pages_read', 0) + new_log.pages_read
            new_minutes = existing_log.get('minutes_read', 0) + new_log.minutes_read
            
            # Combine notes
            existing_notes = existing_log.get('notes', '')
            if existing_notes and new_log.notes:
                new_notes = f"{existing_notes}\n\n{new_log.notes}"
            elif new_log.notes:
                new_notes = new_log.notes
            else:
                new_notes = existing_notes
            
            update_query = """
            MATCH (rl:ReadingLog {id: $log_id})
            SET rl.pages_read = $pages_read,
                rl.minutes_read = $minutes_read,
                rl.notes = $notes
            RETURN rl
            """
            
            result = safe_execute_kuzu_query(update_query, {
                "log_id": log_id,
                "pages_read": new_pages,
                "minutes_read": new_minutes,
                "notes": new_notes
            })
            
            if result:
                logger.info(f"Updated existing reading log {log_id}")
                result_list = _convert_query_result_to_list(result)
                if result_list:
                    node = result_list[0].get('result') or result_list[0].get('col_0') or next(iter(result_list[0].values()), None)
                    if node is not None:
                        return dict(node)
                
            return None
            
        except Exception as e:
            logger.error(f"Error updating existing reading log: {e}")
            return None
    
    def delete_reading_log_sync(self, log_id: str, user_id: str) -> bool:
        """
        Delete a reading log entry.
        
        Args:
            log_id: The reading log ID
            user_id: The user ID (for security check)
            
        Returns:
            True if deleted successfully
        """
        try:
            # Verify the log belongs to the user before deleting
            verify_query = """
            MATCH (u:User {id: $user_id})-[:LOGGED]->(rl:ReadingLog {id: $log_id})
            RETURN rl
            """
            
            verify_result = safe_execute_kuzu_query(verify_query, {
                "user_id": user_id,
                "log_id": log_id
            })
            
            if not verify_result:
                logger.warning(f"Reading log {log_id} not found for user {user_id}")
                return False
            
            # Delete the reading log and all its relationships
            delete_query = """
            MATCH (rl:ReadingLog {id: $log_id})
            DETACH DELETE rl
            """
            
            safe_execute_kuzu_query(delete_query, {"log_id": log_id})
            
            logger.info(f"Deleted reading log {log_id} for user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting reading log {log_id}: {e}")
            return False
    
    def get_user_reading_stats_sync(self, user_id: str, days_back: int = 30) -> Dict[str, Any]:
        """
        Get reading statistics for a user.
        
        Args:
            user_id: The user ID
            days_back: Number of days to look back
            
        Returns:
            Dictionary with reading statistics
        """
        try:
            cutoff_date = date.today() - timedelta(days=days_back)  # Keep as date object
            
            stats_query = """
            MATCH (u:User {id: $user_id})-[:LOGGED]->(rl:ReadingLog)
            WHERE rl.date >= $cutoff_date
            RETURN 
                COUNT(rl) as total_sessions,
                SUM(rl.pages_read) as total_pages,
                SUM(rl.minutes_read) as total_minutes,
                COUNT(DISTINCT rl.date) as days_read
            """
            
            result = safe_execute_kuzu_query(stats_query, {
                "user_id": user_id,
                "cutoff_date": cutoff_date
            })
            
            result_list = _convert_query_result_to_list(result)
            if result_list:
                row = result_list[0]
                stats = {
                    'total_sessions': row.get('col_0', 0),
                    'total_pages': row.get('col_1', 0),
                    'total_minutes': row.get('col_2', 0),
                    'days_read': row.get('col_3', 0),
                    'days_back': days_back
                }
                
                # Calculate averages
                if stats['days_read'] > 0:
                    stats['avg_pages_per_day'] = stats['total_pages'] / stats['days_read']
                    stats['avg_minutes_per_day'] = stats['total_minutes'] / stats['days_read']
                else:
                    stats['avg_pages_per_day'] = 0
                    stats['avg_minutes_per_day'] = 0
                
                return stats
            
            return {
                'total_sessions': 0,
                'total_pages': 0,
                'total_minutes': 0,
                'days_read': 0,
                'avg_pages_per_day': 0,
                'avg_minutes_per_day': 0,
                'days_back': days_back
            }
            
        except Exception as e:
            logger.error(f"Error getting reading stats for user {user_id}: {e}")
            return {
                'total_sessions': 0,
                'total_pages': 0,
                'total_minutes': 0,
                'days_read': 0,
                'avg_pages_per_day': 0,
                'avg_minutes_per_day': 0,
                'days_back': days_back
            }
    
    def get_existing_log_sync(self, book_id: str, user_id: str, log_date: date) -> Optional[Dict[str, Any]]:
        """
        Check if a reading log already exists for a user, book, and date.
        
        Args:
            book_id: The book ID
            user_id: The user ID
            log_date: The date to check
            
        Returns:
            The existing log dictionary or None
        """
        try:
            query = """
            MATCH (u:User {id: $user_id})-[:LOGGED]->(rl:ReadingLog)-[:FOR_BOOK]->(b:Book {id: $book_id})
            WHERE rl.date = $log_date
            RETURN rl
            """
            
            result = safe_execute_kuzu_query(query, {
                "user_id": user_id,
                "book_id": book_id,
                "log_date": log_date  # Pass date object directly
            })
            
            result_list = _convert_query_result_to_list(result)
            if result_list:
                node = result_list[0].get('result') or result_list[0].get('col_0') or next(iter(result_list[0].values()), None)
                if node is not None:
                    return dict(node)
            
            return None
            
        except Exception as e:
            logger.error(f"Error checking existing log: {e}")
            return None
    
    def get_recently_read_books_sync(self, user_id: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Get books that the user has recently logged reading sessions for.
        
        Args:
            user_id: The user ID
            limit: Maximum number of books to return
            
        Returns:
            List of book dictionaries with reading log info
        """
        try:
            # Simplified query without Contribution table since it doesn't exist yet
            query = """
            MATCH (u:User {id: $user_id})-[:LOGGED]->(rl:ReadingLog)-[:FOR_BOOK]->(b:Book)
            WITH b, MAX(rl.date) as latest_log_date
            RETURN DISTINCT b, latest_log_date
            ORDER BY latest_log_date DESC
            LIMIT $limit
            """
            
            results = safe_execute_kuzu_query(query, {
                "user_id": user_id,
                "limit": limit
            })
            
            results_list = _convert_query_result_to_list(results)
            
            books = []
            for result in results_list:
                if 'col_0' in result:
                    book_data = dict(result['col_0'])
                    book_data['latest_log_date'] = result.get('col_1')
                    book_data['authors'] = []  # Will be populated by other means
                    books.append(book_data)
            
            return books
            
        except Exception as e:
            logger.error(f"Error getting recently read books: {e}")
            return []

    def get_user_all_time_reading_stats_sync(self, user_id: str) -> Dict[str, Any]:
        """
        Get all-time reading statistics for a user.
        
        Args:
            user_id: The user ID
            
        Returns:
            Dictionary with all-time reading statistics
        """
        try:
            # Use the same working query pattern as the paginated method to get ALL reading logs
            # This ensures we're counting from the same data source that displays correctly
            all_logs_query = """
            MATCH (u:User {id: $user_id})-[:LOGGED]->(rl:ReadingLog)
            OPTIONAL MATCH (rl)-[:FOR_BOOK]->(b:Book)
            RETURN rl, b
            """
            
            logs_result = safe_execute_kuzu_query(all_logs_query, {
                "user_id": user_id
            })
            
            # Process the results manually to count everything
            logs_list = _convert_query_result_to_list(logs_result)
            if logs_list:
                total_log_entries = len(logs_list)
                total_pages = 0
                total_minutes = 0
                distinct_books = set()
                distinct_days = set()

                def _normalize_book_id(payload: Dict[str, Any]) -> Optional[str]:
                    if not payload:
                        return None
                    candidate = payload.get('id') or payload.get('uid') or payload.get('book_id')
                    return str(candidate) if candidate is not None else None

                def _parse_log_date(raw_value: Any) -> Optional[date]:
                    if raw_value is None:
                        return None
                    if isinstance(raw_value, datetime):
                        return raw_value.date()
                    if isinstance(raw_value, date):
                        return raw_value
                    if isinstance(raw_value, str):
                        cleaned = raw_value.strip()
                        if not cleaned:
                            return None
                        cleaned = cleaned.replace('Z', '+00:00')
                        try:
                            return datetime.fromisoformat(cleaned).date()
                        except ValueError:
                            try:
                                return date.fromisoformat(cleaned.split('T')[0])
                            except ValueError:
                                return None
                    return None

                for result in logs_list:
                    raw_log = result.get('col_0') or result.get('result') or result.get('rl')
                    if not raw_log:
                        continue
                    try:
                        log_data = dict(raw_log)
                    except (TypeError, ValueError):
                        continue

                    # Count pages and minutes (with safe defaults)
                    pages = log_data.get('pages_read') or 0
                    minutes = log_data.get('minutes_read') or 0
                    try:
                        total_pages += int(pages)
                    except (TypeError, ValueError):
                        pass
                    try:
                        total_minutes += int(minutes)
                    except (TypeError, ValueError):
                        pass

                    # Resolve book data, preferring query column but falling back to embedded payload
                    raw_book = result.get('col_1')
                    book_payload: Dict[str, Any] = {}
                    if raw_book:
                        try:
                            book_payload = dict(raw_book)
                        except (TypeError, ValueError):
                            book_payload = {}
                    if not book_payload and isinstance(log_data.get('book'), dict):
                        book_payload = log_data.get('book')  # type: ignore[assignment]

                    book_id = _normalize_book_id(book_payload)
                    if book_id:
                        distinct_books.add(book_id)
                    elif log_data.get('book_id'):
                        distinct_books.add(str(log_data.get('book_id')))

                    # Track distinct days
                    log_date_value = log_data.get('date') or log_data.get('log_date')
                    parsed_date = _parse_log_date(log_date_value)
                    if parsed_date:
                        distinct_days.add(parsed_date.isoformat())
                
                stats = {
                    'total_log_entries': total_log_entries,
                    'total_pages': total_pages,
                    'total_minutes': total_minutes,
                    'distinct_books': len(distinct_books),
                    'distinct_days': len(distinct_days),
                    'total_time_formatted': '0m'
                }
                
                # Calculate total time in more readable format
                total_minutes_val = stats['total_minutes']
                if total_minutes_val and total_minutes_val > 0:
                    days = total_minutes_val // (24 * 60)
                    hours = (total_minutes_val % (24 * 60)) // 60
                    minutes = total_minutes_val % 60
                    stats['total_time_formatted'] = f"{days}d {hours}h {minutes}m"
                
                return stats
            
            return {
                'total_log_entries': 0,
                'total_pages': 0,
                'total_minutes': 0,
                'distinct_books': 0,
                'distinct_days': 0,
                'total_time_formatted': '0m'
            }
            
        except Exception as e:
            logger.error(f"Error getting all-time reading stats for user {user_id}: {e}")
            return {
                'total_log_entries': 0,
                'total_pages': 0,
                'total_minutes': 0,
                'distinct_books': 0,
                'distinct_days': 0,
                'total_time_formatted': '0m'
            }

    def get_user_reading_logs_paginated_sync(self, user_id: str, page: int = 1, per_page: int = 25) -> Dict[str, Any]:
        """
        Get paginated reading logs for a user.
        
        Args:
            user_id: The user ID
            page: Page number (1-based)
            per_page: Number of logs per page
            
        Returns:
            Dictionary with logs, pagination info, and totals
        """
        try:
            # First get total count
            count_query = """
            MATCH (u:User {id: $user_id})-[:LOGGED]->(rl:ReadingLog)
            RETURN COUNT(rl) as total_count
            """
            
            count_result = safe_execute_kuzu_query(count_query, {"user_id": user_id})
            count_list = _convert_query_result_to_list(count_result)
            total_count = count_list[0].get('col_0', 0) if count_list else 0
            
            # Calculate offset
            offset = (page - 1) * per_page
            
            # Get paginated logs with optional book details
            logs_query = """
            MATCH (u:User {id: $user_id})-[:LOGGED]->(rl:ReadingLog)
            OPTIONAL MATCH (rl)-[:FOR_BOOK]->(b:Book)
            RETURN rl, b
            ORDER BY rl.date DESC, rl.created_at DESC
            SKIP $offset
            LIMIT $limit
            """
            
            logs_result = safe_execute_kuzu_query(logs_query, {
                "user_id": user_id,
                "offset": offset,
                "limit": per_page
            })
            
            logs_list = _convert_query_result_to_list(logs_result)
            logs = []
            for result in logs_list:
                if 'col_0' in result:
                    log_data = dict(result['col_0'])
                    book_data = result.get('col_1')
                    
                    if book_data:
                        log_data['book'] = dict(book_data)
                    else:
                        log_data['book'] = None  # Bookless log
                    logs.append(log_data)
            
            # Calculate pagination info
            total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 1
            has_prev = page > 1
            has_next = page < total_pages
            
            return {
                'logs': logs,
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total_count': total_count,
                    'total_pages': total_pages,
                    'has_prev': has_prev,
                    'has_next': has_next
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting paginated reading logs for user {user_id}: {e}")
            return {
                'logs': [],
                'pagination': {
                    'page': 1,
                    'per_page': per_page,
                    'total_count': 0,
                    'total_pages': 1,
                    'has_prev': False,
                    'has_next': False
                }
            }

    def update_reading_log_sync(self, log_id: str, reading_log: ReadingLog) -> Optional[Dict[str, Any]]:
        """
        Update an existing reading log entry.
        
        Args:
            log_id: The ID of the reading log to update
            reading_log: The updated ReadingLog domain object
            
        Returns:
            The updated reading log dictionary or None if update failed
        """
        try:
            # First verify the log exists and belongs to the user
            verify_query = """
            MATCH (u:User {id: $user_id})-[:LOGGED]->(rl:ReadingLog {id: $log_id})
            RETURN rl
            """
            
            verify_result = safe_execute_kuzu_query(verify_query, {
                'user_id': reading_log.user_id,
                'log_id': log_id
            })
            
            if not verify_result:
                logger.error(f"Reading log {log_id} not found for user {reading_log.user_id}")
                return None
            
            # Update the reading log
            update_query = """
            MATCH (rl:ReadingLog {id: $log_id})
            SET rl.date = $log_date,
                rl.pages_read = $pages_read,
                rl.minutes_read = $minutes_read,
                rl.notes = $notes,
                rl.updated_at = $updated_at
            RETURN rl
            """
            
            result = safe_execute_kuzu_query(update_query, {
                "log_id": log_id,
                "log_date": reading_log.date,
                "pages_read": reading_log.pages_read,
                "minutes_read": reading_log.minutes_read,
                "notes": reading_log.notes,
                "updated_at": reading_log.updated_at or datetime.now(dt_timezone.utc)
            })
            
            if result:
                logger.info(f"Updated reading log {log_id}")
                
                # Return updated log data
                updated_log = {
                    'id': log_id,
                    'user_id': reading_log.user_id,
                    'book_id': reading_log.book_id,
                    'date': reading_log.date.isoformat() if reading_log.date else None,
                    'pages_read': reading_log.pages_read,
                    'minutes_read': reading_log.minutes_read,
                    'notes': reading_log.notes,
                    'created_at': reading_log.created_at.isoformat() if reading_log.created_at else None,
                    'updated_at': (reading_log.updated_at or datetime.now(dt_timezone.utc)).isoformat()
                }
                
                return updated_log
                
            return None
            
        except Exception as e:
            logger.error(f"Error updating reading log {log_id}: {e}")
            return None

    def get_reading_log_by_id_sync(self, log_id: str, user_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific reading log by ID.
        
        Args:
            log_id: The ID of the reading log
            user_id: The user ID to verify ownership
            
        Returns:
            The reading log dictionary or None if not found
        """
        try:
            query = """
            MATCH (u:User {id: $user_id})-[:LOGGED]->(rl:ReadingLog {id: $log_id})
            OPTIONAL MATCH (rl)-[:FOR_BOOK]->(b:Book)
            RETURN rl, b
            """
            
            result = safe_execute_kuzu_query(query, {
                'user_id': user_id,
                'log_id': log_id
            })
            
            if result:
                result_list = _convert_query_result_to_list(result)
                if result_list:
                    log_data = dict(result_list[0]['col_0'])
                    book_data = dict(result_list[0]['col_1']) if result_list[0]['col_1'] else None
                    
                    if book_data:
                        log_data['book'] = book_data
                    
                    return log_data
                    
            return None
            
        except Exception as e:
            logger.error(f"Error getting reading log {log_id}: {e}")
            return None
