"""
Reading Log Service

Handles reading log operations for the Kuzu-based system.
Currently a basic implementation that returns empty results.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
import logging
import uuid

from app.infrastructure.kuzu_graph import get_graph_storage
from app.domain.models import ReadingLog

logger = logging.getLogger(__name__)


class KuzuReadingLogService:
    """Service for reading log operations using Kuzu."""
    
    def __init__(self):
        """Initialize the reading log service."""
        self.graph_storage = get_graph_storage()
    
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
    
    def get_user_reading_logs_sync(self, user_id: str, days_back: int = 30, limit: int = 100) -> List[Dict[str, Any]]:
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
            cutoff_date = (date.today() - timedelta(days=days_back)).isoformat()
            
            query = """
            MATCH (u:User {id: $user_id})-[:LOGGED]->(rl:ReadingLog)-[:FOR_BOOK]->(b:Book)
            WHERE rl.date >= $cutoff_date
            RETURN rl, b
            ORDER BY rl.date DESC, rl.created_at DESC
            LIMIT $limit
            """
            
            results = self.graph_storage.query(query, {
                "user_id": user_id,
                "cutoff_date": cutoff_date,
                "limit": limit
            })
            
            logs = []
            for result in results:
                if 'col_0' in result and 'col_1' in result:
                    log_data = result['col_0']
                    book_data = result['col_1']
                    
                    log_dict = dict(log_data)
                    log_dict['book'] = dict(book_data)
                    logs.append(log_dict)
            
            return logs
            
        except Exception as e:
            logger.error(f"Error getting user reading logs: {e}")
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
            
            # Check if log already exists for this user, book, and date
            existing_query = """
            MATCH (u:User {id: $user_id})-[:LOGGED]->(rl:ReadingLog)-[:FOR_BOOK]->(b:Book {id: $book_id})
            WHERE rl.date = $log_date
            RETURN rl
            """
            
            existing_results = self.graph_storage.query(existing_query, {
                "user_id": reading_log.user_id,
                "book_id": reading_log.book_id,
                "log_date": reading_log.date.isoformat()
            })
            
            if existing_results:
                # Update existing log instead of creating new one
                return self._update_existing_log(existing_results[0]['col_0'], reading_log)
            
            # Create new reading log node
            create_log_query = """
            CREATE (rl:ReadingLog {
                id: $log_id,
                date: $log_date,
                pages_read: $pages_read,
                minutes_read: $minutes_read,
                notes: $notes,
                created_at: $created_at
            })
            RETURN rl
            """
            
            log_result = self.graph_storage.query(create_log_query, {
                "log_id": log_id,
                "log_date": reading_log.date.isoformat(),
                "pages_read": reading_log.pages_read,
                "minutes_read": reading_log.minutes_read,
                "notes": reading_log.notes,
                "created_at": reading_log.created_at.isoformat()
            })
            
            if not log_result:
                logger.error("Failed to create reading log node")
                return None
            
            # Create relationships
            user_rel_query = """
            MATCH (u:User {id: $user_id}), (rl:ReadingLog {id: $log_id})
            CREATE (u)-[:LOGGED]->(rl)
            """
            
            book_rel_query = """
            MATCH (rl:ReadingLog {id: $log_id}), (b:Book {id: $book_id})
            CREATE (rl)-[:FOR_BOOK]->(b)
            """
            
            self.graph_storage.query(user_rel_query, {
                "user_id": reading_log.user_id,
                "log_id": log_id
            })
            
            self.graph_storage.query(book_rel_query, {
                "log_id": log_id,
                "book_id": reading_log.book_id
            })
            
            # Return the created log
            created_log = dict(log_result[0]['col_0'])
            created_log['id'] = log_id
            
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
            
            result = self.graph_storage.query(update_query, {
                "log_id": log_id,
                "pages_read": new_pages,
                "minutes_read": new_minutes,
                "notes": new_notes
            })
            
            if result:
                logger.info(f"Updated existing reading log {log_id}")
                return dict(result[0]['col_0'])
            
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
            
            verify_result = self.graph_storage.query(verify_query, {
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
            
            self.graph_storage.query(delete_query, {"log_id": log_id})
            
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
            cutoff_date = (date.today() - timedelta(days=days_back)).isoformat()
            
            stats_query = """
            MATCH (u:User {id: $user_id})-[:LOGGED]->(rl:ReadingLog)
            WHERE rl.date >= $cutoff_date
            RETURN 
                COUNT(rl) as total_sessions,
                SUM(rl.pages_read) as total_pages,
                SUM(rl.minutes_read) as total_minutes,
                COUNT(DISTINCT rl.date) as days_read
            """
            
            result = self.graph_storage.query(stats_query, {
                "user_id": user_id,
                "cutoff_date": cutoff_date
            })
            
            if result and 'col_0' in result[0]:
                stats = {
                    'total_sessions': result[0].get('col_0', 0),
                    'total_pages': result[0].get('col_1', 0),
                    'total_minutes': result[0].get('col_2', 0),
                    'days_read': result[0].get('col_3', 0),
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
            
            result = self.graph_storage.query(query, {
                "user_id": user_id,
                "book_id": book_id,
                "log_date": log_date.isoformat()
            })
            
            if result and 'col_0' in result[0]:
                return dict(result[0]['col_0'])
            
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
            query = """
            MATCH (u:User {id: $user_id})-[:LOGGED]->(rl:ReadingLog)-[:FOR_BOOK]->(b:Book)
            OPTIONAL MATCH (b)<-[:CONTRIBUTED_TO]-(c:Contribution)<-[:MADE]-(p:Person)
            WHERE c.contribution_type IN ['authored', 'co_authored']
            WITH b, MAX(rl.date) as latest_log_date, COLLECT(DISTINCT p.name) as authors
            RETURN DISTINCT b, latest_log_date, authors
            ORDER BY latest_log_date DESC
            LIMIT $limit
            """
            
            results = self.graph_storage.query(query, {
                "user_id": user_id,
                "limit": limit
            })
            
            books = []
            for result in results:
                if 'col_0' in result:
                    book_data = dict(result['col_0'])
                    book_data['latest_log_date'] = result.get('col_1')
                    book_data['authors'] = result.get('col_2', []) or []
                    books.append(book_data)
            
            return books
            
        except Exception as e:
            logger.error(f"Error getting recently read books: {e}")
            return []
