"""
Connection Recovery Utilities

This module provides utilities for detecting and recovering from database connection issues,
particularly after backup/restore operations.
"""

import logging
from typing import Optional
from app.utils.safe_kuzu_manager import SafeKuzuManager, get_safe_kuzu_manager

logger = logging.getLogger(__name__)


def _convert_query_result_to_list(result):
    """Convert SafeKuzuManager query result to legacy list format"""
    if hasattr(result, 'get_as_df'):
        return result.get_as_df().to_dict('records')
    elif hasattr(result, 'get_next'):
        rows = []
        while result.has_next():
            rows.append(result.get_next())
        return rows
    else:
        return list(result) if result else []


def handle_connection_error(error_message: str) -> bool:
    """
    Handle database connection errors by attempting to refresh connections.
    
    Args:
        error_message: The error message that was encountered
        
    Returns:
        True if connection was successfully refreshed, False otherwise
    """
    try:
        # Check if this is a connection-related error
        connection_error_indicators = [
            "connection is closed",
            "connection closed",
            "database is closed",
            "no connection",
            "connection timeout",
            "connection lost"
        ]
        
        error_lower = error_message.lower()
        is_connection_error = any(indicator in error_lower for indicator in connection_error_indicators)
        
        if not is_connection_error:
            return False
            
        logger.warning(f"🔌 Connection error detected: {error_message}")
        logger.info("🔄 Attempting to recover database connections...")
        
        # Import here to avoid circular imports
        from app.services.simple_backup_service import SimpleBackupService
        backup_service = SimpleBackupService()
        
        # Simple connection refresh by getting fresh database instance
        try:
            safe_manager = get_safe_kuzu_manager()
            # Test connection by running a simple query
            test_result = safe_manager.execute_query("RETURN 1 as test")
            success = test_result is not None
        except Exception:
            success = False
        
        if success:
            logger.info("✅ Connection recovery successful")
        else:
            logger.error("❌ Connection recovery failed")
            
        return success
        
    except Exception as e:
        logger.error(f"Failed to handle connection error: {e}")
        return False


def with_connection_recovery(func):
    """
    Decorator that automatically attempts connection recovery if a database operation fails
    with a connection error.
    
    Usage:
        @with_connection_recovery
        def my_database_operation():
            # database operation that might fail due to connection issues
            pass
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            error_message = str(e)
            
            # Try to recover from connection error
            if handle_connection_error(error_message):
                # Retry the operation once after recovery
                try:
                    logger.info("🔄 Retrying operation after connection recovery...")
                    return func(*args, **kwargs)
                except Exception as retry_error:
                    logger.error(f"Operation failed even after connection recovery: {retry_error}")
                    raise retry_error
            else:
                # Not a connection error or recovery failed, re-raise original exception
                raise e
                
    return wrapper


def check_and_refresh_connections() -> bool:
    """
    Manually check and refresh database connections if needed.
    
    Returns:
        True if connections are healthy or were successfully refreshed, False otherwise
    """
    try:
        safe_manager = get_safe_kuzu_manager()
        
        # Check if database connection is working
        try:
            test_result = safe_manager.execute_query("RETURN 1 as test")
            health_status = 'healthy' if test_result is not None else 'unhealthy'
        except Exception:
            health_status = 'unhealthy'
        
        if health_status == 'healthy':
            logger.debug("Database connections are healthy")
            return True
        else:
            logger.warning("Database connections are unhealthy, attempting refresh...")
            # Simple refresh by creating a new SafeKuzuManager instance
            try:
                safe_manager = get_safe_kuzu_manager()
                test_result = safe_manager.execute_query("RETURN 1 as test")
                return test_result is not None
            except Exception:
                return False
            
    except Exception as e:
        logger.error(f"Failed to check and refresh connections: {e}")
        return False
