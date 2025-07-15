"""
Connection Recovery Utilities

This module provides utilities for detecting and recovering from database connection issues,
particularly after backup/restore operations.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


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
        from app.services.backup_restore_service import get_backup_service
        backup_service = get_backup_service()
        
        # Attempt emergency connection refresh
        success = backup_service.emergency_connection_refresh()
        
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
        from app.services.backup_restore_service import get_backup_service
        backup_service = get_backup_service()
        
        # Check connection health
        health = backup_service.check_connection_health()
        
        if health.get('overall_health') == 'healthy':
            logger.debug("Database connections are healthy")
            return True
        else:
            logger.warning("Database connections are unhealthy, attempting refresh...")
            return backup_service.emergency_connection_refresh()
            
    except Exception as e:
        logger.error(f"Failed to check and refresh connections: {e}")
        return False
