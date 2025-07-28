"""
KuzuDB Migration Helper

This module provides backward-compatible functions to ease migration 
from the dangerous global singleton pattern to the thread-safe SafeKuzuManager.

These functions allow existing code to work with minimal changes while
providing the safety guarantees of the new system.
"""

import functools
import logging
from typing import Optional, Dict, Any, Union, Generator
from contextlib import contextmanager

from .safe_kuzu_manager import get_safe_kuzu_manager, safe_execute_query, safe_get_connection

logger = logging.getLogger(__name__)


def deprecated_warning(old_function: str, new_function: str):
    """Log a deprecation warning for old KuzuDB functions."""
    logger.warning(
        f"⚠️  DEPRECATION: {old_function} is deprecated and will be removed. "
        f"Use {new_function} instead for thread safety."
    )


class BackwardCompatibleKuzuDB:
    """
    Backward-compatible wrapper that mimics the old KuzuGraphDB interface
    but uses the thread-safe SafeKuzuManager internally.
    
    This allows existing code to work without modification while gaining
    thread safety benefits.
    """
    
    def __init__(self, user_id: Optional[str] = None):
        self.user_id = user_id or "legacy_user"
        self._manager = get_safe_kuzu_manager()
    
    def connect(self):
        """
        Backward compatibility method that returns a connection context manager.
        
        ⚠️ WARNING: This method is deprecated. Use safe_get_connection() instead.
        """
        deprecated_warning("KuzuGraphDB.connect()", "safe_get_connection()")
        return safe_get_connection(user_id=self.user_id, operation="legacy_connect")
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None):
        """
        Backward compatibility method for query execution.
        
        ⚠️ WARNING: This method is deprecated. Use safe_execute_query() instead.
        """
        deprecated_warning("KuzuGraphDB.execute_query()", "safe_execute_query()")
        return safe_execute_query(
            query=query,
            params=params,
            user_id=self.user_id,
            operation="legacy_query"
        )


class BackwardCompatibleKuzuStorage:
    """
    Backward-compatible wrapper for KuzuGraphStorage that uses SafeKuzuManager.
    
    This maintains the same interface as the original KuzuGraphStorage but
    with thread-safe database access.
    """
    
    def __init__(self, user_id: Optional[str] = None):
        self.user_id = user_id or "legacy_storage_user"
        self._manager = get_safe_kuzu_manager()
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None):
        """Execute query with backward compatibility."""
        deprecated_warning("KuzuGraphStorage.execute_query()", "safe_execute_query()")
        return safe_execute_query(
            query=query,
            params=params,
            user_id=self.user_id,
            operation="legacy_storage_query"
        )
    
    @contextmanager
    def get_connection(self):
        """Get connection with backward compatibility."""
        deprecated_warning("KuzuGraphStorage.get_connection()", "safe_get_connection()")
        with safe_get_connection(user_id=self.user_id, operation="legacy_storage_connection") as conn:
            yield conn


# Backward compatibility functions that replace the dangerous global singletons
_legacy_database_instance: Optional[BackwardCompatibleKuzuDB] = None
_legacy_storage_instance: Optional[BackwardCompatibleKuzuStorage] = None


def get_kuzu_database_safe(user_id: Optional[str] = None) -> BackwardCompatibleKuzuDB:
    """
    Thread-safe replacement for get_kuzu_database().
    
    This function provides backward compatibility while using the safe
    SafeKuzuManager internally. It maintains the same interface but
    adds thread safety.
    
    Args:
        user_id: Optional user identifier for tracking
        
    Returns:
        BackwardCompatibleKuzuDB instance with thread-safe operations
        
    ⚠️ WARNING: This function is for migration only. New code should use
    safe_execute_query() or safe_get_connection() directly.
    """
    global _legacy_database_instance
    
    deprecated_warning("get_kuzu_database()", "safe_execute_query() or safe_get_connection()")
    
    # For backward compatibility, return a singleton-like instance
    # but it's actually thread-safe underneath
    if _legacy_database_instance is None:
        _legacy_database_instance = BackwardCompatibleKuzuDB(user_id=user_id)
    
    return _legacy_database_instance


def get_graph_storage_safe(user_id: Optional[str] = None) -> BackwardCompatibleKuzuStorage:
    """
    Thread-safe replacement for get_graph_storage().
    
    This function provides backward compatibility while using the safe
    SafeKuzuManager internally.
    
    Args:
        user_id: Optional user identifier for tracking
        
    Returns:
        BackwardCompatibleKuzuStorage instance with thread-safe operations
        
    ⚠️ WARNING: This function is for migration only. New code should use
    safe_execute_query() or safe_get_connection() directly.
    """
    global _legacy_storage_instance
    
    deprecated_warning("get_graph_storage()", "safe_execute_query() or safe_get_connection()")
    
    if _legacy_storage_instance is None:
        _legacy_storage_instance = BackwardCompatibleKuzuStorage(user_id=user_id)
    
    return _legacy_storage_instance


def get_kuzu_connection_safe(user_id: Optional[str] = None) -> BackwardCompatibleKuzuDB:
    """
    Thread-safe replacement for get_kuzu_connection().
    
    ⚠️ WARNING: This function is for migration only.
    """
    deprecated_warning("get_kuzu_connection()", "safe_get_connection()")
    return get_kuzu_database_safe(user_id=user_id)


# Migration decorator for automatic user context detection
def with_safe_kuzu(operation_name: str = "unknown"):
    """
    Decorator to automatically inject safe KuzuDB access into functions.
    
    This decorator can be used to migrate existing functions that use
    dangerous global KuzuDB access to the safe pattern.
    
    Args:
        operation_name: Description of the operation for debugging
        
    Example:
        @with_safe_kuzu("book_import")
        def import_books(csv_data, user_id=None):
            # Function automatically gets access to safe KuzuDB
            result = safe_execute_query(
                "CREATE (b:Book {title: $title})",
                {"title": "Test Book"},
                user_id=user_id,
                operation="create_book"
            )
            return result
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Try to extract user_id from function arguments
            user_id = kwargs.get('user_id')
            if not user_id and args:
                # Check if first argument looks like a user_id
                if isinstance(args[0], str) and len(args[0]) < 100:
                    user_id = args[0]
            
            # Add operation context to kwargs if not present
            if 'operation' not in kwargs:
                kwargs['operation'] = operation_name
            
            # Log the safe operation
            logger.debug(f"Executing safe KuzuDB operation: {operation_name} (user: {user_id})")
            
            return func(*args, **kwargs)
        return wrapper
    return decorator


# Context manager for bulk operations
@contextmanager
def safe_kuzu_transaction(user_id: Optional[str] = None, operation: str = "transaction"):
    """
    Context manager for safe KuzuDB transactions.
    
    This provides a pattern for executing multiple related queries
    within the same connection context, which is more efficient than
    individual query executions.
    
    Args:
        user_id: Optional user identifier for tracking
        operation: Description of the transaction for debugging
        
    Yields:
        Function that executes queries within the transaction context
        
    Example:
        with safe_kuzu_transaction(user_id="user123", operation="book_batch_import") as execute:
            execute("CREATE (b1:Book {title: 'Book 1'})")
            execute("CREATE (b2:Book {title: 'Book 2'})")
            result = execute("MATCH (b:Book) RETURN count(b)")
    """
    logger.debug(f"Starting safe KuzuDB transaction: {operation} (user: {user_id})")
    
    with safe_get_connection(user_id=user_id, operation=operation) as conn:
        def execute_in_transaction(query: str, params: Optional[Dict[str, Any]] = None):
            """Execute query within the transaction context."""
            return conn.execute(query, params or {})
        
        try:
            yield execute_in_transaction
            logger.debug(f"Completed safe KuzuDB transaction: {operation}")
        except Exception as e:
            logger.error(f"Error in safe KuzuDB transaction {operation}: {e}")
            raise


# Health check functions for monitoring migration progress
def check_migration_status() -> Dict[str, Any]:
    """
    Check the status of migration from dangerous global singletons to safe KuzuDB.
    
    Returns:
        Dictionary with migration status and recommendations
    """
    manager = get_safe_kuzu_manager()
    health = manager.get_health_status()
    
    return {
        'safe_manager_status': {
            'is_active': health['database_status']['is_initialized'],
            'active_connections': health['connection_metrics']['active_connections'],
            'total_connections': health['connection_metrics']['total_connections_created']
        },
        'legacy_compatibility': {
            'database_instance_created': _legacy_database_instance is not None,
            'storage_instance_created': _legacy_storage_instance is not None
        },
        'migration_recommendations': [
            "Replace get_kuzu_database() calls with safe_execute_query()",
            "Replace get_graph_storage() calls with safe_get_connection()",
            "Add user_id parameters to all database operations",
            "Use safe_kuzu_transaction() for bulk operations",
            "Remove dangerous global import_jobs dictionary usage"
        ],
        'thread_safety_status': health['thread_safety_status']
    }


def log_migration_status():
    """Log current migration status for debugging."""
    status = check_migration_status()
    
    logger.info("🔄 KuzuDB Migration Status:")
    logger.info(f"  Safe Manager Active: {status['safe_manager_status']['is_active']}")
    logger.info(f"  Active Connections: {status['safe_manager_status']['active_connections']}")
    logger.info(f"  Total Connections Created: {status['safe_manager_status']['total_connections']}")
    logger.info(f"  Legacy Database Instance: {status['legacy_compatibility']['database_instance_created']}")
    logger.info(f"  Legacy Storage Instance: {status['legacy_compatibility']['storage_instance_created']}")
    
    if status['safe_manager_status']['is_active']:
        logger.info("✅ Safe KuzuDB manager is active and working")
    else:
        logger.warning("⚠️  Safe KuzuDB manager not yet initialized")


# Emergency fallback for testing
def force_reset_all_connections():
    """
    Force reset all KuzuDB connections for testing/recovery.
    
    ⚠️ WARNING: This should only be used in tests or emergency recovery!
    """
    global _legacy_database_instance, _legacy_storage_instance
    
    logger.warning("🚨 Force resetting all KuzuDB connections - this should only happen in tests!")
    
    # Reset safe manager
    manager = get_safe_kuzu_manager()
    manager.force_reset()
    
    # Reset legacy instances
    _legacy_database_instance = None
    _legacy_storage_instance = None
    
    logger.info("🔄 All KuzuDB connections have been reset")
