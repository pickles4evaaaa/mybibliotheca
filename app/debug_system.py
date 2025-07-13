"""
Debug system for bibliotheca application.
Provides admin-controlled debugging with logging and UI components.
"""

import os
import logging
from typing import Optional, Dict, Any, List
from functools import wraps
from flask import session, current_app, request
from flask_login import current_user
import json
from datetime import datetime


class DebugManager:
    """Manages debug mode state and logging."""
    
    def __init__(self):
        self.logger = logging.getLogger('bibliotheca.debug')
        
        # Set up debug logger with specific formatting
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '🐛 [%(asctime)s] %(levelname)s - %(name)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.DEBUG)
    
    def is_debug_enabled(self) -> bool:
        """Check if debug mode is enabled globally."""
        try:
            # Check MYBIBLIOTHECA_DEBUG environment variable (default to false for performance)
            debug_state = os.getenv('MYBIBLIOTHECA_DEBUG', 'false')
            return debug_state.lower() in ['true', 'on', '1']
        except Exception as e:
            self.logger.error(f"Failed to check debug status: {e}")
            return False  # Return False on error to prevent performance issues
    
    def is_user_admin(self, user=None) -> bool:
        """Check if the current user is an admin."""
        try:
            if user is None:
                try:
                    user = current_user
                except Exception:
                    return False
            
            if not user or not user.is_authenticated:
                return False
            
            # Check if user has admin role or is marked as admin
            return getattr(user, 'is_admin', False) or getattr(user, 'role', '') == 'admin'
        except Exception:
            return False
    
    def should_show_debug(self, user=None) -> bool:
        """Determine if debug info should be shown to the current user."""
        try:
            # Only show debug info if debug is enabled AND user is admin
            return self.is_debug_enabled() and self.is_user_admin(user)
        except Exception as e:
            self.logger.error(f"Error in should_show_debug: {e}")
            return False  # Return False on error to prevent performance issues
    
    def enable_debug_mode(self, user_id: str) -> bool:
        """Enable debug mode (admin only)."""
        try:
            if not self.is_user_admin():
                return False
            
            # For Kuzu version, we'll use environment variable control
            self.log_debug(f"Debug mode controlled via MYBIBLIOTHECA_DEBUG environment variable")
            self.log_debug(f"Debug mode enable requested by user {user_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to enable debug mode: {e}")
            return False
    
    def disable_debug_mode(self, user_id: str) -> bool:
        """Disable debug mode (admin only)."""
        try:
            if not self.is_user_admin():
                return False
            
            # For Kuzu version, we'll use environment variable control
            self.log_debug(f"Debug mode controlled via MYBIBLIOTHECA_DEBUG environment variable")
            self.log_debug(f"Debug mode disable requested by user {user_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to disable debug mode: {e}")
            return False
    
    def get_debug_status(self) -> Dict[str, Any]:
        """Get current debug mode status and metadata."""
        try:
            status = {
                'enabled': self.is_debug_enabled(),
                'enabled_by': os.getenv('DEBUG_ENABLED_BY'),
                'enabled_at': os.getenv('DEBUG_ENABLED_AT'),
                'disabled_by': os.getenv('DEBUG_DISABLED_BY'),
                'disabled_at': os.getenv('DEBUG_DISABLED_AT')
            }
            
            return status
        except Exception as e:
            self.logger.error(f"Failed to get debug status: {e}")
            return {'enabled': False}
    
    def log_debug(self, message: str, category: str = "GENERAL", extra_data: Optional[Dict[str, Any]] = None):
        """Log debug message with category and optional extra data."""
        if not self.is_debug_enabled():
            return
        
        # Safely get user and request info
        user_id = 'anonymous'
        request_path = None
        request_method = None
        
        try:
            if current_user and current_user.is_authenticated:
                user_id = str(getattr(current_user, 'id', 'anonymous'))
        except Exception:
            pass
        
        try:
            if request:
                request_path = request.path
                request_method = request.method
        except Exception:
            pass
        
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'category': category,
            'message': message,
            'extra_data': extra_data or {},
            'user_id': user_id,
            'request_path': request_path,
            'request_method': request_method
        }
        
        # Log to container logs
        self.logger.debug(f"[{category}] {message} | Extra: {extra_data}")
        
        # For Docker deployment, rely on container logging instead of Redis storage
        # Debug logs can be viewed through Docker logs or logging system
    
    def get_debug_logs(self, date: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get debug logs for a specific date (admin only)."""
        if not self.should_show_debug():
            return []
        
        try:
            # For Docker deployment, debug logs are available through container logs
            # Return a placeholder message directing to container logs
            return [{
                'timestamp': datetime.utcnow().isoformat(),
                'category': 'SYSTEM',
                'message': 'Debug logs are available through Docker container logs. Use: docker logs <container_name>',
                'extra_data': {},
                'user_id': 'system',
                'request_path': None,
                'request_method': None
            }]
        except Exception as e:
            self.logger.error(f"Failed to get debug logs: {e}")
            return []


# Global debug manager instance
debug_manager = None


def get_debug_manager() -> DebugManager:
    """Get the global debug manager instance."""
    global debug_manager
    if debug_manager is None:
        try:
            # Use simplified debug manager for Kuzu version
            debug_manager = DebugManager()
        except Exception as e:
            debug_manager = DebugManager()
    return debug_manager


def debug_log(message: str, category: str = "GENERAL", extra_data: Optional[Dict[str, Any]] = None):
    """Convenience function for debug logging."""
    get_debug_manager().log_debug(message, category, extra_data)


def debug_enabled():
    """Decorator to only execute function if debug is enabled."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if get_debug_manager().is_debug_enabled():
                return func(*args, **kwargs)
            return None
        return wrapper
    return decorator


def admin_required_debug():
    """Decorator for debug functions that require admin access."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if get_debug_manager().should_show_debug():
                return func(*args, **kwargs)
            return None
        return wrapper
    return decorator


# Enhanced debugging functions for specific areas
def debug_book_details(book_data, uid, user_id, operation="VIEW"):
    """Enhanced debugging for book details page."""
    debug_log(f"🔍 [{operation}] Starting book details for UID: {uid}, User: {user_id}", "BOOK_DETAILS")
    
    if book_data:
        if isinstance(book_data, dict):
            debug_log(f"📖 [{operation}] Book data (dict): title='{book_data.get('title', 'NO_TITLE')}', id='{book_data.get('id', 'NO_ID')}'", "BOOK_DETAILS")
            debug_log(f"👥 [{operation}] Authors: {book_data.get('authors', 'NO_AUTHORS')}", "BOOK_DETAILS")
            debug_log(f"📚 [{operation}] Categories: {book_data.get('categories', 'NO_CATEGORIES')}", "BOOK_DETAILS")
            debug_log(f"🏢 [{operation}] Publisher: {book_data.get('publisher', 'NO_PUBLISHER')}", "BOOK_DETAILS")
            debug_log(f"📍 [{operation}] Locations: {book_data.get('locations', 'NO_LOCATIONS')}", "BOOK_DETAILS")
            debug_log(f"🎯 [{operation}] Reading status: {book_data.get('ownership', {}).get('reading_status', 'NO_STATUS')}", "BOOK_DETAILS")
            debug_log(f"💾 [{operation}] Custom metadata: {book_data.get('custom_metadata', 'NO_METADATA')}", "BOOK_DETAILS")
        else:
            debug_log(f"📖 [{operation}] Book object: title='{getattr(book_data, 'title', 'NO_TITLE')}', id='{getattr(book_data, 'id', 'NO_ID')}'", "BOOK_DETAILS")
            debug_log(f"👥 [{operation}] Authors: {getattr(book_data, 'authors', 'NO_AUTHORS')}", "BOOK_DETAILS")
            debug_log(f"📚 [{operation}] Categories: {getattr(book_data, 'categories', 'NO_CATEGORIES')}", "BOOK_DETAILS")
    else:
        debug_log(f"❌ [{operation}] No book data found for UID: {uid}", "BOOK_DETAILS")


def debug_person_details(person_data, person_id, user_id, operation="VIEW"):
    """Enhanced debugging for person details page."""
    debug_log(f"🔍 [{operation}] Starting person details for ID: {person_id}, User: {user_id}", "PERSON_DETAILS")
    
    if person_data:
        if isinstance(person_data, dict):
            debug_log(f"👤 [{operation}] Person data (dict): name='{person_data.get('name', 'NO_NAME')}', id='{person_data.get('id', 'NO_ID')}'", "PERSON_DETAILS")
            debug_log(f"📊 [{operation}] Book count: {person_data.get('book_count', 'NO_COUNT')}", "PERSON_DETAILS")
            debug_log(f"🎭 [{operation}] Contributions: {person_data.get('contributions', 'NO_CONTRIBUTIONS')}", "PERSON_DETAILS")
        else:
            debug_log(f"👤 [{operation}] Person object: name='{getattr(person_data, 'name', 'NO_NAME')}', id='{getattr(person_data, 'id', 'NO_ID')}'", "PERSON_DETAILS")
            debug_log(f"📊 [{operation}] Book count: {getattr(person_data, 'book_count', 'NO_COUNT')}", "PERSON_DETAILS")
            debug_log(f"🎭 [{operation}] Contributions: {getattr(person_data, 'contributions', 'NO_CONTRIBUTIONS')}", "PERSON_DETAILS")
    else:
        debug_log(f"❌ [{operation}] No person data found for ID: {person_id}", "PERSON_DETAILS")


def debug_genre_details(genre_data, genre_id, user_id, operation="VIEW"):
    """Enhanced debugging for genre/category details page."""
    debug_log(f"🔍 [{operation}] Starting genre details for ID: {genre_id}, User: {user_id}", "GENRE_DETAILS")
    
    if genre_data:
        if isinstance(genre_data, dict):
            debug_log(f"🏷️ [{operation}] Genre data (dict): name='{genre_data.get('name', 'NO_NAME')}', id='{genre_data.get('id', 'NO_ID')}'", "GENRE_DETAILS")
            debug_log(f"📊 [{operation}] Book count: {genre_data.get('book_count', 'NO_COUNT')}", "GENRE_DETAILS")
            debug_log(f"🌳 [{operation}] Parent: {genre_data.get('parent_id', 'NO_PARENT')}", "GENRE_DETAILS")
            debug_log(f"🌿 [{operation}] Children: {genre_data.get('children', 'NO_CHILDREN')}", "GENRE_DETAILS")
        else:
            debug_log(f"🏷️ [{operation}] Genre object: name='{getattr(genre_data, 'name', 'NO_NAME')}', id='{getattr(genre_data, 'id', 'NO_ID')}'", "GENRE_DETAILS")
            debug_log(f"📊 [{operation}] Book count: {getattr(genre_data, 'book_count', 'NO_COUNT')}", "GENRE_DETAILS")
            debug_log(f"🌳 [{operation}] Parent: {getattr(genre_data, 'parent_id', 'NO_PARENT')}", "GENRE_DETAILS")
    else:
        debug_log(f"❌ [{operation}] No genre data found for ID: {genre_id}", "GENRE_DETAILS")


def debug_metadata_operation(book_id, uid, user_id, metadata_data, operation="VIEW"):
    """Enhanced debugging for metadata operations."""
    debug_log(f"🔍 [{operation}] Starting metadata operation for Book UID: {uid}, User: {user_id}", "METADATA")
    debug_log(f"📝 [{operation}] Book ID: {book_id}", "METADATA")
    
    if metadata_data:
        if isinstance(metadata_data, dict):
            debug_log(f"📊 [{operation}] Metadata fields count: {len(metadata_data)}", "METADATA")
            for field_name, field_value in metadata_data.items():
                debug_log(f"🏷️ [{operation}] Field '{field_name}': {field_value}", "METADATA")
        else:
            debug_log(f"📊 [{operation}] Metadata object: {metadata_data}", "METADATA")
    else:
        debug_log(f"❌ [{operation}] No metadata found", "METADATA")


def debug_service_call(service_name, method_name, params, result, operation="CALL"):
    """Enhanced debugging for service layer calls."""
    debug_log(f"🔧 [{operation}] Service call: {service_name}.{method_name}", "SERVICE")
    debug_log(f"📝 [{operation}] Parameters: {params}", "SERVICE")
    
    if result is not None:
        if isinstance(result, list):
            debug_log(f"📊 [{operation}] Result: List with {len(result)} items", "SERVICE")
        elif isinstance(result, dict):
            debug_log(f"📊 [{operation}] Result: Dict with keys: {list(result.keys())}", "SERVICE")
        else:
            debug_log(f"📊 [{operation}] Result type: {type(result)}", "SERVICE")
    else:
        debug_log(f"❌ [{operation}] Result: None", "SERVICE")


def debug_template_data(template_name, data_dict, operation="RENDER"):
    """Enhanced debugging for template rendering."""
    debug_log(f"🎨 [{operation}] Rendering template: {template_name}", "TEMPLATE")
    
    if data_dict:
        debug_log(f"📊 [{operation}] Template data keys: {list(data_dict.keys())}", "TEMPLATE")
        for key, value in data_dict.items():
            if isinstance(value, list):
                debug_log(f"📋 [{operation}] {key}: List with {len(value)} items", "TEMPLATE")
            elif isinstance(value, dict):
                debug_log(f"📋 [{operation}] {key}: Dict with {len(value)} keys", "TEMPLATE")
            else:
                debug_log(f"📋 [{operation}] {key}: {type(value)} - {str(value)[:100]}{'...' if len(str(value)) > 100 else ''}", "TEMPLATE")
    else:
        debug_log(f"❌ [{operation}] No template data", "TEMPLATE")
