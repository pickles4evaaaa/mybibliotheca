"""
Template context processors for making common objects available in templates.
"""

import os
from flask import current_app
from app.debug_system import get_debug_manager


def inject_debug_manager():
    """Make debug manager available in all templates."""
    try:
        debug_manager = get_debug_manager()
        
        # Test that the debug manager is working properly
        if debug_manager and hasattr(debug_manager, 'should_show_debug'):
            return {
                'debug_manager': debug_manager,
                'get_debug_manager': get_debug_manager  # Keep the old one for backward compatibility
            }
        else:
            raise Exception("Debug manager is not properly initialized")
            
    except Exception as e:
        # If debug manager fails, provide safe fallbacks
        
        # Create a dummy debug manager that always returns False
        class DummyDebugManager:
            def should_show_debug(self, user=None):
                return False
            def is_debug_enabled(self):
                return False
            def is_user_admin(self, user=None):
                return False
        
        dummy_manager = DummyDebugManager()
        return {
            'debug_manager': dummy_manager,
            'get_debug_manager': lambda: dummy_manager
        }


def inject_site_config():
    """Make site configuration available in all templates."""
    # Import here to avoid circular imports
    from app.admin import load_system_config
    
    try:
        # Load from config file first, fall back to environment variables
        system_config = load_system_config()
        site_name = system_config.get('site_name', os.getenv('SITE_NAME', 'MyBibliotheca'))
        server_timezone = system_config.get('server_timezone', os.getenv('TIMEZONE', 'UTC'))
    except Exception:
        # Fallback to environment variables if config loading fails
        site_name = os.getenv('SITE_NAME', 'MyBibliotheca')
        server_timezone = os.getenv('TIMEZONE', 'UTC')
    
    return {
        'site_name': site_name,
        'server_timezone': server_timezone
    }


def register_context_processors(app):
    """Register all context processors with the Flask app."""
    app.context_processor(inject_debug_manager)
    app.context_processor(inject_site_config)
