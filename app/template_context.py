"""
Template context processors for making common objects available in templates.
"""

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


def register_context_processors(app):
    """Register all context processors with the Flask app."""
    app.context_processor(inject_debug_manager)
