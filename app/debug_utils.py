"""
Debug utilities for Bibliotheca
Provides comprehensive debugging capabilities for troubleshooting
"""

import logging
import sys
from datetime import datetime
from flask import request, session, current_app, g
from flask_login import current_user
from functools import wraps

# Configure debug logger
debug_logger = logging.getLogger('bibliotheca.debug')

def setup_debug_logging():
    """Setup debug logging based on configuration"""
    if not current_app.config.get('DEBUG_MODE', False):
        return
    
    # Remove existing handlers to avoid duplicates
    debug_logger.handlers.clear()
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, current_app.config.get('DEBUG_LOG_LEVEL', 'INFO')))
    
    # Create detailed formatter
    formatter = logging.Formatter(
        '[%(asctime)s] [DEBUG:%(name)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    
    debug_logger.addHandler(console_handler)
    debug_logger.setLevel(logging.DEBUG)
    debug_logger.propagate = False
    
    debug_logger.info("🐛 Debug logging enabled")

def debug_log(category, message, level='INFO'):
    """Log debug message if debugging is enabled for the category"""
    if not current_app.config.get('DEBUG_MODE', False):
        return
    
    # Check if specific category debugging is enabled
    debug_key = f'DEBUG_{category.upper()}'
    if not current_app.config.get(debug_key, False):
        return
    
    log_level = getattr(logging, level.upper(), logging.INFO)
    debug_logger.log(log_level, f"[{category.upper()}] {message}")

def debug_csrf(message, level='INFO'):
    """Log CSRF-related debug information"""
    debug_log('CSRF', message, level)

def debug_session(message, level='INFO'):
    """Log session-related debug information"""
    debug_log('SESSION', message, level)

def debug_auth(message, level='INFO'):
    """Log authentication-related debug information"""
    debug_log('AUTH', message, level)

def debug_request(message, level='INFO'):
    """Log request-related debug information"""
    debug_log('REQUESTS', message, level)

def debug_csrf_token():
    """Debug CSRF token information"""
    if not current_app.config.get('DEBUG_CSRF', False):
        return
    
    try:
        from flask_wtf.csrf import validate_csrf
        from flask import session
        
        debug_csrf(f"Session keys: {list(session.keys())}")
        debug_csrf(f"CSRF enabled: {current_app.config.get('WTF_CSRF_ENABLED', False)}")
        debug_csrf(f"Session has _csrf_token: {'_csrf_token' in session}")
        debug_csrf(f"Session has csrf_token: {'csrf_token' in session}")
        
        if '_csrf_token' in session:
            debug_csrf(f"CSRF token (_csrf_token) exists: {session['_csrf_token'][:10]}...")
        if 'csrf_token' in session:
            debug_csrf(f"CSRF token (csrf_token) exists: {session['csrf_token'][:10]}...")
        if '_csrf_token' not in session and 'csrf_token' not in session:
            debug_csrf("No CSRF token in session")
            
    except Exception as e:
        debug_csrf(f"Error checking CSRF token: {e}", 'ERROR')

def debug_session_info():
    """Debug session information"""
    if not current_app.config.get('DEBUG_SESSION', False):
        return
    
    try:
        debug_session(f"Session ID: {session.get('_permanent', 'N/A')}")
        debug_session(f"Session keys: {list(session.keys())}")
        debug_session(f"Session permanent: {session.permanent}")
        debug_session(f"User authenticated: {current_user.is_authenticated if current_user else 'No current_user'}")
        
        if current_user and current_user.is_authenticated:
            debug_session(f"User ID: {current_user.id}")
            debug_session(f"Username: {current_user.username}")
            debug_session(f"Must change password: {getattr(current_user, 'password_must_change', 'N/A')}")
            
    except Exception as e:
        debug_session(f"Error checking session: {e}", 'ERROR')

def debug_request_info():
    """Debug request information"""
    if not current_app.config.get('DEBUG_REQUESTS', False):
        return
    
    try:
        debug_request(f"Method: {request.method}")
        debug_request(f"Endpoint: {request.endpoint}")
        debug_request(f"URL: {request.url}")
        debug_request(f"Remote addr: {request.remote_addr}")
        debug_request(f"User agent: {request.user_agent}")
        
        if request.method == 'POST':
            debug_request(f"Form keys: {list(request.form.keys())}")
            debug_request(f"Has CSRF token in form: {'csrf_token' in request.form}")
            
    except Exception as e:
        debug_request(f"Error checking request: {e}", 'ERROR')

def debug_middleware():
    """Middleware to log debug information for each request"""
    if not current_app.config.get('DEBUG_MODE', False):
        return
    
    debug_request_info()
    debug_session_info()
    debug_csrf_token()

def debug_route(category='GENERAL'):
    """Decorator to add debug logging to routes"""
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if current_app.config.get('DEBUG_MODE', False):
                debug_log(category, f"Entering route: {f.__name__}")
                debug_middleware()
                
                try:
                    result = f(*args, **kwargs)
                    debug_log(category, f"Route {f.__name__} completed successfully")
                    return result
                except Exception as e:
                    debug_log(category, f"Route {f.__name__} failed: {e}", 'ERROR')
                    raise
            else:
                return f(*args, **kwargs)
        return decorated_function
    return decorator

def get_debug_info():
    """Get comprehensive debug information as a dictionary"""
    if not current_app.config.get('DEBUG_MODE', False):
        return {'debug_disabled': True}
    
    debug_info = {
        'timestamp': datetime.now().isoformat(),
        'debug_mode': True,
        'config': {
            'DEBUG_CSRF': current_app.config.get('DEBUG_CSRF', False),
            'DEBUG_SESSION': current_app.config.get('DEBUG_SESSION', False),
            'DEBUG_AUTH': current_app.config.get('DEBUG_AUTH', False),
            'DEBUG_REQUESTS': current_app.config.get('DEBUG_REQUESTS', False),
            'WTF_CSRF_ENABLED': current_app.config.get('WTF_CSRF_ENABLED', False),
        },
        'request': {
            'method': request.method,
            'endpoint': request.endpoint,
            'url': request.url,
            'form_keys': list(request.form.keys()) if request.method == 'POST' else None,
            'has_csrf_in_form': 'csrf_token' in request.form if request.method == 'POST' else None,
        },
        'session': {
            'keys': list(session.keys()),
            'permanent': session.permanent,
            'has_csrf_token': '_csrf_token' in session,
            'has_csrf_token_alt': 'csrf_token' in session,
            'csrf_token_preview': session.get('_csrf_token', '')[:10] + '...' if '_csrf_token' in session else None,
            'csrf_token_alt_preview': session.get('csrf_token', '')[:10] + '...' if 'csrf_token' in session else None,
        },
        'user': {
            'authenticated': current_user.is_authenticated if current_user else False,
            'id': current_user.id if current_user and current_user.is_authenticated else None,
            'username': current_user.username if current_user and current_user.is_authenticated else None,
            'must_change_password': getattr(current_user, 'password_must_change', None) if current_user and current_user.is_authenticated else None,
        }
    }
    
    return debug_info

def print_debug_banner():
    """Print debug mode banner"""
    if not current_app.config.get('DEBUG_MODE', False):
        return
        
    print("=" * 60)
    print("🐛 BIBLIOTHECA DEBUG MODE ENABLED")
    print("=" * 60)
    print(f"CSRF Debug: {current_app.config.get('DEBUG_CSRF', False)}")
    print(f"Session Debug: {current_app.config.get('DEBUG_SESSION', False)}")
    print(f"Auth Debug: {current_app.config.get('DEBUG_AUTH', False)}")
    print(f"Request Debug: {current_app.config.get('DEBUG_REQUESTS', False)}")
    print("=" * 60)
