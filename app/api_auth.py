"""
API Authentication Module

Provides secure authentication for API endpoints using API tokens
while maintaining CSRF protection for web interface.
"""

import secrets
import hashlib
from datetime import datetime, timedelta
from functools import wraps
from flask import request, jsonify, current_app
from flask_login import current_user


class APIToken:
    """API Token management for secure API access."""
    
    @staticmethod
    def generate_token(user_id: str, name: str = "API Token") -> tuple[str, str]:
        """
        Generate a new API token for a user.
        Returns (token, hashed_token) tuple.
        """
        # Generate a secure random token
        token = secrets.token_urlsafe(32)
        
        # Hash the token for storage (never store plain tokens)
        hashed_token = hashlib.sha256(token.encode()).hexdigest()
        
        return token, hashed_token
    
    @staticmethod
    def verify_token(token: str, hashed_token: str) -> bool:
        """Verify if a token matches its hash."""
        if not token or not hashed_token:
            return False
        
        computed_hash = hashlib.sha256(token.encode()).hexdigest()
        return secrets.compare_digest(computed_hash, hashed_token)


def api_token_required(f):
    """
    Decorator for API endpoints that require token authentication.
    This bypasses CSRF protection for API calls while maintaining security.
    Validates token directly without relying on Flask-Login sessions.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        import logging
        logger = logging.getLogger(__name__)
        
        # Check if this is an API request
        if request.path.startswith('/api/'):
            logger.info(f"API request to {request.path}")
            # First check for token authentication
            auth_header = request.headers.get('Authorization')
            logger.info(f"Authorization header: {auth_header[:30] if auth_header else None}...")
            
            if auth_header and auth_header.startswith('Bearer '):
                token = auth_header.split(' ')[1]
                logger.info(f"Extracted token: {token[:10]}...")
                if validate_api_token(token):
                    logger.info("Token validation successful, allowing access")
                    # Token is valid, proceed with the request
                    return f(*args, **kwargs)
                else:
                    logger.info("Token validation failed")
                    # Invalid token
                    return jsonify({'error': 'Invalid API token'}), 401
            
            # No token provided, check if there's an active session
            # Use hasattr to avoid triggering unauthorized handler
            logger.info("No Bearer token, checking session authentication")
            from flask_login import current_user
            try:
                # Try to access current_user carefully
                if hasattr(current_user, 'is_authenticated') and current_user.is_authenticated:
                    logger.info("User authenticated via session, allowing access")
                    # User is logged in via web interface, allow access
                    return f(*args, **kwargs)
            except Exception as e:
                logger.info(f"Session check failed: {e}")
            
            logger.info("No authentication found, returning 401")
            # No authentication provided
            return jsonify({
                'error': 'Authentication required', 
                'message': 'Provide API token via Authorization header or login via web interface',
                'authentication_methods': [
                    'Bearer token in Authorization header',
                    'Session-based login via web interface'
                ]
            }), 401
        
        # For non-API requests, fall back to regular authentication check
        from flask_login import current_user
        if not current_user.is_authenticated:
            return jsonify({'error': 'Authentication required'}), 401
        
        return f(*args, **kwargs)
    
    return decorated_function


def validate_api_token(token: str) -> bool:
    """
    Validate an API token.
    For now, this is a simple implementation.
    In production, you'd check against stored tokens in database.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # Simple validation for development
    # In production, implement proper token storage and validation
    logger.info(f"validate_api_token called with token: {token[:10] if token else None}...")
    
    if not token:
        logger.info("No token provided")
        return False
    
    # For development, accept a hardcoded test token
    # TODO: Replace with proper database-backed token validation
    test_token = current_app.config.get('API_TEST_TOKEN', 'dev-token-12345')
    logger.info(f"Comparing with test_token: {test_token[:10]}...")
    
    result = secrets.compare_digest(token, test_token)
    logger.info(f"Token validation result: {result}")
    return result


def api_auth_optional(f):
    """
    Decorator for API endpoints where authentication is optional.
    Still bypasses CSRF for API calls but doesn't require authentication.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check for API token
        auth_header = request.headers.get('Authorization')
        
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header.split(' ')[1]
            if not validate_api_token(token):
                return jsonify({'error': 'Invalid API token'}), 401
        
        # Proceed regardless of authentication status
        return f(*args, **kwargs)
    
    return decorated_function
