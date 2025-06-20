"""
Flask application factory for Redis-only Bibliotheca.

This version completely removes SQLite dependency and uses Redis as the sole data store.
"""

import os
from flask import Flask, session, request, jsonify, redirect, url_for
from flask_login import LoginManager
from flask_wtf.csrf import CSRFProtect
from config import Config

login_manager = LoginManager()
csrf = CSRFProtect()

@login_manager.user_loader
def load_user(user_id):
    """Load user from Redis via the user service."""
    from .services import user_service
    try:
        user = user_service.get_user_by_id_sync(user_id)
        return user
    except Exception as e:
        print(f"Error loading user {user_id}: {e}")
        return None

@login_manager.unauthorized_handler
def unauthorized():
    """Custom unauthorized handler that returns JSON for API requests."""
    # Check if this is an API request
    if request.path.startswith('/api/'):
        return jsonify({
            'error': 'Authentication required',
            'message': 'This API endpoint requires authentication. Provide an API token or login.',
            'authentication_methods': [
                'Bearer token in Authorization header',
                'Session-based login via web interface'
            ]
        }), 401
    
    # For web requests, redirect to login page as usual
    return redirect(url_for('auth.login', next=request.endpoint))

def create_development_admin():
    """Create development admin user from environment variables if specified."""
    dev_username = os.getenv('DEV_ADMIN_USERNAME')
    dev_password = os.getenv('DEV_ADMIN_PASSWORD')
    
    if dev_username and dev_password:
        from .services import user_service
        from .domain.models import User
        
        # Check if admin user already exists
        try:
            existing_admin = user_service.get_user_by_username_sync(dev_username)
            if not existing_admin:
                print(f"🔧 Creating development admin user: {dev_username}")
                try:
                    # Create admin user using the service
                    admin_user = user_service.create_user_sync(
                        username=dev_username,
                        email=f"{dev_username}@localhost.dev",
                        password_hash=None  # Will be set by the service
                    )
                    
                    # Set password and admin status
                    admin_user.set_password(dev_password)
                    admin_user.is_admin = True
                    admin_user.is_active = True
                    
                    # Update the user in Redis
                    user_service.update_user_profile_sync(
                        int(admin_user.id), 
                        username=dev_username, 
                        email=f"{dev_username}@localhost.dev"
                    )
                    
                    print(f"✅ Development admin user '{dev_username}' created successfully!")
                    return True
                except Exception as e:
                    print(f"❌ Failed to create development admin user: {e}")
                    return False
            else:
                print(f"ℹ️  Development admin user '{dev_username}' already exists")
                return True
        except Exception as e:
            print(f"❌ Error checking for existing admin user: {e}")
            return False
    return False

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # Initialize debug utilities
    from .debug_utils import setup_debug_logging, print_debug_banner, debug_middleware
    
    with app.app_context():
        setup_debug_logging()
        print_debug_banner()

    # Initialize extensions (no SQLAlchemy)
    csrf.init_app(app)
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = 'Please log in to access this page.'
    login_manager.login_message_category = 'info'

    # Template context processor to make CSRF token globally available
    @app.context_processor
    def inject_csrf_token():
        """Make CSRF token available in all templates."""
        from flask_wtf.csrf import generate_csrf
        return dict(csrf_token=generate_csrf)

    # CSRF error handler
    @app.errorhandler(400)
    def handle_csrf_error(e):
        """Handle CSRF errors with user-friendly messages."""
        if "CSRF" in str(e) or "csrf" in str(e.description):
            # Check if this is an AJAX request
            if request.is_json or 'application/json' in request.headers.get('Content-Type', ''):
                return jsonify({
                    'error': 'CSRF token missing or invalid',
                    'message': 'Please refresh the page and try again. Include X-CSRFToken header for API requests.',
                    'csrf_token': csrf.generate_csrf()
                }), 400
            else:
                # For web requests, redirect back with error message
                from flask import flash, redirect, url_for
                flash('Security token expired. Please try again.', 'error')
                return redirect(request.referrer or url_for('main.index'))
        return e

    # REDIS DATABASE INITIALIZATION
    with app.app_context():
        print("🚀 Initializing Redis-only Bibliotheca...")
        
        # Test Redis connection
        try:
            from .infrastructure.redis_graph import get_graph_storage
            storage = get_graph_storage()
            # Simple connection test
            print("✅ Redis connection successful")
        except Exception as e:
            print(f"❌ Redis connection failed: {e}")
            print("🔧 Make sure Redis is running and accessible")
        
        # Development auto-setup: Create admin user from environment variables
        create_development_admin()
        
        print("🎉 Redis-only initialization completed successfully!")

    # Add middleware to check for setup requirements
    @app.before_request
    def check_setup_and_password_requirements():
        from flask import request, redirect, url_for
        from flask_login import current_user
        from .debug_utils import debug_middleware
        from .services import user_service
        
        # Run debug middleware if enabled
        debug_middleware()
        
        # Check if setup is needed (no users exist)
        try:
            user_count = user_service.get_user_count_sync()
            if user_count == 0:
                # Skip for setup route and static files
                if request.endpoint in ['auth.setup', 'static'] or (request.endpoint and request.endpoint.startswith('static')):
                    return
                # Redirect to setup page
                return redirect(url_for('auth.setup'))
        except Exception as e:
            print(f"Error checking user count: {e}")
            # If we can't check users, assume setup is needed
            if request.endpoint not in ['auth.setup', 'static']:
                return redirect(url_for('auth.setup'))
        
        # For API endpoints, skip the session-based authentication checks
        if request.path.startswith('/api/'):
            print("DEBUG: API request detected, skipping session checks")
            return
        
        # Skip if user is not authenticated (for non-API endpoints)
        if not current_user.is_authenticated:
            return
        
        # Skip for certain routes to avoid redirect loops
        allowed_endpoints = [
            'auth.forced_password_change',
            'auth.logout',
            'auth.setup',
            'static'
        ]
        
        # Allow API and AJAX requests, and skip for static files
        if request.endpoint in allowed_endpoints or (request.endpoint and request.endpoint.startswith('static')):
            return
        
        # Check if user must change password
        if hasattr(current_user, 'password_must_change') and current_user.password_must_change:
            if request.endpoint != 'auth.forced_password_change':
                return redirect(url_for('auth.forced_password_change'))

    # Register blueprints
    from .routes import bp
    from .auth import auth
    from .admin import admin
    try:
        from .location_routes import bp as locations_bp
        app.register_blueprint(locations_bp)
    except ImportError as e:
        print(f"⚠️  Could not import location routes: {e}")
    
    app.register_blueprint(bp)
    app.register_blueprint(auth, url_prefix='/auth')
    app.register_blueprint(admin, url_prefix='/admin')
    
    # Register API blueprints
    from .api.books import books_api
    from .api.reading_logs import reading_logs_api
    from .api.users import users_api
    app.register_blueprint(books_api)
    app.register_blueprint(reading_logs_api)
    app.register_blueprint(users_api)

    return app
