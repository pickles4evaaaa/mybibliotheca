"""
Flask application factory for Redis-only Bibliotheca.

This version completely removes SQLite dependency and uses Redis as the sole data store.
"""

import os
from flask import Flask, session, request, jsonify, redirect, url_for
from flask_login import LoginManager
from flask_wtf.csrf import CSRFProtect
from flask_session import Session
from config import Config

login_manager = LoginManager()
csrf = CSRFProtect()
sess = Session()

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
    
    print(f"🔍 Debug: DEV_ADMIN_USERNAME = {dev_username}")
    print(f"🔍 Debug: DEV_ADMIN_PASSWORD = {'***' if dev_password else 'None'}")
    
    if dev_username and dev_password:
        from .services import user_service
        from .domain.models import User
        from werkzeug.security import generate_password_hash
        
        # Check if admin user already exists
        try:
            print(f"🔍 Checking for existing admin user: {dev_username}")
            existing_admin = user_service.get_user_by_username_sync(dev_username)
            if existing_admin:
                print(f"� Admin user {dev_username} already exists, skipping creation")
                return True
            else:
                print(f"�🔧 Creating development admin user: {dev_username}")
                try:
                    # Hash the password first
                    password_hash = generate_password_hash(dev_password)
                    print(f"🔍 Password hash created: {password_hash[:20]}...")
                    
                    # Create admin user using the service with all required fields
                    admin_user = user_service.create_user_sync(
                        username=dev_username,
                        email=f"{dev_username}@localhost.dev",
                        password_hash=password_hash,
                        is_admin=True,
                        is_active=True,
                        password_must_change=False
                    )
                    
                    print(f"✅ Development admin user '{dev_username}' created successfully!")
                    print(f"🔍 Admin user ID: {admin_user.id}, Password hash: {admin_user.password_hash[:20] if admin_user.password_hash else 'None'}...")
                    return True
                except Exception as e:
                    print(f"❌ Failed to create development admin user: {e}")
                    import traceback
                    traceback.print_exc()
                    return False
        except Exception as e:
            print(f"❌ Error checking for existing admin user: {e}")
            return False
    else:
        print("🔍 No development admin credentials provided")
    return False

def _check_for_sqlite_migration():
    """Disabled: Manual migration preferred over automatic migration."""
    pass

def _initialize_default_templates():
    """Initialize default import templates for Goodreads and StoryGraph if they don't exist."""
    try:
        import os
        from datetime import datetime
        from .domain.models import ImportMappingTemplate
        from .services import import_mapping_service
        
        # Check if verbose logging is enabled
        verbose_init = os.getenv('BIBLIOTHECA_VERBOSE_INIT', 'true').lower() == 'true'
        
        # Check if default templates already exist
        goodreads_template = None
        storygraph_template = None
        try:
            goodreads_template = import_mapping_service.get_template_by_id_sync("default_goodreads")
            storygraph_template = import_mapping_service.get_template_by_id_sync("default_storygraph")
            
            if goodreads_template and storygraph_template:
                if verbose_init:
                    print("✅ Default import templates already exist")
                return
        except:
            pass  # Templates don't exist, we'll create them
        
        if verbose_init:
            print("🔄 Creating default import templates...")
        
        # Create Goodreads template
        if not goodreads_template:
            goodreads_template = ImportMappingTemplate(
                id="default_goodreads",
                user_id="__system__",
                name="Goodreads Export (Default)",
                description="Default template for standard Goodreads library export CSV files",
                source_type="goodreads",
                sample_headers=[
                    "Book Id", "Title", "Author", "Author l-f", "Additional Authors", 
                    "ISBN", "ISBN13", "My Rating", "Average Rating", "Publisher", 
                    "Binding", "Number of Pages", "Year Published", "Original Publication Year", 
                    "Date Read", "Date Added", "Bookshelves", "Bookshelves with positions", 
                    "Exclusive Shelf", "My Review", "Spoiler", "Private Notes", "Read Count", "Owned Copies"
                ],
                field_mappings={
                    "Title": {"action": "map_existing", "target_field": "title"},
                    "Author": {"action": "map_existing", "target_field": "author"},
                    "Additional Authors": {"action": "map_existing", "target_field": "additional_authors"},
                    "ISBN13": {"action": "map_existing", "target_field": "isbn"},
                    "ISBN": {"action": "map_existing", "target_field": "isbn"},
                    "My Rating": {"action": "map_existing", "target_field": "rating"},
                    "Average Rating": {"action": "map_existing", "target_field": "average_rating"},
                    "Publisher": {"action": "map_existing", "target_field": "publisher"},
                    "Number of Pages": {"action": "map_existing", "target_field": "page_count"},
                    "Year Published": {"action": "map_existing", "target_field": "publication_year"},
                    "Original Publication Year": {"action": "map_existing", "target_field": "original_publication_year"},
                    "Date Read": {"action": "map_existing", "target_field": "date_read"},
                    "Date Added": {"action": "map_existing", "target_field": "date_added"},
                    "Bookshelves": {"action": "map_existing", "target_field": "reading_status"},
                    "Exclusive Shelf": {"action": "map_existing", "target_field": "reading_status"},
                    "My Review": {"action": "map_existing", "target_field": "notes"},
                    "Private Notes": {"action": "map_existing", "target_field": "private_notes"}
                },
                times_used=0,
                last_used=None,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            import_mapping_service.create_template_sync(goodreads_template)
            if verbose_init:
                print("✅ Created Goodreads default template")
        
        # Create StoryGraph template
        if not storygraph_template:
            storygraph_template = ImportMappingTemplate(
                id="default_storygraph",
                user_id="__system__",
                name="StoryGraph Export (Default)",
                description="Default template for standard StoryGraph library export CSV files",
                source_type="storygraph", 
                sample_headers=[
                    "Title", "Authors", "Contributors", "ISBN/UID", "Format",
                    "Read Status", "Date Added", "Last Date Read", "Dates Read", 
                    "Read Count", "Moods", "Pace", "Character- or Plot-Driven?",
                    "Strong Character Development?", "Loveable Characters?", 
                    "Diverse Characters?", "Flawed Characters?", "Star Rating",
                    "Review", "Content Warnings", "Content Warning Description", 
                    "Tags", "Owned?"
                ],
                field_mappings={
                    "Title": {"action": "map_existing", "target_field": "title"},
                    "Authors": {"action": "map_existing", "target_field": "author"},
                    "Contributors": {"action": "map_existing", "target_field": "additional_authors"},
                    "ISBN/UID": {"action": "map_existing", "target_field": "isbn"},
                    "Format": {"action": "map_existing", "target_field": "format"},
                    "Read Status": {"action": "map_existing", "target_field": "reading_status"},
                    "Date Added": {"action": "map_existing", "target_field": "date_added"},
                    "Last Date Read": {"action": "map_existing", "target_field": "date_read"},
                    "Dates Read": {"action": "map_existing", "target_field": "date_ranges"},
                    "Read Count": {"action": "map_existing", "target_field": "read_count"},
                    "Star Rating": {"action": "map_existing", "target_field": "rating"},
                    "Review": {"action": "map_existing", "target_field": "notes"},
                    "Tags": {"action": "map_existing", "target_field": "categories"},
                    "Moods": {"action": "map_existing", "target_field": "categories"}
                },
                times_used=0,
                last_used=None,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            import_mapping_service.create_template_sync(storygraph_template)
            if verbose_init:
                print("✅ Created StoryGraph default template")
            
        if verbose_init:
            print("🎉 Default import templates initialized successfully!")
        
    except Exception as e:
        verbose_init = os.getenv('BIBLIOTHECA_VERBOSE_INIT', 'true').lower() == 'true'
        if verbose_init:
            print(f"⚠️  Warning: Could not initialize default templates: {e}")
        # Don't fail app startup if templates can't be created


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # Initialize debug utilities
    from .debug_utils import setup_debug_logging, print_debug_banner, debug_middleware
    
    with app.app_context():
        setup_debug_logging()
        print_debug_banner()
        
        # Check for SQLite migration needs
        _check_for_sqlite_migration()

    # Initialize extensions (no SQLAlchemy)
    csrf.init_app(app)
    sess.init_app(app)  # Initialize Flask-Session
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

    @app.context_processor
    def inject_site_name():
        """Make site name available in all templates."""
        from .infrastructure.redis_graph import get_graph_storage
        try:
            redis_client = get_graph_storage().redis
            site_name = redis_client.get('site_name')
            if site_name:
                site_name = site_name.decode('utf-8') if isinstance(site_name, bytes) else site_name
            else:
                site_name = 'MyBibliotheca'
        except Exception:
            site_name = 'MyBibliotheca'
        return dict(site_name=site_name)

    @app.context_processor
    def inject_theme_preference():
        """Make theme preference available in all templates."""
        from flask_login import current_user
        try:
            if current_user.is_authenticated:
                # Try to get user's theme preference from Redis
                from .infrastructure.redis_graph import get_graph_storage
                redis_client = get_graph_storage().redis
                theme_key = f'user_theme:{current_user.id}'
                theme = redis_client.get(theme_key)
                if theme:
                    theme = theme.decode('utf-8') if isinstance(theme, bytes) else theme
                else:
                    # Check session as fallback for Redis
                    from flask import session
                    theme = session.get('theme', 'light')
            else:
                # For non-authenticated users, check session or default to light
                from flask import session
                theme = session.get('theme', 'light')
        except Exception:
            theme = 'light'
        return dict(current_theme=theme)

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
                # For web requests, handle differently based on route
                from flask import flash, redirect, url_for
                
                # Special handling for onboarding routes
                if request.endpoint and request.endpoint.startswith('onboarding.'):
                    flash('Security token expired. The page will be refreshed with a new token.', 'warning')
                    # Redirect to the same onboarding step to refresh the form
                    if 'step' in request.view_args:
                        return redirect(url_for('onboarding.step', step_num=request.view_args['step']))
                    else:
                        return redirect(url_for('onboarding.start'))
                else:
                    flash('Security token expired. Please try again.', 'error')
                    return redirect(request.referrer or url_for('main.index'))
        return e

    def check_for_migration_reminder():
        """Migration reminder disabled - now available only through admin panel."""
        # SQLite migration detection disabled to prevent startup issues
        # Migration is available through the admin panel -> /admin/migration
        pass

    # REDIS DATABASE INITIALIZATION
    with app.app_context():
        # Use environment variable to control verbose logging across multiple workers
        verbose_init = os.getenv('BIBLIOTHECA_VERBOSE_INIT', 'true').lower() == 'true'
        
        if verbose_init:
            print("🚀 Initializing Redis-only MyBibliotheca...")
        
        # Test Redis connection
        try:
            from .infrastructure.redis_graph import get_graph_storage
            storage = get_graph_storage()
            # Simple connection test
            if verbose_init:
                print("✅ Redis connection successful")
            
            # Initialize services and attach to app
            from .services import (
                RedisBookService, RedisUserService, RedisReadingLogService,
                RedisCustomFieldService, RedisImportMappingService, RedisDirectImportService
            )
            
            try:
                app.book_service = RedisBookService()
                app.user_service = RedisUserService()
                app.reading_log_service = RedisReadingLogService()
                app.custom_field_service = RedisCustomFieldService()
                app.import_mapping_service = RedisImportMappingService()
                app.direct_import_service = RedisDirectImportService()
                if verbose_init:
                    print("📦 Services initialized successfully")
            except Exception as e:
                print(f"⚠️ Warning: Could not initialize some services: {e}")
            
            # Initialize default import templates
            if verbose_init:
                _initialize_default_templates()
            else:
                # Still run template initialization, just silently
                try:
                    _initialize_default_templates()
                except Exception:
                    pass  # Fail silently for worker processes
            
        except Exception as e:
            print(f"❌ Redis connection failed: {e}")
            print("🔧 Make sure Redis is running and accessible")
        
        # Development mode - skip auto admin creation, use setup page instead
        if verbose_init:
            print("🔧 Development mode: Use the setup page to create your admin user")
            print("🎉 Redis-only initialization completed successfully!")
        
        # Check for SQLite databases that might need migration
        check_for_migration_reminder()

    # Add middleware to check for setup requirements
    @app.before_request
    def check_setup_and_password_requirements():
        from flask import request, redirect, url_for
        from flask_login import current_user
        from .debug_utils import debug_middleware, debug_auth, debug_csrf
        from .services import user_service
        
        # Run debug middleware if enabled
        debug_middleware()
        
        # Special debugging for setup route
        if request.endpoint == 'auth.setup':
            debug_auth("🔍 BEFORE_REQUEST: Setup route detected!")
            if request.method == 'POST':
                debug_auth(f"Form keys in before_request: {list(request.form.keys())}")
                debug_auth(f"Content type: {request.content_type}")
                debug_auth(f"Has form data: {bool(request.form)}")
            
            debug_auth(f"Session keys before processing: {list(session.keys()) if 'session' in globals() else 'No session'}")
            
            # Force session and CSRF token to be established on GET request
            if request.method == 'GET':
                if 'csrf_token' not in session:
                    from flask_wtf.csrf import generate_csrf
                    debug_csrf("🔧 No CSRF token in session for setup GET. Generating one now.")
                    generate_csrf()
                    # Explicitly mark the session as modified to ensure it's saved
                    session.modified = True
                    debug_csrf(f"🔧 Session marked as modified. Keys: {list(session.keys())}")
        
        # Special handling for onboarding routes
        if request.endpoint and request.endpoint.startswith('onboarding.'):
            debug_auth("🔍 BEFORE_REQUEST: Onboarding route detected!")
            if request.method == 'GET':
                # Ensure CSRF token is available for onboarding forms
                if 'csrf_token' not in session:
                    from flask_wtf.csrf import generate_csrf
                    debug_csrf("🔧 No CSRF token in session for onboarding GET. Generating one now.")
                    generate_csrf()
                    session.modified = True
                    debug_csrf(f"🔧 Session marked as modified for onboarding. Keys: {list(session.keys())}")
            elif request.method == 'POST':
                debug_auth(f"Onboarding POST: Form keys: {list(request.form.keys())}")
                debug_auth(f"CSRF token in form: {'csrf_token' in request.form}")
                debug_auth(f"CSRF token in session: {'csrf_token' in session}")
                if 'csrf_token' in request.form and 'csrf_token' in session:
                    debug_auth(f"Form CSRF: {request.form.get('csrf_token')[:10]}...")
                    debug_auth(f"Session CSRF: {session.get('csrf_token')[:10] if session.get('csrf_token') else 'None'}...")
                    from flask_wtf.csrf import generate_csrf
                    generate_csrf()
                    # Explicitly mark the session as modified to ensure it's saved
                    session.modified = True
                    debug_csrf(f"🔧 Session marked as modified. Keys: {list(session.keys())}")
        
        # Special handling for onboarding routes to ensure CSRF tokens work
        if request.endpoint and request.endpoint.startswith('onboarding.'):
            debug_auth(f"🔍 BEFORE_REQUEST: Onboarding route detected: {request.endpoint}")
            if request.method == 'GET':
                if 'csrf_token' not in session:
                    from flask_wtf.csrf import generate_csrf
                    debug_csrf("🔧 No CSRF token in session for onboarding GET. Generating one now.")
                    generate_csrf()
                    session.modified = True
                    debug_csrf(f"🔧 Session marked as modified for onboarding. Keys: {list(session.keys())}")
        
        # Check if setup is needed (no users exist)
        try:
            user_count = user_service.get_user_count_sync()
            if user_count == 0:
                # Skip for setup route, onboarding routes, and static files
                if (request.endpoint in ['auth.setup', 'static'] or 
                    (request.endpoint and (request.endpoint.startswith('static') or 
                                         request.endpoint.startswith('onboarding.')))):
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
            'onboarding.start',
            'onboarding.step',
            'onboarding.complete',
            'migration.check_migration_status',
            'migration.migration_wizard',
            'migration.configure_migration',
            'migration.execute_migration',
            'migration.run_migration',
            'migration.migration_success',
            'migration.dismiss_migration',
            'static'
        ]
        
        # Allow API and AJAX requests, and skip for static files and onboarding routes
        if (request.endpoint in allowed_endpoints or 
            (request.endpoint and (request.endpoint.startswith('static') or 
                                 request.endpoint.startswith('onboarding.')))):
            return
        
        # Check for migration needs (DISABLED - migration now manual only)
        # Automatic migration detection disabled to prevent redirect loops
        # Migration is now available only through admin panel -> /admin/migration
        pass
        
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
    
    try:
        from .metadata_routes import metadata_bp
        app.register_blueprint(metadata_bp)
    except ImportError as e:
        print(f"⚠️  Could not import metadata routes: {e}")
    
    try:
        from .migration_routes import migration_bp
        app.register_blueprint(migration_bp)
    except ImportError as e:
        print(f"⚠️  Could not import migration routes: {e}")
    
    try:
        from .advanced_migration_routes import migration_bp as advanced_migration_bp
        app.register_blueprint(advanced_migration_bp)
    except ImportError as e:
        print(f"⚠️  Could not import advanced migration routes: {e}")
    
    try:
        from .onboarding_system import onboarding_bp
        app.register_blueprint(onboarding_bp)
    except ImportError as e:
        print(f"⚠️  Could not import onboarding system: {e}")
    
    try:
        from .genre_routes import genres_bp
        app.register_blueprint(genres_bp, url_prefix='/genres')
    except ImportError as e:
        print(f"⚠️  Could not import genre routes: {e}")
    
    app.register_blueprint(bp)
    app.register_blueprint(auth, url_prefix='/auth')
    app.register_blueprint(admin, url_prefix='/admin')
    
    # Register debug admin routes
    try:
        from .debug_routes import bp as debug_admin_bp
        app.register_blueprint(debug_admin_bp)
        # Only show registration messages when verbose logging is enabled
        verbose_init = os.getenv('BIBLIOTHECA_VERBOSE_INIT', 'true').lower() == 'true'
        if verbose_init:
            print("✅ Debug admin routes registered")
    except Exception as e:
        print(f"⚠️  Could not import debug routes: {e}")
    
    # Register template context processors
    try:
        from .template_context import register_context_processors
        register_context_processors(app)
        # Only show registration messages when verbose logging is enabled
        verbose_init = os.getenv('BIBLIOTHECA_VERBOSE_INIT', 'true').lower() == 'true'
        if verbose_init:
            print("✅ Template context processors registered")
    except Exception as e:
        print(f"⚠️  Could not register template context processors: {e}")
    
    # Register API blueprints
    from .api.books import books_api
    from .api.reading_logs import reading_logs_api
    from .api.users import users_api
    app.register_blueprint(books_api)
    app.register_blueprint(reading_logs_api)
    app.register_blueprint(users_api)

    return app
