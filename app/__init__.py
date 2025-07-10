"""
Flask application factory for Kuzu-based Bibliotheca.

This version completely removes SQLite dependency and uses Kuzu as the sole data store.
"""

import os
import time
from datetime import datetime
from pathlib import Path
from flask import Flask, session, request, jsonify, redirect, url_for
from flask_login import LoginManager
from flask_wtf.csrf import CSRFProtect
from flask_session import Session
from config import Config

login_manager = LoginManager()
csrf = CSRFProtect()
sess = Session()

# Global flag to track template creation failures and prevent crash loops
_template_creation_disabled = True  # Disabled due to KuzuDB segfault
_template_creation_failures = 0
_template_creation_last_attempt = None

@login_manager.user_loader
def load_user(user_id):
    """Load user from Kuzu via the user service."""
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
                    
                    if admin_user:
                        print(f"✅ Development admin user '{dev_username}' created successfully!")
                        print(f"🔍 Admin user ID: {admin_user.id}, Password hash: {admin_user.password_hash[:20] if admin_user.password_hash else 'None'}...")
                        return True
                    else:
                        print(f"❌ Failed to create development admin user: user_service.create_user_sync returned None")
                        return False
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

# Global variable to track template initialization attempts
_template_init_last_attempt = 0

def _initialize_default_templates():
    """Initialize default import templates for Goodreads and StoryGraph if they don't exist."""
    try:
        import time
        from datetime import datetime
        from .domain.models import ImportMappingTemplate
        from .services import import_mapping_service
        
        global _template_creation_disabled, _template_creation_failures, _template_creation_last_attempt
        
        # Check if verbose logging is enabled
        verbose_init = os.getenv('BIBLIOTHECA_VERBOSE_INIT', 'true').lower() == 'true'
        
        # Circuit breaker: disable template creation if too many failures
        if _template_creation_disabled:
            if verbose_init:
                print("🚫 Template creation disabled due to KuzuDB segmentation fault")
                print("   The application will continue without default import templates")
            return
        
        # Check if template creation failed recently
        if _template_creation_last_attempt and _template_creation_failures >= 3:
            time_since_last = time.time() - _template_creation_last_attempt
            if time_since_last < 300:  # Don't retry for 5 minutes after 3 failures
                if verbose_init:
                    print(f"⏳ Skipping template initialization - {_template_creation_failures} recent failures")
                return
            else:
                # Reset failures after cooldown period
                _template_creation_failures = 0
        
        _template_creation_last_attempt = time.time()
        
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
            try:
                if verbose_init:
                    print("🔄 Creating Goodreads template...")
                    
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
                
                # Attempt to create template with error tracking
                created_template = import_mapping_service.create_template_sync(goodreads_template)
                if created_template:
                    if verbose_init:
                        print("✅ Created Goodreads default template")
                    # Reset failure count on success
                    _template_creation_failures = 0
                else:
                    if verbose_init:
                        print("⚠️  Failed to create Goodreads default template")
                    _template_creation_failures += 1
                    # Disable template creation after 5 failures
                    if _template_creation_failures >= 5:
                        _template_creation_disabled = True
                        if verbose_init:
                            print("🚫 Template creation disabled after 5 failures")
                
            except Exception as e:
                if verbose_init:
                    print(f"⚠️  Error creating Goodreads template: {e}")
                _template_creation_failures += 1
                # Disable template creation after 5 failures
                if _template_creation_failures >= 5:
                    _template_creation_disabled = True
                    if verbose_init:
                        print("🚫 Template creation disabled after 5 failures")
                # Continue without crashing the application
        
        # Create StoryGraph template
        if not storygraph_template:
            try:
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
                
                # Attempt to create template with error tracking
                created_template = import_mapping_service.create_template_sync(storygraph_template)
                if created_template:
                    if verbose_init:
                        print("✅ Created StoryGraph default template")
                    # Reset failure count on success
                    _template_creation_failures = 0
                else:
                    if verbose_init:
                        print("⚠️  Failed to create StoryGraph default template")
                    _template_creation_failures += 1
                    # Disable template creation after 5 failures
                    if _template_creation_failures >= 5:
                        _template_creation_disabled = True
                        if verbose_init:
                            print("🚫 Template creation disabled after 5 failures")
                
            except Exception as e:
                if verbose_init:
                    print(f"⚠️  Error creating StoryGraph template: {e}")
                _template_creation_failures += 1
                # Disable template creation after 5 failures
                if _template_creation_failures >= 5:
                    _template_creation_disabled = True
                    if verbose_init:
                        print("🚫 Template creation disabled after 5 failures")
                # Continue without crashing the application
            
        if verbose_init:
            print("🎉 Default import templates initialized successfully!")
        
    except Exception as e:
        verbose_init = os.getenv('BIBLIOTHECA_VERBOSE_INIT', 'true').lower() == 'true'
        if verbose_init:
            print(f"⚠️  Warning: Could not initialize default templates: {e}")
        # Don't fail app startup if templates can't be created


def create_app():
    import os
    
    # Ensure static folder exists and is correctly configured
    static_folder = os.path.join(os.path.dirname(__file__), 'static')
    if not os.path.exists(static_folder):
        os.makedirs(static_folder, exist_ok=True)
    
    # Disable Flask's default static file handling completely
    app = Flask(__name__, static_folder=None, static_url_path=None)
    app.config.from_object(Config)
    
    # Explicitly set the secret key for Flask-Session compatibility
    # Must be set before Flask-Session initialization
    app.secret_key = app.config['SECRET_KEY']
    
    # Verify the secret key is set
    if not app.secret_key:
        raise RuntimeError("SECRET_KEY must be set in environment or config")

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
    login_manager.login_view = 'auth.login'  # type: ignore
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
        # For now, use a default site name since settings service methods don't exist yet
        site_name = 'MyBibliotheca'
        return dict(site_name=site_name)

    @app.context_processor
    def inject_theme_preference():
        """Make theme preference available in all templates."""
        from flask_login import current_user
        from flask import session
        theme = 'light'  # Default theme
        try:
            if current_user.is_authenticated:
                # For now, just use session-based theme preference
                # TODO: Implement user settings in KuzuUserService
                theme = session.get('theme', 'light')
            else:
                # For non-authenticated users, check session or default to light
                theme = session.get('theme', 'light')
        except Exception:
            # In case of error, fallback to session or default
            theme = session.get('theme', 'light')
        return dict(current_theme=theme)

    # CSRF error handler
    @app.errorhandler(400)
    def handle_csrf_error(e):
        """Handle CSRF errors with user-friendly messages."""
        if "CSRF" in str(e) or "csrf" in str(e.description):
            # Check if this is an AJAX request
            if request.is_json or 'application/json' in request.headers.get('Content-Type', ''):
                from flask_wtf.csrf import generate_csrf
                return jsonify({
                    'error': 'CSRF token missing or invalid',
                    'message': 'Please refresh the page and try again. Include X-CSRFToken header for API requests.',
                    'csrf_token': generate_csrf()
                }), 400
            else:
                # For web requests, handle differently based on route
                from flask import flash, redirect, url_for
                
                # Special handling for onboarding routes
                if request.endpoint and request.endpoint.startswith('onboarding.'):
                    flash('Security token expired. The page will be refreshed with a new token.', 'warning')
                    # Redirect to the same onboarding step to refresh the form
                    if request.view_args and 'step' in request.view_args:
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

    # KUZU DATABASE INITIALIZATION
    with app.app_context():
        # Use environment variable to control verbose logging across multiple workers
        verbose_init = os.getenv('BIBLIOTHECA_VERBOSE_INIT', 'true').lower() == 'true'
        
        if verbose_init:
            print("🚀 Initializing Kuzu-based MyBibliotheca...")
            print(f"🔍 App initialization - Container: {os.getenv('HOSTNAME', 'unknown')}")
            print(f"🔍 App initialization - Process ID: {os.getpid()}")
            print(f"🔍 App initialization - Database path: {os.getenv('KUZU_DB_PATH', 'default')}")
        
        # Test Kuzu connection
        try:
            from .infrastructure.kuzu_graph import get_graph_storage
            storage = get_graph_storage()
            # Simple connection test
            if verbose_init:
                print("✅ Kuzu connection successful")
                
                # Log database state at app startup
                try:
                    user_count = storage.count_nodes('User')
                    book_count = storage.count_nodes('Book')
                    owns_count = storage.count_relationships('OWNS')
                    print(f"📊 Database state at startup:")
                    print(f"📊   - Users: {user_count}")
                    print(f"📊   - Books: {book_count}")
                    print(f"📊   - OWNS relationships: {owns_count}")
                except Exception as count_e:
                    print(f"⚠️ Could not get database counts at startup: {count_e}")
            
            # Initialize services and attach to app using Kuzu service instances
            from .services import (
                book_service, user_service, reading_log_service,
                custom_field_service, import_mapping_service, direct_import_service
            )
            
            try:
                app.book_service = book_service  # type: ignore
                app.user_service = user_service  # type: ignore
                app.reading_log_service = reading_log_service  # type: ignore
                app.custom_field_service = custom_field_service  # type: ignore
                app.import_mapping_service = import_mapping_service  # type: ignore
                app.direct_import_service = direct_import_service  # type: ignore
                if verbose_init:
                    print("📦 Kuzu services initialized successfully")
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
            print(f"❌ Kuzu connection failed: {e}")
            print("🔧 Make sure KuzuDB is running and accessible")
            import traceback
            traceback.print_exc()
        
        # Development mode - skip auto admin creation, use setup page instead
        if verbose_init:
            print("🔧 Development mode: Use the setup page to create your admin user")
            print("🎉 Kuzu-based initialization completed successfully!")
        
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
                    form_csrf = request.form.get('csrf_token')
                    session_csrf = session.get('csrf_token')
                    debug_auth(f"Form CSRF: {form_csrf[:10] if form_csrf else 'None'}...")
                    debug_auth(f"Session CSRF: {session_csrf[:10] if session_csrf else 'None'}...")
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
            debug_auth(f"Before request user count check: {user_count} for endpoint: {request.endpoint}")
            
            if user_count == 0:
                # Skip for setup route, onboarding routes, and static files
                if (request.endpoint in ['auth.setup', 'static'] or 
                    (request.endpoint and (request.endpoint.startswith('static') or 
                                         request.endpoint.startswith('onboarding.')))):
                    debug_auth(f"Skipping setup redirect for allowed endpoint: {request.endpoint}")
                    return
                # Redirect to setup page
                debug_auth(f"No users found, redirecting to setup from: {request.endpoint}")
                return redirect(url_for('auth.setup'))
            else:
                debug_auth(f"Users exist ({user_count}), allowing access to: {request.endpoint}")
        except Exception as e:
            debug_auth(f"Error checking user count: {e}")
            print(f"Error checking user count: {e}")
            # If we can't check users, be more conservative about redirecting
            if request.endpoint not in ['auth.setup', 'static', 'auth.login']:
                debug_auth(f"User count check failed, redirecting to setup from: {request.endpoint}")
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

    # Add explicit static file serving for production (gunicorn doesn't serve static files by default)
    @app.route('/static/<path:filename>')
    def serve_static(filename):
        """Serve static files in production mode."""
        import os
        from flask import send_from_directory
        # Use the explicit static folder path since we disabled Flask's static handling
        # Point to the volume-mounted static directory
        static_dir = '/app/static'
        print(f"🔧 [STATIC] Serving {filename} from {static_dir}")
        return send_from_directory(static_dir, filename)

    # Register application routes via modular blueprints
    from .routes import register_blueprints
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
    
    # Note: Genre routes temporarily disabled due to missing category management methods
    # try:
    #     from .routes.genre_routes import genres_bp
    #     app.register_blueprint(genres_bp, url_prefix='/genres')
    # except ImportError as e:
    #     print(f"⚠️  Could not import genre routes: {e}")
    
    # Register main and modular routes
    register_blueprints(app)
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

    # Add shutdown logging
    import atexit
    from datetime import datetime
    from pathlib import Path
    
    def shutdown_handler():
        """Log database state and properly close KuzuDB connection at application shutdown."""
        try:
            print(f"🛑 [APP_SHUTDOWN] Application shutting down - Container: {os.getenv('HOSTNAME', 'unknown')}")
            print(f"🛑 [APP_SHUTDOWN] Process ID: {os.getpid()}")
            print(f"🛑 [APP_SHUTDOWN] Shutdown time: {datetime.now()}")
            
            # Try to get final database state
            from .infrastructure.kuzu_graph import get_graph_storage
            storage = get_graph_storage()
            
            user_count = storage.count_nodes('User')
            book_count = storage.count_nodes('Book')
            owns_count = storage.count_relationships('OWNS')
            
            print(f"🛑 [APP_SHUTDOWN] Final database state:")
            print(f"🛑 [APP_SHUTDOWN]   - Users: {user_count}")
            print(f"🛑 [APP_SHUTDOWN]   - Books: {book_count}")
            print(f"🛑 [APP_SHUTDOWN]   - OWNS relationships: {owns_count}")
            
            # Check database files
            db_path = Path(os.getenv('KUZU_DB_PATH', '/app/data/kuzu'))
            if db_path.exists():
                files = list(db_path.glob("*"))
                total_size = sum(f.stat().st_size for f in files if f.is_file())
                print(f"🛑 [APP_SHUTDOWN] Database files: {len(files)} files, {total_size} bytes total")
            
            # 🔥 CRITICAL FIX: Properly close the KuzuDB connection
            print(f"🛑 [APP_SHUTDOWN] Closing KuzuDB connection to ensure data persistence...")
            storage.connection.disconnect()  # Call disconnect on the KuzuGraphDB connection, not storage
            print(f"🛑 [APP_SHUTDOWN] ✅ KuzuDB connection closed successfully")
            
        except Exception as e:
            print(f"🛑 [APP_SHUTDOWN] Error during shutdown logging: {e}")
            # Even if logging fails, try to close the connection
            try:
                from .infrastructure.kuzu_graph import get_graph_storage
                storage = get_graph_storage()
                storage.connection.disconnect()  # Call disconnect on the KuzuGraphDB connection, not storage
                print(f"🛑 [APP_SHUTDOWN] ✅ KuzuDB connection closed after error")
            except Exception as close_error:
                print(f"🛑 [APP_SHUTDOWN] ❌ Failed to close KuzuDB connection: {close_error}")
    
    atexit.register(shutdown_handler)

    return app
