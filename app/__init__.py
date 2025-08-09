"""
Flask application factory for Kuzu-based Bibliotheca.

This version completely removes SQLite dependency and uses Kuzu as the sole data store.
"""

import os
import sys
import time
import atexit
import logging
from datetime import datetime
from pathlib import Path
from flask import Flask, session, request, jsonify, redirect, url_for
from flask_login import LoginManager
from flask_wtf.csrf import CSRFProtect
from flask_session import Session
from config import Config

logger = logging.getLogger(__name__)

login_manager = LoginManager()
csrf = CSRFProtect()
sess = Session()

# Global flag to track template creation failures and prevent crash loops
_template_creation_disabled = True  # Disabled by default to prevent crashes
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


    if dev_username and dev_password:
        from .services import user_service
        from .domain.models import User
        from werkzeug.security import generate_password_hash

        # Check if admin user already exists
        try:
            existing_admin = user_service.get_user_by_username_sync(dev_username)
            if existing_admin:
                print(f"� Admin user {dev_username} already exists, skipping creation")
                return True
            else:
                print(f"�🔧 Creating development admin user: {dev_username}")
                try:
                    # Hash the password first
                    password_hash = generate_password_hash(dev_password)

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
                        return True
                    else:
                        return False
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    return False
        except Exception as e:
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
        verbose_init = os.getenv('MYBIBLIOTHECA_VERBOSE_INIT', 'true').lower() == 'true'

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
                        "Bookshelves": {"action": "map_existing", "target_field": "custom_global_bookshelves"},
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
                    print(f"Template creation failed: {e}")
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
                    print(f"Template creation failed: {e}")
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
        verbose_init = os.getenv('MYBIBLIOTHECA_VERBOSE_INIT', 'true').lower() == 'true'
        if verbose_init:
            print("✅ Template initialization completed")
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

    # Suppress asyncio debug logging unless explicitly needed
    import logging
    logging.getLogger('asyncio').setLevel(logging.INFO)

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

    # Add custom template filters
    @app.template_filter('basename')
    def basename_filter(path):
        """Extract the basename from a file path."""
        import os
        if not path:
            return ''
        return os.path.basename(str(path))

    @app.template_filter('from_json')
    def from_json_filter(json_string):
        """Parse JSON string into Python object."""
        import json
        if not json_string:
            return []
        try:
            return json.loads(json_string)
        except (json.JSONDecodeError, TypeError):
            return []

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
        verbose_init = os.getenv('MYBIBLIOTHECA_VERBOSE_INIT', 'true').lower() == 'true'

        if verbose_init:
            print("🚀 Initializing Kuzu-based MyBibliotheca...")

        # Test Kuzu connection (only if not in gunicorn worker spawn)
        # Skip KuzuDB connection test during import/fork to avoid lock conflicts
        kuzu_init_attempted = False
        try:
            from .utils.kuzu_migration_helper import safe_execute_query
            # Simple connection test using safe method
            test_result = safe_execute_query("RETURN 1 AS test", {})
            kuzu_init_attempted = True
            if verbose_init:
                print("✅ Kuzu connection successful")

                # Log database state at app startup
                try:
                    user_result = safe_execute_query("MATCH (u:User) RETURN COUNT(u) AS count", {})
                    book_result = safe_execute_query("MATCH (b:Book) RETURN COUNT(b) AS count", {})
                    owns_result = safe_execute_query("MATCH ()-[r:OWNS]->() RETURN COUNT(r) AS count", {})

                    user_count = 0
                    if user_result and user_result.has_next():
                        row = user_result.get_next()
                        user_count = row[0] if row else 0

                    book_count = 0
                    if book_result and book_result.has_next():
                        row = book_result.get_next()
                        book_count = row[0] if row else 0

                    owns_count = 0
                    if owns_result and owns_result.has_next():
                        row = owns_result.get_next()
                        owns_count = row[0] if row else 0

                    if verbose_init:
                        print(f"📊 Database state: Users: {user_count}, Books: {book_count}, Ownership relationships: {owns_count}")
                except Exception as count_e:
                    if verbose_init:
                        print(f"Error counting nodes: {count_e}")

        except Exception as e:
            if "Could not set lock on file" in str(e):
                if verbose_init:
                    print("⏳ KuzuDB connection deferred - will initialize on first request")
                kuzu_init_attempted = False  # Mark as not attempted due to lock conflict
            else:
                print("🔧 Make sure KuzuDB is running and accessible")
                if verbose_init:
                    import traceback
                    traceback.print_exc()

        # Only initialize services if KuzuDB connection was successful
        if kuzu_init_attempted:
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

                # Clear restart flag if it exists (after successful restore)
                try:
                    from .services.simple_backup_service import get_simple_backup_service
                    backup_service = get_simple_backup_service()
                    if backup_service.check_restart_required():
                        backup_service.clear_restart_flag()
                        if verbose_init:
                            print("🔄 Restart flag cleared after successful restoration")
                except Exception as restart_e:
                    if verbose_init:
                        print(f"Note: Could not check/clear restart flag: {restart_e}")

            except Exception as e:
                if verbose_init:
                    print(f"Error initializing services: {e}")

            # Initialize default import templates
            if verbose_init:
                _initialize_default_templates()
            else:
                # Still run template initialization, just silently
                try:
                    _initialize_default_templates()
                except Exception:
                    pass  # Fail silently for worker processes
        else:
            if verbose_init:
                print("⚠️ Services initialization deferred - will initialize on first request")

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

        static_dir = os.path.join(os.path.dirname(__file__), 'static')

        return send_from_directory(static_dir, filename)

    # Add routes to serve user data files from data directory
    @app.route('/covers/<path:filename>')
    def serve_covers(filename):
        """Serve cover images from data directory."""
        import os
        from flask import send_from_directory

        # Check for Docker (production) or local development
        docker_data_dir = '/data/covers'
        local_data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'covers')

        if os.path.exists(docker_data_dir):
            covers_dir = docker_data_dir
        else:
            covers_dir = local_data_dir

        return send_from_directory(covers_dir, filename)

    @app.route('/uploads/<path:filename>')
    def serve_uploads(filename):
        """Serve uploaded files from data directory."""
        import os
        from flask import send_from_directory

        # Check for Docker (production) or local development
        docker_data_dir = '/data/uploads'
        local_data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'uploads')

        if os.path.exists(docker_data_dir):
            uploads_dir = docker_data_dir
        else:
            uploads_dir = local_data_dir

        return send_from_directory(uploads_dir, filename)

    # Register application routes via modular blueprints
    from .routes import register_blueprints
    from .auth import auth
    from .admin import admin
    try:
        from .location_routes import bp as locations_bp
        app.register_blueprint(locations_bp)
    except ImportError as e:
        print(f"Could not import locations blueprint: {e}")

    try:
        from .metadata_routes import metadata_bp
        app.register_blueprint(metadata_bp)
    except ImportError as e:
        print(f"Could not import metadata blueprint: {e}")

    try:
        from .migration_routes import migration_bp
        app.register_blueprint(migration_bp)
    except ImportError as e:
        print(f"Could not import migration blueprint: {e}")

    try:
        from .advanced_migration_routes import migration_bp as advanced_migration_bp
        app.register_blueprint(advanced_migration_bp)
    except ImportError as e:
        print(f"Could not import advanced migration blueprint: {e}")

    try:
        from .onboarding_system import onboarding_bp
        app.register_blueprint(onboarding_bp)
    except ImportError as e:
        print(f"Could not import onboarding blueprint: {e}")

    # Note: Genre routes are now registered via register_blueprints() in routes/__init__.py

    # Register main and modular routes
    register_blueprints(app)
    app.register_blueprint(auth, url_prefix='/auth')
    app.register_blueprint(admin, url_prefix='/admin')

    # Register simple backup routes
    try:
        from .routes.simple_backup_routes import simple_backup_bp
        app.register_blueprint(simple_backup_bp)
        verbose_init = os.getenv('MYBIBLIOTHECA_VERBOSE_INIT', 'true').lower() == 'true'
        if verbose_init:
            print("✅ Simple backup routes registered")
    except Exception as e:
        print(f"Could not register simple backup routes: {e}")

    # Simple backup routes removed - viz comparison routes removed

    # Register debug admin routes
    try:
        from .debug_routes import bp as debug_admin_bp
        app.register_blueprint(debug_admin_bp)
        # Only show registration messages when verbose logging is enabled
        verbose_init = os.getenv('MYBIBLIOTHECA_VERBOSE_INIT', 'true').lower() == 'true'
        if verbose_init:
            print("✅ Debug admin routes registered")
    except Exception as e:
        print(f"Could not register debug admin routes: {e}")

    # Register template context processors
    try:
        from .template_context import register_context_processors
        register_context_processors(app)
        # Only show registration messages when verbose logging is enabled
        verbose_init = os.getenv('MYBIBLIOTHECA_VERBOSE_INIT', 'true').lower() == 'true'
        if verbose_init:
            print("✅ Template context processors registered")
    except Exception as e:
        print(f"Could not register template context processors: {e}")

    # Register API blueprints
    from .api.books import books_api
    from .api.reading_logs import reading_logs_api
    from .api.users import users_api
    app.register_blueprint(books_api)
    app.register_blueprint(reading_logs_api)
    app.register_blueprint(users_api)

    # Add shutdown logging
    from datetime import datetime
    from pathlib import Path

    def shutdown_handler():
        """Log database state and properly close KuzuDB connection at application shutdown."""
        try:
            print(f"🛑 [APP_SHUTDOWN] Application shutting down - Container: {os.getenv('HOSTNAME', 'unknown')}")
            print(f"🛑 [APP_SHUTDOWN] Process ID: {os.getpid()}")
            print(f"🛑 [APP_SHUTDOWN] Shutdown time: {datetime.now()}")

            # Try to get final database state using safe methods
            from .utils.kuzu_migration_helper import safe_execute_query

            try:
                user_result = safe_execute_query("MATCH (u:User) RETURN COUNT(u) AS count", {})
                book_result = safe_execute_query("MATCH (b:Book) RETURN COUNT(b) AS count", {})
                owns_result = safe_execute_query("MATCH ()-[r:OWNS]->() RETURN COUNT(r) AS count", {})

                user_count = user_result[0].get('count', 0) if user_result else 0
                book_count = book_result[0].get('count', 0) if book_result else 0
                owns_count = owns_result[0].get('count', 0) if owns_result else 0
            except Exception:
                user_count = book_count = owns_count = "unknown"

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

            # 🔥 CRITICAL FIX: Close KuzuDB connections properly
            print(f"🛑 [APP_SHUTDOWN] Closing KuzuDB connections to ensure data persistence...")
            from .utils.safe_kuzu_manager import get_safe_kuzu_manager
            manager = get_safe_kuzu_manager()
            stale_count = manager.cleanup_stale_connections(max_age_minutes=0)  # Clean up all
            print(f"🛑 [APP_SHUTDOWN] ✅ KuzuDB connections closed ({stale_count} cleaned up)")

        except Exception as e:
            print(f"🛑 [APP_SHUTDOWN] Error during shutdown logging: {e}")
            # Even if logging fails, try to close connections
            try:
                from .utils.safe_kuzu_manager import get_safe_kuzu_manager
                manager = get_safe_kuzu_manager()
                manager.cleanup_stale_connections(max_age_minutes=0)
                print(f"🛑 [APP_SHUTDOWN] ✅ KuzuDB connections closed after error")
            except Exception as close_error:
                print(f"🛑 [APP_SHUTDOWN] ❌ Failed to close KuzuDB connection: {close_error}")

    # Register shutdown handler for both normal exit AND signal termination
    atexit.register(shutdown_handler)

    # 🔥 CRITICAL FIX: Add signal handlers for Docker SIGTERM/SIGINT
    import signal

    def signal_shutdown_handler(signum, frame):
        """Handle Docker container shutdown signals (SIGTERM, SIGINT)."""
        print(f"🛑 [SIGNAL_SHUTDOWN] Received signal {signum}")
        shutdown_handler()
        # Exit gracefully after handling shutdown
        sys.exit(0)

    # Register signal handlers for Docker container termination
    signal.signal(signal.SIGTERM, signal_shutdown_handler)  # Docker stop
    signal.signal(signal.SIGINT, signal_shutdown_handler)   # Ctrl+C

    # Only log signal handler registration in debug mode
    debug_mode = os.getenv('KUZU_DEBUG', 'false').lower() == 'true'
    if debug_mode:
        logger.debug("Signal handlers registered for SIGTERM/SIGINT - KuzuDB persistence fixed!")

    return app
