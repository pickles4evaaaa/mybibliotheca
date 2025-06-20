import os
import shutil
from datetime import datetime
from flask import Flask, session, request, jsonify, redirect, url_for
from flask_login import LoginManager
from flask_wtf.csrf import CSRFProtect
from sqlalchemy import inspect, text
from .models import db, User
from config import Config
import shutil
from datetime import datetime
from flask import Flask, session, request, jsonify
from flask_login import LoginManager
from flask_wtf.csrf import CSRFProtect
from sqlalchemy import inspect, text
from .models import db, User
from config import Config

login_manager = LoginManager()
csrf = CSRFProtect()

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

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

def backup_database(db_path):
    """Create a backup of the database before migration"""
    if not os.path.exists(db_path):
        return None
    
    # Create backups directory if it doesn't exist
    db_dir = os.path.dirname(db_path)
    backup_dir = os.path.join(db_dir, 'backups')
    os.makedirs(backup_dir, exist_ok=True)
    
    # Create backup filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    db_filename = os.path.basename(db_path)
    backup_path = os.path.join(backup_dir, f"{db_filename}.backup_{timestamp}")
    
    try:
        shutil.copy2(db_path, backup_path)
        print(f"✅ Database backup created: {backup_path}")
        return backup_path
    except Exception as e:
        print(f"⚠️  Failed to create database backup: {e}")
        return None

def check_if_migrations_needed(inspector):
    """Check if any migrations are needed before creating backup"""
    existing_tables = inspector.get_table_names()
    
    # Check if this is a fresh database
    if not existing_tables:
        return False, "fresh_database"
    
    migrations_needed = []
    
    # Check for missing user table
    if 'user' not in existing_tables:
        migrations_needed.append("user_table")
    
    # Check for missing columns in existing tables
    if 'book' in existing_tables:
        columns = [column['name'] for column in inspector.get_columns('book')]
        book_fields = ['user_id', 'description', 'published_date', 'page_count', 'categories', 
                      'publisher', 'language', 'average_rating', 'rating_count', 'created_at']
        missing_book_fields = [field for field in book_fields if field not in columns]
        if missing_book_fields:
            migrations_needed.append(f"book_columns: {missing_book_fields}")
    
    if 'user' in existing_tables:
        columns = [column['name'] for column in inspector.get_columns('user')]
        user_fields = ['failed_login_attempts', 'locked_until', 'last_login', 
                      'share_current_reading', 'share_reading_activity', 'share_library',
                      'reading_streak_offset']  # Add the streak offset field
        missing_user_fields = [field for field in user_fields if field not in columns]
        if missing_user_fields:
            migrations_needed.append(f"user_security_privacy: {missing_user_fields}")
    
    if 'reading_log' in existing_tables:
        columns = [column['name'] for column in inspector.get_columns('reading_log')]
        if 'user_id' not in columns or 'created_at' not in columns:
            migrations_needed.append("reading_log_fields")
    
    return len(migrations_needed) > 0, migrations_needed

def run_security_privacy_migration(inspector, db_engine):
    """Add security and privacy fields to user table"""
    if 'user' not in inspector.get_table_names():
        return  # User table doesn't exist yet
    
    try:
        columns = [column['name'] for column in inspector.get_columns('user')]
        
        # Security and privacy fields to add
        security_privacy_fields = [
            ('failed_login_attempts', 'INTEGER DEFAULT 0'),
            ('locked_until', 'DATETIME'),
            ('last_login', 'DATETIME'),
            ('share_current_reading', 'BOOLEAN DEFAULT 1'),
            ('share_reading_activity', 'BOOLEAN DEFAULT 1'),
            ('share_library', 'BOOLEAN DEFAULT 1')
        ]
        
        missing_fields = [field for field, _ in security_privacy_fields if field not in columns]
        
        if missing_fields:
            print(f"🔄 Adding security/privacy fields: {missing_fields}")
            with db_engine.connect() as conn:
                for field_name, field_def in security_privacy_fields:
                    if field_name not in columns:
                        conn.execute(text(f"ALTER TABLE user ADD COLUMN {field_name} {field_def}"))
                        print(f"✅ Added {field_name} to user table")
                conn.commit()
            print("✅ Security/privacy migration completed.")
        else:
            print("✅ Security/privacy fields already present.")
            
    except Exception as e:
        print(f"⚠️  Security/privacy migration failed: {e}")

def add_streak_offset_column(inspector, engine):
    """Add reading_streak_offset column to users table"""
    try:
        columns = [column['name'] for column in inspector.get_columns('user')]
        if 'reading_streak_offset' not in columns:
            print("🔄 Adding reading_streak_offset column to user table...")
            with engine.connect() as conn:
                conn.execute(text("ALTER TABLE user ADD COLUMN reading_streak_offset INTEGER DEFAULT 0"))
                conn.commit()
            print("✅ reading_streak_offset column added successfully")
        else:
            print("✅ reading_streak_offset column already exists")
    except Exception as e:
        print(f"⚠️  Error adding reading_streak_offset column: {e}")

def assign_existing_books_to_admin():
    """Assign existing books without user_id to the admin user"""
    try:
        # Import Book model here to avoid circular imports
        from .models import Book
        
        # Find the admin user
        admin_user = User.query.filter_by(is_admin=True).first()
        if not admin_user:
            print("⚠️  No admin user found, cannot assign books")
            return
        
        # Find books without user_id
        orphaned_books = Book.query.filter_by(user_id=None).all()
        if not orphaned_books:
            print("✅ No orphaned books found")
            return
            
        # Assign orphaned books to admin
        for book in orphaned_books:
            book.user_id = admin_user.id
            
        db.session.commit()
        print(f"✅ Assigned {len(orphaned_books)} orphaned books to admin user: {admin_user.username}")
        
    except Exception as e:
        print(f"⚠️  Failed to assign orphaned books to admin: {e}")
        db.session.rollback()

def create_development_admin():
    """Create development admin user from environment variables if specified."""
    dev_username = os.getenv('DEV_ADMIN_USERNAME')
    dev_password = os.getenv('DEV_ADMIN_PASSWORD')
    
    if dev_username and dev_password:
        # Check if admin user already exists
        existing_admin = User.query.filter_by(username=dev_username).first()
        if not existing_admin:
            print(f"🔧 Creating development admin user: {dev_username}")
            try:
                admin_user = User(
                    username=dev_username,
                    email=f"{dev_username}@localhost.dev",
                    is_admin=True,
                    is_active=True
                )
                admin_user.set_password(dev_password)
                db.session.add(admin_user)
                db.session.commit()
                print(f"✅ Development admin user '{dev_username}' created successfully!")
                return True
            except Exception as e:
                print(f"❌ Failed to create development admin user: {e}")
                db.session.rollback()
                return False
        else:
            print(f"ℹ️  Development admin user '{dev_username}' already exists")
            return True
    return False

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)
    app.config['SECRET_KEY'] = 'your-secret-key'

    # Initialize debug utilities
    from .debug_utils import setup_debug_logging, print_debug_banner, debug_middleware
    
    with app.app_context():
        setup_debug_logging()
        print_debug_banner()

    # Initialize extensions
    db.init_app(app)
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

    # DATABASE MIGRATION SECTION
    with app.app_context():
        db_path = app.config.get('SQLALCHEMY_DATABASE_URI', '').replace('sqlite:///', '')
        
        # Create inspector for checking database schema
        inspector = inspect(db.engine)
        
        # Check if migrations are needed BEFORE running any queries
        migrations_needed, migration_list = check_if_migrations_needed(inspector)
        
        if migrations_needed:
            print("🔄 Creating database backup before migration...")
            backup_path = backup_database(db_path)
            if backup_path:
                print(f"📁 Backup saved to: {backup_path}")
        else:
            print("✅ Database schema is up-to-date, no migrations needed")
        
        existing_tables = inspector.get_table_names()
        
        if not existing_tables:
            print("📚 Creating fresh database schema...")
            db.create_all()
            print("✅ Database schema created. Setup required on first visit.")
        else:
            print("📚 Database already exists...")
            print("✅ Tables present, checking for migrations...")
            
            # Check for user table (new in v2)
            if 'user' not in existing_tables:
                print("🔄 Adding user authentication tables...")
                db.create_all()
                print("✅ User tables created. Setup required on first visit.")
            else:
                # CRITICAL: Add streak offset column FIRST before any User queries
                add_streak_offset_column(inspector, db.engine)
                
                # Refresh inspector after adding column
                inspector = inspect(db.engine)
            
            # Run security/privacy field migration
            run_security_privacy_migration(inspector, db.engine)
            
            # Only assign orphaned books AFTER user table exists and columns are added
            if 'user' in inspector.get_table_names():
                try:
                    # Now it's safe to query User model
                    admin_users = User.query.filter_by(is_admin=True).count()
                    if admin_users > 0:
                        print("📚 Checking for orphaned books...")
                        assign_existing_books_to_admin()
                except Exception as e:
                    print(f"⚠️  Error checking for admin users: {e}")
            
            # Check for new columns in book table
            if 'book' in existing_tables:
                try:
                    columns = [column['name'] for column in inspector.get_columns('book')]
                    
                    # Check for user_id column (critical for v2)
                    if 'user_id' not in columns:
                        print("🔄 Adding user_id to book table...")
                        with db.engine.connect() as conn:
                            trans = conn.begin()
                            try:
                                conn.execute(text("ALTER TABLE book ADD COLUMN user_id INTEGER"))
                                trans.commit()
                                print("✅ user_id column added to book table.")
                            except Exception as e:
                                trans.rollback()
                                raise e
                        
                        # Assign books to admin after adding user_id column (only if admin exists)
                        try:
                            if User.query.filter_by(is_admin=True).count() > 0:
                                assign_existing_books_to_admin()
                        except Exception as e:
                            print(f"⚠️  Error assigning books to admin: {e}")
                    
                    # Check for other missing columns
                    new_columns = ['description', 'published_date', 'page_count', 'categories', 
                                 'publisher', 'language', 'average_rating', 'rating_count', 'created_at']
                    missing_columns = [col for col in new_columns if col not in columns]
                    
                    if missing_columns:
                        print(f"🔄 Adding missing book columns: {missing_columns}")
                        with db.engine.connect() as conn:
                            trans = conn.begin()
                            try:
                                for col_name in missing_columns:
                                    if col_name in ['page_count', 'rating_count']:
                                        conn.execute(text(f"ALTER TABLE book ADD COLUMN {col_name} INTEGER"))
                                    elif col_name == 'average_rating':
                                        conn.execute(text(f"ALTER TABLE book ADD COLUMN {col_name} REAL"))
                                    elif col_name in ['categories', 'publisher']:
                                        conn.execute(text(f"ALTER TABLE book ADD COLUMN {col_name} VARCHAR(500)"))
                                    elif col_name == 'language':
                                        conn.execute(text(f"ALTER TABLE book ADD COLUMN {col_name} VARCHAR(10)"))
                                    elif col_name == 'published_date':
                                        conn.execute(text(f"ALTER TABLE book ADD COLUMN {col_name} VARCHAR(50)"))
                                    elif col_name == 'created_at':
                                        conn.execute(text(f"ALTER TABLE book ADD COLUMN {col_name} DATETIME"))
                                    else:  # description
                                        conn.execute(text(f"ALTER TABLE book ADD COLUMN {col_name} TEXT"))
                                trans.commit()
                                print("✅ Book schema migration completed.")
                            except Exception as e:
                                trans.rollback()
                                raise e
                        
                except Exception as e:
                    print(f"⚠️  Book schema migration failed: {e}")
            
            # Check for reading_log table updates
            if 'reading_log' in existing_tables:
                try:
                    columns = [column['name'] for column in inspector.get_columns('reading_log')]
                    missing_reading_log_columns = []
                    
                    if 'user_id' not in columns:
                        missing_reading_log_columns.append('user_id')
                    if 'created_at' not in columns:
                        missing_reading_log_columns.append('created_at')
                    
                    if missing_reading_log_columns:
                        print(f"🔄 Adding missing reading_log columns: {missing_reading_log_columns}")
                        with db.engine.connect() as conn:
                            if 'user_id' in missing_reading_log_columns:
                                conn.execute(text("ALTER TABLE reading_log ADD COLUMN user_id INTEGER"))
                            if 'created_at' in missing_reading_log_columns:
                                conn.execute(text("ALTER TABLE reading_log ADD COLUMN created_at DATETIME"))
                            conn.commit()
                        print("✅ reading_log table updated.")
                        
                        # Assign reading logs to admin user if needed
                        if 'user_id' in missing_reading_log_columns:
                            try:
                                admin_user = User.query.filter_by(is_admin=True).first()
                                if admin_user:
                                    from .models import ReadingLog
                                    unassigned_logs = ReadingLog.query.filter_by(user_id=None).all()
                                    if unassigned_logs:
                                        print(f"🔄 Assigning {len(unassigned_logs)} reading logs to admin user...")
                                        for log in unassigned_logs:
                                            log.user_id = admin_user.id
                                        db.session.commit()
                                        print("✅ Reading logs assigned to admin user.")
                            except Exception as e:
                                print(f"⚠️  Reading log migration failed: {e}")
                        
                except Exception as e:
                    print(f"⚠️  Reading log migration failed: {e}")
        
        print("🎉 Database migration completed successfully!")
        
        # Development auto-setup: Create admin user from environment variables
        create_development_admin()

    # Add middleware to check for setup and forced password changes
    @app.before_request
    def check_setup_and_password_requirements():
        from flask import request, redirect, url_for
        from flask_login import current_user, login_user
        from .debug_utils import debug_middleware
        from .api_auth import validate_api_token
        
        # Run debug middleware if enabled
        debug_middleware()
        
        # Check if setup is needed (no users exist)
        if User.query.count() == 0:
            # Skip for setup route and static files
            if request.endpoint in ['auth.setup', 'static'] or (request.endpoint and request.endpoint.startswith('static')):
                return
            # Redirect to setup page
            return redirect(url_for('auth.setup'))
        
        # For API endpoints, skip the session-based authentication checks
        if request.path.startswith('/api/'):
            print("DEBUG: API request detected, skipping session checks")
            # API authentication is handled by the @api_token_required decorator
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
