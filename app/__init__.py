import os
import shutil
from datetime import datetime
from flask import Flask, session
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
                      'share_current_reading', 'share_reading_activity', 'share_library']
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

def create_default_admin_if_needed():
    """Create default admin user if no users exist"""
    try:
        if User.query.count() == 0:
            admin_username = os.environ.get('ADMIN_USERNAME', 'admin')
            admin_email = os.environ.get('ADMIN_EMAIL', 'admin@bibliotheca.local')
            admin_password = os.environ.get('ADMIN_PASSWORD', 'G7#xP@9zL!qR2')
            
            print(f"Creating default admin with username: {admin_username}, email: {admin_email}")
            
            admin_user = User(
                username=admin_username,
                email=admin_email,
                is_admin=True,
                created_at=datetime.now()
            )
            
            # Debug password validation
            if not User.is_password_strong(admin_password):
                print("Password validation failed for default admin password.")
                raise ValueError("Default admin password does not meet security requirements.")
            
            admin_user.set_password(admin_password)
            db.session.add(admin_user)
            db.session.commit()
            
            print(f"✅ Created default admin user: {admin_username}")
            print(f"⚠️  Default password: {admin_password}")
            print("🔒 Please change the admin password after setup!")
            return admin_user
        else:
            print("✅ Users already exist, skipping admin creation.")
            return None
    except Exception as e:
        print(f"⚠️  Failed to create default admin: {e}")
        return None

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
    csrf.init_app(app)

    # Handle database migrations with backup
    with app.app_context():
        # Get database path from config (directory already ensured by config.py)
        db_path = app.config.get('DATABASE_PATH')
        
        inspector = inspect(db.engine)
        
        # Check if migrations are needed before creating backup
        migrations_needed, migration_details = check_if_migrations_needed(inspector)
        
        if migrations_needed:
            print(f"🔄 Migrations needed: {migration_details}")
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
            create_default_admin_if_needed()
        else:
            print("✅ Tables present, checking for migrations...")
            
            # Check for user table (new in v2)
            if 'user' not in existing_tables:
                print("🔄 Adding user authentication tables...")
                db.create_all()
                create_default_admin_if_needed()
                print("✅ User tables created.")
            
            # Run security/privacy field migration
            run_security_privacy_migration(inspector, db.engine)
            
            # Check for new columns in book table
            if 'book' in existing_tables:
                try:
                    columns = [column['name'] for column in inspector.get_columns('book')]
                    
                    # Check for user_id column (critical for v2)
                    if 'user_id' not in columns:
                        print("🔄 Adding user_id to book table...")
                        with db.engine.connect() as conn:
                            # Add user_id column as nullable first
                            conn.execute(text("ALTER TABLE book ADD COLUMN user_id INTEGER"))
                            # We'll handle data migration separately
                            conn.commit()
                        print("✅ user_id column added to book table.")
                        
                        # Assign books to default admin if no users exist
                        if User.query.count() == 0:
                            admin_user = create_default_admin_if_needed()
                            if admin_user:
                                # Import here to avoid circular import
                                from .models import Book
                                unassigned_books = Book.query.filter_by(user_id=None).all()
                                if unassigned_books:
                                    print(f"🔄 Assigning {len(unassigned_books)} books to admin user...")
                                    for book in unassigned_books:
                                        book.user_id = admin_user.id
                                    db.session.commit()
                                    print("✅ Books assigned to admin user.")
                    
                    # Check for other missing columns
                    new_columns = ['description', 'published_date', 'page_count', 'categories', 
                                 'publisher', 'language', 'average_rating', 'rating_count', 'created_at']
                    missing_columns = [col for col in new_columns if col not in columns]
                    
                    if missing_columns:
                        print(f"🔄 Adding missing book columns: {missing_columns}")
                        with db.engine.connect() as conn:
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
                            conn.commit()
                        print("✅ Book schema migration completed.")
                        
                except Exception as e:
                    print(f"⚠️  Book schema migration failed: {e}")
                    print("📚 Creating fresh database schema...")
                    db.create_all()
            
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
        
        print("🎉 Database migration completed successfully!")

    # Add middleware to check for forced password changes
    @app.before_request
    def check_password_change_required():
        from flask import request, redirect, url_for
        from flask_login import current_user
        from .debug_utils import debug_middleware
        
        # Run debug middleware if enabled
        debug_middleware()
        
        # Skip if user is not authenticated
        if not current_user.is_authenticated:
            return
        
        # Skip for certain routes to avoid redirect loops
        allowed_endpoints = [
            'auth.forced_password_change',
            'auth.logout',
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
    app.register_blueprint(bp)
    app.register_blueprint(auth, url_prefix='/auth')
    app.register_blueprint(admin, url_prefix='/admin')

    return app
