import os
import secrets
import platform
from dotenv import load_dotenv

# Load environment variables from .env file(s)
# 1) Project root .env
load_dotenv()

basedir = os.path.abspath(os.path.dirname(__file__))

def ensure_data_directory():
    """Ensure data directory exists with proper permissions for both Docker and standalone (cross-platform)"""
    data_dir = os.path.join(basedir, 'data')
    
    # Create directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)
    
    # Create subdirectories for user data
    covers_dir = os.path.join(data_dir, 'covers')
    uploads_dir = os.path.join(data_dir, 'uploads')
    os.makedirs(covers_dir, exist_ok=True)
    os.makedirs(uploads_dir, exist_ok=True)
    
    # Set permissions for standalone (Docker handles this in entrypoint)
    # Only set Unix permissions on non-Windows systems
    if not os.environ.get('DATABASE_URL'):  # Not in Docker environment
        if platform.system() != "Windows":
            try:
                # Set directory permissions (755 = rwxr-xr-x)
                os.chmod(data_dir, 0o755)
                os.chmod(covers_dir, 0o755)
                os.chmod(uploads_dir, 0o755)
            except (OSError, PermissionError):
                # Ignore permission errors (common on some systems)
                pass
    
    return data_dir

# Initialize data directory
data_dir = ensure_data_directory()

# 2) Overlay data/.env so settings saved at runtime persist via the mounted volume
try:
    data_env_path = os.path.join(basedir, 'data', '.env')
    if os.path.exists(data_env_path):
        load_dotenv(dotenv_path=data_env_path, override=True)
except Exception:
    pass

# Ensure Flask-Session directory exists
flask_sessions_dir = os.path.join(data_dir, 'flask_sessions')
if not os.path.exists(flask_sessions_dir):
    os.makedirs(flask_sessions_dir, exist_ok=True)
    if platform.system() != "Windows":
        try:
            os.chmod(flask_sessions_dir, 0o755)
        except (OSError, PermissionError):
            pass

class Config:
    # Expose data directory path for other modules
    DATA_DIR = data_dir
    # Security
    SECRET_KEY = os.environ.get('SECRET_KEY')
    if not SECRET_KEY:
        # For development, generate a temporary secret key
        # In production, always set SECRET_KEY environment variable
        if os.environ.get('FLASK_ENV') == 'development' or os.environ.get('FLASK_DEBUG'):
            SECRET_KEY = secrets.token_hex(32)
            print("⚠️  WARNING: Using temporary SECRET_KEY for development. Set SECRET_KEY in .env for production!")
        else:
            # In a multi-worker setup (like Gunicorn), it's critical that all workers
            # share the same secret key. Generating a random key per worker will
            # lead to session corruption and CSRF failures.
            raise ValueError("No SECRET_KEY set for Flask application. Please set it in your .env file.")
    
    # CSRF Settings with better defaults
    WTF_CSRF_ENABLED = True
    WTF_CSRF_TIME_LIMIT = 3600  # 1 hour
    WTF_CSRF_SSL_STRICT = False  # Allow CSRF over HTTP for development
    
    # Session settings for better reliability
    # For development, disable secure cookies to allow HTTP sessions
    SESSION_COOKIE_SECURE = False  # Set to True only in production with HTTPS
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    # SESSION_PERMANENT default is False - will be set to True in login route when "Remember Me" is checked
    # This allows users to have both temporary sessions (browser close = logout) and persistent sessions
    SESSION_PERMANENT = False
    PERMANENT_SESSION_LIFETIME = 86400  # 24 hours (when session.permanent = True)

    # Flask-Session Configuration
    # Use filesystem sessions for development/Docker, Redis for production
    SESSION_TYPE = os.environ.get('SESSION_TYPE', 'filesystem')  # 'filesystem', 'redis', 'null'
    SESSION_USE_SIGNER = True  # Sign session cookies for security
    SESSION_KEY_PREFIX = 'bibliotheca:'
    
    # Filesystem session configuration (for development/Docker)
    SESSION_FILE_DIR = os.path.join(data_dir, 'flask_sessions')
    SESSION_FILE_THRESHOLD = 500  # Maximum number of sessions to store
    
    # Redis session configuration (for production)
    SESSION_REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
    SESSION_REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
    SESSION_REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD')
    SESSION_REDIS_DB = int(os.environ.get('REDIS_SESSION_DB', 0))

    # Database configuration
    DATABASE_PATH = os.path.join(data_dir, 'books.db')
    SQLALCHEMY_DATABASE_URI = f"sqlite:///{DATABASE_PATH}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # File uploads
    MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB max file size

    # Kuzu Database Configuration
    KUZU_DB_PATH = os.environ.get('KUZU_DB_PATH') or './data/kuzu'
    GRAPH_DATABASE_ENABLED = os.environ.get('GRAPH_DATABASE_ENABLED', 'true').lower() == 'true'

    # External APIs
    ISBN_API_KEY = os.environ.get('ISBN_API_KEY') or 'your_isbn_api_key'
    
    # Application settings
    SITE_NAME = os.environ.get('SITE_NAME', 'MyBibliotheca')
    TIMEZONE = os.environ.get('TIMEZONE') or 'UTC'
    
    # Authentication settings
    # Remember cookie duration when "Remember Me" is checked
    REMEMBER_COOKIE_DURATION = 86400 * 7  # 7 days
    # Use FLASK_DEBUG environment variable (FLASK_ENV is deprecated in Flask 2.3+)
    # In production (FLASK_DEBUG=false), use secure cookies (requires HTTPS)
    REMEMBER_COOKIE_SECURE = os.environ.get('FLASK_DEBUG', 'false').lower() == 'false'
    REMEMBER_COOKIE_HTTPONLY = True  # Prevent JavaScript access to cookie for security
    REMEMBER_COOKIE_SAMESITE = 'Lax'  # CSRF protection while allowing normal navigation
    
    # File Upload settings
    MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB max file upload
    
    # Email settings (for password reset)
    MAIL_SERVER = os.environ.get('MAIL_SERVER', 'localhost')
    MAIL_PORT = int(os.environ.get('MAIL_PORT', 587))
    MAIL_USE_TLS = os.environ.get('MAIL_USE_TLS', 'true').lower() in ['true', 'on', '1']
    MAIL_USERNAME = os.environ.get('MAIL_USERNAME')
    MAIL_PASSWORD = os.environ.get('MAIL_PASSWORD')
    MAIL_DEFAULT_SENDER = os.environ.get('MAIL_DEFAULT_SENDER', 'noreply@bibliotheca.local')
    
    # Admin settings
    ADMIN_EMAIL = os.environ.get('ADMIN_EMAIL', 'admin@bibliotheca.local')
    ADMIN_USERNAME = os.environ.get('ADMIN_USERNAME', 'admin')
    ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD')  # No default - must be set via env var
    
    # API Authentication
    # For development, use a simple test token. In production, implement proper token management.
    API_TEST_TOKEN = os.environ.get('API_TEST_TOKEN')  # No default - must be set via env var
    
    # Debug settings (disabled by default for security)
    DEBUG_MODE = os.environ.get('MYBIBLIOTHECA_DEBUG', 'false').lower() in ['true', 'on', '1']
    DEBUG_CSRF = os.environ.get('MYBIBLIOTHECA_DEBUG_CSRF', 'false').lower() in ['true', 'on', '1']
    DEBUG_SESSION = os.environ.get('MYBIBLIOTHECA_DEBUG_SESSION', 'false').lower() in ['true', 'on', '1']
    DEBUG_AUTH = os.environ.get('MYBIBLIOTHECA_DEBUG_AUTH', 'false').lower() in ['true', 'on', '1']
    DEBUG_REQUESTS = os.environ.get('MYBIBLIOTHECA_DEBUG_REQUESTS', 'false').lower() in ['true', 'on', '1']
    
    # Debug log level (only used if debug mode is enabled)
    DEBUG_LOG_LEVEL = os.environ.get('MYBIBLIOTHECA_DEBUG_LOG_LEVEL', 'INFO')

    # Database configuration
    DATABASE_PATH = os.path.join(data_dir, 'books.db')
    SQLALCHEMY_DATABASE_URI = f"sqlite:///{DATABASE_PATH}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
