import os
import secrets
import platform

basedir = os.path.abspath(os.path.dirname(__file__))

def ensure_data_directory():
    """Ensure data directory exists with proper permissions for both Docker and standalone (cross-platform)"""
    data_dir = os.path.join(basedir, 'data')
    
    # Create directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)
    
    # Set permissions for standalone (Docker handles this in entrypoint)
    # Only set Unix permissions on non-Windows systems
    if not os.environ.get('DATABASE_URL'):  # Not in Docker environment
        if platform.system() != "Windows":
            try:
                # Set directory permissions (755 = rwxr-xr-x)
                os.chmod(data_dir, 0o755)
            except (OSError, PermissionError):
                # Ignore permission errors (common on some systems)
                pass
    
    return data_dir

# Initialize data directory
data_dir = ensure_data_directory()

class Config:
    # Security
    SECRET_KEY = os.environ.get('SECRET_KEY')
    if not SECRET_KEY:
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
    SESSION_PERMANENT = False  # Don't require permanent sessions for CSRF
    PERMANENT_SESSION_LIFETIME = 86400  # 24 hours
    
    # Additional session settings to ensure sessions work properly
    SESSION_TYPE = 'filesystem'  # Use filesystem sessions for reliability
    SESSION_USE_SIGNER = True  # Sign session cookies for security

    # File uploads
    MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB max file size

    # Redis Database Configuration
    REDIS_URL = os.environ.get('REDIS_URL') or 'redis://localhost:6379/0'
    GRAPH_DATABASE_ENABLED = os.environ.get('GRAPH_DATABASE_ENABLED', 'true').lower() == 'true'

    # External APIs
    ISBN_API_KEY = os.environ.get('ISBN_API_KEY') or 'your_isbn_api_key'
    
    # Application settings
    TIMEZONE = os.environ.get('TIMEZONE') or 'UTC'
    
    # Authentication settings
    REMEMBER_COOKIE_DURATION = 86400 * 7  # 7 days
    # Use FLASK_DEBUG environment variable (FLASK_ENV is deprecated in Flask 2.3+)
    REMEMBER_COOKIE_SECURE = os.environ.get('FLASK_DEBUG', 'false').lower() == 'false'
    REMEMBER_COOKIE_HTTPONLY = True
    
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
    DEBUG_MODE = os.environ.get('BIBLIOTHECA_DEBUG', 'false').lower() in ['true', 'on', '1']
    DEBUG_CSRF = os.environ.get('BIBLIOTHECA_DEBUG_CSRF', 'false').lower() in ['true', 'on', '1']
    DEBUG_SESSION = os.environ.get('BIBLIOTHECA_DEBUG_SESSION', 'false').lower() in ['true', 'on', '1']
    DEBUG_AUTH = os.environ.get('BIBLIOTHECA_DEBUG_AUTH', 'false').lower() in ['true', 'on', '1']
    DEBUG_REQUESTS = os.environ.get('BIBLIOTHECA_DEBUG_REQUESTS', 'false').lower() in ['true', 'on', '1']
    
    # Debug log level (only used if debug mode is enabled)
    DEBUG_LOG_LEVEL = os.environ.get('BIBLIOTHECA_DEBUG_LOG_LEVEL', 'INFO')
