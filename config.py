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
    SECRET_KEY = os.environ.get('SECRET_KEY') or secrets.token_urlsafe(32)
    WTF_CSRF_ENABLED = True
    WTF_CSRF_TIME_LIMIT = 3600  # 1 hour

    # Database - unified path handling for Docker and standalone
    DATABASE_PATH = os.path.join(data_dir, 'books.db')
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or f"sqlite:///{DATABASE_PATH}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # External APIs
    ISBN_API_KEY = os.environ.get('ISBN_API_KEY') or 'your_isbn_api_key'
    
    # Application settings
    TIMEZONE = os.environ.get('TIMEZONE') or 'UTC'
    READING_STREAK_OFFSET = int(os.environ.get('READING_STREAK_OFFSET', 0))
    
    # Authentication settings
    REMEMBER_COOKIE_DURATION = 86400 * 7  # 7 days
    # Use FLASK_DEBUG environment variable (FLASK_ENV is deprecated in Flask 2.3+)
    REMEMBER_COOKIE_SECURE = os.environ.get('FLASK_DEBUG', 'false').lower() == 'false'
    REMEMBER_COOKIE_HTTPONLY = True
    
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
    ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'changeme123')
    
    # Debug settings (disabled by default for security)
    DEBUG_MODE = os.environ.get('BIBLIOTHECA_DEBUG', 'false').lower() in ['true', 'on', '1']
    DEBUG_CSRF = os.environ.get('BIBLIOTHECA_DEBUG_CSRF', 'false').lower() in ['true', 'on', '1']
    DEBUG_SESSION = os.environ.get('BIBLIOTHECA_DEBUG_SESSION', 'false').lower() in ['true', 'on', '1']
    DEBUG_AUTH = os.environ.get('BIBLIOTHECA_DEBUG_AUTH', 'false').lower() in ['true', 'on', '1']
    DEBUG_REQUESTS = os.environ.get('BIBLIOTHECA_DEBUG_REQUESTS', 'false').lower() in ['true', 'on', '1']
    
    # Debug log level (only used if debug mode is enabled)
    DEBUG_LOG_LEVEL = os.environ.get('BIBLIOTHECA_DEBUG_LOG_LEVEL', 'INFO')
