import os
import secrets

basedir = os.path.abspath(os.path.dirname(__file__))

class Config:
    # Security
    SECRET_KEY = os.environ.get('SECRET_KEY') or secrets.token_urlsafe(32)
    WTF_CSRF_ENABLED = True
    WTF_CSRF_TIME_LIMIT = 3600  # 1 hour

    # Database
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
        f"sqlite:///{os.path.join(basedir, 'data', 'books.db')}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    DATABASE_PATH = os.path.join(basedir, 'data', 'books.db')

    # External APIs
    ISBN_API_KEY = os.environ.get('ISBN_API_KEY') or 'your_isbn_api_key'
    
    # Application settings
    TIMEZONE = os.environ.get('TIMEZONE') or 'UTC'
    READING_STREAK_OFFSET = int(os.environ.get('READING_STREAK_OFFSET', 500))
    
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
