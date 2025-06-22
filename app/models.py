from datetime import datetime, timezone, timedelta
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
import secrets
import re

db = SQLAlchemy()

class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    is_admin = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    is_active = db.Column(db.Boolean, default=True)
    
    # Security fields for account lockout
    failed_login_attempts = db.Column(db.Integer, default=0)
    locked_until = db.Column(db.DateTime, nullable=True)
    last_login = db.Column(db.DateTime, nullable=True)
    
    # Privacy settings for sharing
    share_current_reading = db.Column(db.Boolean, default=True)
    share_reading_activity = db.Column(db.Boolean, default=True)
    share_library = db.Column(db.Boolean, default=True)
    
    # Password security
    password_must_change = db.Column(db.Boolean, default=False)
    password_changed_at = db.Column(db.DateTime, nullable=True)
    
    # Reading streak offset
    reading_streak_offset = db.Column(db.Integer, default=0)
    
    # Relationships
    books = db.relationship('Book', backref='user', lazy=True, cascade='all, delete-orphan')
    
    def set_password(self, password, validate=True):
        """Set password hash"""
        # Validate password strength before setting (unless explicitly bypassed)
        if validate and not self.is_password_strong(password):
            raise ValueError("Password does not meet security requirements")
        
        self.password_hash = generate_password_hash(password)
        self.password_changed_at = datetime.now(timezone.utc)
        # Clear password change requirement when password is set (unless it's initial setup)
        if validate:
            self.password_must_change = False
    
    def check_password(self, password):
        """Check password hash"""
        return check_password_hash(self.password_hash, password)
    
    @staticmethod
    def is_password_strong(password):
        """
        Check if password meets security requirements:
        - At least 12 characters long
        - Contains uppercase letter
        - Contains lowercase letter
        - Contains number
        - Contains special character
        - Not in common password blacklist
        """
        if len(password) < 12:
            print("Password validation failed: Too short")
            return False
        
        if not re.search(r'[A-Z]', password):
            print("Password validation failed: Missing uppercase letter")
            return False
        
        if not re.search(r'[a-z]', password):
            print("Password validation failed: Missing lowercase letter")
            return False
        
        if not re.search(r'\d', password):
            print("Password validation failed: Missing number")
            return False
        
        if not re.search(r'[!@#$%^&*()_+\-=\[\]{};\':"\\|,.<>\/?]', password):
            print("Password validation failed: Missing special character")
            return False
        
        # Common password blacklist
        common_passwords = {
            'password123', 'password1234', 'admin123', 'administrator',
            'qwerty123', 'welcome123', 'letmein123', 'password!',
        }
        
        if password.lower() in common_passwords:
            print("Password validation failed: Password is in the common password blacklist")
            return False
        
        return True
    
    @staticmethod
    def get_password_requirements():
        """Return a list of password requirements for display to users"""
        return [
            "At least 12 characters long",
            "Contains at least one uppercase letter (A-Z)",
            "Contains at least one lowercase letter (a-z)",
            "Contains at least one number (0-9)",
            "Contains at least one special character (!@#$%^&*()_+-=[]{};\':\"\\|,.<>/?)",
            "Not a commonly used password"
        ]
    
    def is_locked(self):
        """Check if account is currently locked"""
        if self.locked_until is None:
            return False
        return datetime.now(timezone.utc) < self.locked_until
    
    def increment_failed_login(self):
        """Increment failed login attempts and lock account if needed"""
        self.failed_login_attempts += 1
        if self.failed_login_attempts >= 5:
            # Lock account for 30 minutes
            self.locked_until = datetime.now(timezone.utc) + timedelta(minutes=30)
        db.session.commit()
    
    def reset_failed_login(self):
        """Reset failed login attempts (called on successful login)"""
        self.failed_login_attempts = 0
        self.locked_until = None
        self.last_login = datetime.now(timezone.utc)
        db.session.commit()
    
    def unlock_account(self):
        """Admin function to unlock a locked account"""
        self.failed_login_attempts = 0
        self.locked_until = None
        db.session.commit()
    
    def get_reading_streak(self):
        """Get the user's current reading streak with their personal offset"""
        from app.utils import calculate_reading_streak
        return calculate_reading_streak(self.id, self.reading_streak_offset)
    
    def __repr__(self):
        return f'<User {self.username}>'

class Book(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    uid = db.Column(db.String(12), unique=True, nullable=False, default=lambda: secrets.token_urlsafe(6))
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    title = db.Column(db.String(255), nullable=False)
    author = db.Column(db.String(255), nullable=False)
    isbn = db.Column(db.String(13), nullable=False)  # Removed unique constraint for multi-user
    start_date = db.Column(db.Date, nullable=True)
    finish_date = db.Column(db.Date, nullable=True)
    cover_url = db.Column(db.String(512), nullable=True)
    want_to_read = db.Column(db.Boolean, default=False)
    library_only = db.Column(db.Boolean, default=False)
    # New metadata fields
    description = db.Column(db.Text, nullable=True)
    published_date = db.Column(db.String(50), nullable=True)
    page_count = db.Column(db.Integer, nullable=True)
    categories = db.Column(db.String(500), nullable=True)  # Store as comma-separated string
    publisher = db.Column(db.String(255), nullable=True)
    language = db.Column(db.String(10), nullable=True)
    average_rating = db.Column(db.Float, nullable=True)
    rating_count = db.Column(db.Integer, nullable=True)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    
    # Add unique constraint for ISBN per user
    __table_args__ = (
        db.UniqueConstraint('user_id', 'isbn', name='unique_user_isbn'),
    )

    def __init__(self, title, author, isbn, user_id, start_date=None, finish_date=None, cover_url=None, want_to_read=False, library_only=False, description=None, published_date=None, page_count=None, categories=None, publisher=None, language=None, average_rating=None, rating_count=None, **kwargs):
        self.title = title
        self.author = author
        self.isbn = isbn
        self.user_id = user_id
        self.start_date = start_date
        self.finish_date = finish_date
        self.cover_url = cover_url
        self.want_to_read = want_to_read
        self.library_only = library_only
        self.description = description
        self.published_date = published_date
        self.page_count = page_count
        self.categories = categories
        self.publisher = publisher
        self.language = language
        self.average_rating = average_rating
        self.rating_count = rating_count
        # If you have other fields, set them here or with kwargs

    def save(self):
        db.session.add(self)
        db.session.commit()

    @classmethod
    def get_all_books(cls):
        return cls.query.all()
    
    @classmethod
    def get_user_books(cls, user_id):
        return cls.query.filter_by(user_id=user_id).all()

    @classmethod
    def get_book_by_isbn(cls, isbn):
        return cls.query.filter_by(isbn=isbn).first()
    
    @classmethod
    def get_user_book_by_isbn(cls, user_id, isbn):
        return cls.query.filter_by(user_id=user_id, isbn=isbn).first()
    
    @property
    def secure_cover_url(self):
        """Return HTTPS version of cover URL for security."""
        if self.cover_url and self.cover_url.startswith('http://'):
            return self.cover_url.replace('http://', 'https://')
        return self.cover_url
    
    def __repr__(self):
        return f'<Book {self.title} by {self.author}>'

class ReadingLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    book_id = db.Column(db.Integer, db.ForeignKey('book.id'), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    date = db.Column(db.Date, nullable=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))

    book = db.relationship('Book', backref=db.backref('reading_logs', lazy=True))
    user = db.relationship('User', backref=db.backref('reading_logs', lazy=True))
    
    # Ensure unique log per user per book per date
    __table_args__ = (
        db.UniqueConstraint('user_id', 'book_id', 'date', name='unique_user_book_date'),
    )
    
    def __repr__(self):
        return f'<ReadingLog {self.user_id}:{self.book_id} on {self.date}>'

class Task(db.Model):
    id = db.Column(db.String(36), primary_key=True, default=lambda: secrets.token_urlsafe(27))
    name = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text, nullable=True)
    status = db.Column(db.String(20), default='pending')  # pending, running, completed, failed
    progress = db.Column(db.Integer, default=0)  # 0-100
    total_items = db.Column(db.Integer, default=0)
    processed_items = db.Column(db.Integer, default=0)
    success_count = db.Column(db.Integer, default=0)
    error_count = db.Column(db.Integer, default=0)
    current_item = db.Column(db.String(255), nullable=True)
    error_message = db.Column(db.Text, nullable=True)
    result = db.Column(db.JSON, nullable=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(timezone.utc))
    started_at = db.Column(db.DateTime, nullable=True)
    completed_at = db.Column(db.DateTime, nullable=True)
    
    user = db.relationship('User', backref=db.backref('tasks', lazy=True))
    
    def update_progress(self, processed=None, current_item=None, success_count=None, error_count=None):
        """Update task progress"""
        if processed is not None:
            self.processed_items = processed
        if current_item is not None:
            self.current_item = current_item
        if success_count is not None:
            self.success_count = success_count
        if error_count is not None:
            self.error_count = error_count
        
        if self.total_items > 0:
            self.progress = int((self.processed_items / self.total_items) * 100)
        
        db.session.commit()
    
    def mark_started(self):
        """Mark task as started"""
        self.status = 'running'
        self.started_at = datetime.now(timezone.utc)
        db.session.commit()
    
    def mark_completed(self, result=None):
        """Mark task as completed"""
        self.status = 'completed'
        self.completed_at = datetime.now(timezone.utc)
        self.progress = 100
        if result:
            self.result = result
        db.session.commit()
    
    def mark_failed(self, error_message):
        """Mark task as failed"""
        self.status = 'failed'
        self.completed_at = datetime.now(timezone.utc)
        self.error_message = error_message
        db.session.commit()
    
    def __repr__(self):
        return f'<Task {self.name}>'