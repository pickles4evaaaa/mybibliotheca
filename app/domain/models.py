"""
Domain models for the graph database migration.

These models represent the core business entities independent of persistence concerns.
They define the structure and relationships according to the planning document.
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Optional, List, Dict, Any
from enum import Enum


class ReadingStatus(Enum):
    """Reading status enumeration - flexible and extensible."""
    PLAN_TO_READ = "plan_to_read"
    READING = "reading"
    READ = "read"
    ON_HOLD = "on_hold"
    DNF = "did_not_finish"  # Did Not Finish
    
    # Legacy compatibility
    WANT_TO_READ = "plan_to_read"  # Alias for migration
    CURRENTLY_READING = "reading"  # Alias for migration
    HAS_READ = "read"  # Alias for migration
    LIBRARY_ONLY = "library_only"  # Special case - no reading intent


class OwnershipStatus(Enum):
    """Ownership status enumeration."""
    OWNED = "owned"
    BORROWED = "borrowed"
    LOANED = "loaned"
    WISHLIST = "wishlist"  # Don't own, want to acquire


class MediaType(Enum):
    """Media type enumeration."""
    PHYSICAL = "physical"
    EBOOK = "ebook"
    AUDIOBOOK = "audiobook"
    KINDLE = "kindle"


class CustomFieldType(Enum):
    """Custom field type enumeration."""
    TEXT = "text"
    NUMBER = "number"
    BOOLEAN = "boolean"
    DATE = "date"
    RATING_5 = "rating_5"  # 1-5 scale
    RATING_10 = "rating_10"  # 1-10 scale
    LIST = "list"  # Multiple values, comma-separated
    TAGS = "tags"  # Similar to list but with tag-like UI
    URL = "url"
    EMAIL = "email"
    TEXTAREA = "textarea"  # Long text


@dataclass
class CustomFieldDefinition:
    """Definition of a custom metadata field that can be applied to books."""
    id: Optional[str] = None
    name: str = ""  # Internal name (e.g., "reading_pace")
    display_name: str = ""  # User-friendly name (e.g., "Reading Pace")
    field_type: CustomFieldType = CustomFieldType.TEXT
    description: Optional[str] = None
    
    # Ownership and sharing
    created_by_user_id: str = ""
    is_shareable: bool = False  # Can other users see/use this definition
    is_global: bool = False  # Applies to global book data vs user-specific
    
    # Field configuration
    default_value: Optional[str] = None
    placeholder_text: Optional[str] = None
    help_text: Optional[str] = None
    
    # For list/tags fields - predefined options
    predefined_options: List[str] = field(default_factory=list)
    allow_custom_options: bool = True
    
    # For rating fields
    rating_min: int = 1
    rating_max: int = 5
    rating_labels: Dict[int, str] = field(default_factory=dict)  # e.g., {1: "Poor", 5: "Excellent"}
    
    # Usage statistics
    usage_count: int = 0  # How many users are using this definition
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ImportMappingTemplate:
    """Saved mapping template for CSV imports to avoid re-mapping same formats."""
    id: Optional[str] = None
    user_id: str = ""
    name: str = ""  # User-friendly name (e.g., "Goodreads Export")
    description: Optional[str] = None
    
    # Import source identification
    source_type: str = ""  # "goodreads", "storygraph", "custom"
    sample_headers: List[str] = field(default_factory=list)  # For matching detection
    
    # Field mappings
    field_mappings: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    # Format: {
    #   "csv_column_name": {
    #     "action": "map_existing|create_custom|skip",
    #     "target_field": "field_name",  # if map_existing
    #     "custom_field_def": CustomFieldDefinition,  # if create_custom
    #     "is_global": bool  # if create_custom
    #   }
    # }
    
    # Usage tracking
    times_used: int = 0
    last_used: Optional[datetime] = None
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Author:
    """Author domain model."""
    id: Optional[str] = None
    name: str = ""
    normalized_name: str = ""  # For fuzzy matching
    birth_year: Optional[int] = None
    death_year: Optional[int] = None
    bio: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def __post_init__(self):
        if not self.normalized_name and self.name:
            self.normalized_name = self._normalize_name(self.name)
    
    @staticmethod
    def _normalize_name(name: str) -> str:
        """Normalize author name for matching (handle variations like 'Smith, John' vs 'John Smith')."""
        return name.strip().lower()


@dataclass
class Publisher:
    """Publisher domain model."""
    id: Optional[str] = None
    name: str = ""
    normalized_name: str = ""
    founded_year: Optional[int] = None
    country: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def __post_init__(self):
        if not self.normalized_name and self.name:
            self.normalized_name = self.name.strip().lower()


@dataclass
class Location:
    """Location domain model for tracking where books are kept."""
    id: Optional[str] = None
    user_id: str = ""
    name: str = ""
    description: Optional[str] = None
    is_default: bool = False
    is_active: bool = True
    
    # Location metadata
    address: Optional[str] = None
    location_type: str = "home"  # "home", "office", "vacation", "storage", "other"
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class Series:
    """Book series domain model."""
    id: Optional[str] = None
    name: str = ""
    normalized_name: str = ""
    description: Optional[str] = None
    total_books: Optional[int] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def __post_init__(self):
        if not self.normalized_name and self.name:
            self.normalized_name = self.name.strip().lower()


@dataclass
class Category:
    """Category/Genre domain model with hierarchical support."""
    id: Optional[str] = None
    name: str = ""
    normalized_name: str = ""
    parent_id: Optional[str] = None
    description: Optional[str] = None
    level: int = 0  # Hierarchy level (0 = root)
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def __post_init__(self):
        if not self.normalized_name and self.name:
            self.normalized_name = self.name.strip().lower()


@dataclass
class Book:
    """Core book domain model - represents global book data shared across users."""
    id: Optional[str] = None
    title: str = ""
    normalized_title: str = ""
    subtitle: Optional[str] = None
    isbn13: Optional[str] = None
    isbn10: Optional[str] = None
    asin: Optional[str] = None
    description: Optional[str] = None
    published_date: Optional[datetime] = None
    page_count: Optional[int] = None
    language: str = "en"
    cover_url: Optional[str] = None
    google_books_id: Optional[str] = None
    openlibrary_id: Optional[str] = None
    
    # Global metadata (not user-specific)
    average_rating: Optional[float] = None
    rating_count: Optional[int] = None
    
    # Custom metadata fields (global, shared across all users)
    custom_metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    # Relationships (will be resolved via repository)
    authors: List[Author] = field(default_factory=list)
    publisher: Optional[Publisher] = None
    series: Optional[Series] = None
    series_volume: Optional[str] = None
    series_order: Optional[int] = None
    categories: List[Category] = field(default_factory=list)
    
    def __post_init__(self):
        if not self.normalized_title and self.title:
            self.normalized_title = self._normalize_title(self.title)
    
    @staticmethod
    def _normalize_title(title: str) -> str:
        """Normalize title for fuzzy matching."""
        return title.strip().lower()
    
    @property
    def primary_isbn(self) -> Optional[str]:
        """Get the primary ISBN (prefer ISBN13 over ISBN10)."""
        return self.isbn13 or self.isbn10
    
    @property
    def uid(self) -> Optional[str]:
        """Alias for id for backward compatibility."""
        return self.id
    
    def get_deduplication_key(self) -> str:
        """Get a key for deduplication matching."""
        if self.isbn13:
            return f"isbn13:{self.isbn13}"
        elif self.isbn10:
            return f"isbn10:{self.isbn10}"
        elif self.title and self.authors:
            author_names = [author.normalized_name for author in self.authors]
            return f"title_author:{self.normalized_title}:{':'.join(sorted(author_names))}"
        else:
            return f"title:{self.normalized_title}"


@dataclass
class User:
    """User domain model."""
    id: Optional[str] = None
    username: str = ""
    email: str = ""
    password_hash: str = ""
    
    # Privacy settings (from current schema)
    share_current_reading: bool = True
    share_reading_activity: bool = True
    share_library: bool = False
    
    # Security fields
    is_admin: bool = False
    is_active: bool = True
    password_must_change: bool = False
    failed_login_attempts: int = 0
    locked_until: Optional[datetime] = None
    last_login: Optional[datetime] = None
    password_changed_at: Optional[datetime] = None
    
    # Reading settings
    reading_streak_offset: int = 0
    
    # User metadata (future enhancement)
    display_name: Optional[str] = None
    bio: Optional[str] = None
    location: Optional[str] = None
    website: Optional[str] = None
    
    # System fields
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    # Flask-Login compatibility methods
    def is_authenticated(self) -> bool:
        """Required by Flask-Login."""
        return True
    
    def is_anonymous(self) -> bool:
        """Required by Flask-Login."""
        return False
    
    def get_id(self) -> str:
        """Required by Flask-Login."""
        return self.id
    
    def set_password(self, password: str):
        """Set password hash using werkzeug."""
        from werkzeug.security import generate_password_hash
        self.password_hash = generate_password_hash(password)
        self.password_changed_at = datetime.utcnow()
        self.password_must_change = False
    
    def check_password(self, password: str) -> bool:
        """Check password using werkzeug."""
        from werkzeug.security import check_password_hash
        return check_password_hash(self.password_hash, password)
    
    @staticmethod
    def is_password_strong(password: str) -> bool:
        """
        Check if password meets security requirements:
        - At least 12 characters long
        - Contains uppercase letter
        - Contains lowercase letter
        - Contains number
        - Contains special character
        - Not in common password blacklist
        """
        import re
        
        if len(password) < 12:
            return False
        
        if not re.search(r'[A-Z]', password):
            return False
        
        if not re.search(r'[a-z]', password):
            return False
        
        if not re.search(r'\d', password):
            return False
        
        if not re.search(r'[!@#$%^&*()_+\-=\[\]{};\':"\\|,.<>\/?]', password):
            return False
        
        # Common password blacklist
        common_passwords = {
            'password123', 'password1234', 'admin123', 'administrator',
            'qwerty123', 'welcome123', 'letmein123', 'password!',
        }
        
        if password.lower() in common_passwords:
            return False
        
        return True
    
    @staticmethod
    def get_password_requirements() -> List[str]:
        """Return a list of password requirements for display to users"""
        return [
            "At least 12 characters long",
            "Contains at least one uppercase letter (A-Z)",
            "Contains at least one lowercase letter (a-z)",
            "Contains at least one number (0-9)",
            "Contains at least one special character (!@#$%^&*()_+-=[]{};\':\"\\|,.<>/?)",
            "Not a commonly used password"
        ]
    
    def is_locked(self) -> bool:
        """Check if the user account is currently locked."""
        if self.locked_until is None:
            return False
        return datetime.utcnow() < self.locked_until
    
    def reset_failed_login(self) -> None:
        """Reset failed login attempts and unlock the account."""
        self.failed_login_attempts = 0
        self.locked_until = None
    
    def increment_failed_login(self, lock_threshold: int = 5) -> None:
        """Increment failed login attempts and lock account if threshold reached."""
        self.failed_login_attempts += 1
        if self.failed_login_attempts >= lock_threshold:
            # Lock account for 30 minutes
            from datetime import timedelta
            self.locked_until = datetime.utcnow() + timedelta(minutes=30)
    
    def get_reading_streak(self) -> int:
        """Get the user's current reading streak with their personal offset."""
        from ..utils import calculate_reading_streak
        return calculate_reading_streak(self.id, self.reading_streak_offset)
    
    def unlock_account(self) -> None:
        """Admin function to unlock a locked account (alias for reset_failed_login)."""
        self.reset_failed_login()


@dataclass
class UserBookRelationship:
    """Represents a user's relationship with a book (user-specific data)."""
    user_id: str
    book_id: str
    
    # Reading status
    reading_status: ReadingStatus = ReadingStatus.PLAN_TO_READ
    date_added: datetime = field(default_factory=datetime.utcnow)
    start_date: Optional[datetime] = None
    finish_date: Optional[datetime] = None
    
    # Ownership and location tracking
    ownership_status: OwnershipStatus = OwnershipStatus.OWNED
    media_type: MediaType = MediaType.PHYSICAL
    
    # Borrowing/Loaning tracking
    borrowed_from: Optional[str] = None  # Name or contact info
    borrowed_from_user_id: Optional[str] = None  # If borrowed from another user in system
    borrowed_date: Optional[datetime] = None
    borrowed_due_date: Optional[datetime] = None
    
    loaned_to: Optional[str] = None  # Name or contact info
    loaned_to_user_id: Optional[str] = None  # If loaned to another user in system
    loaned_date: Optional[datetime] = None
    loaned_due_date: Optional[datetime] = None
    
    # Location tracking - can be in multiple locations
    locations: List[str] = field(default_factory=list)  # List of location IDs
    primary_location_id: Optional[str] = None
    
    # User-specific data
    user_rating: Optional[float] = None
    rating_date: Optional[datetime] = None
    user_review: Optional[str] = None
    review_date: Optional[datetime] = None
    is_review_spoiler: bool = False
    
    # Personal organization
    personal_notes: Optional[str] = None
    user_tags: List[str] = field(default_factory=list)
    
    # Custom metadata fields (user-specific)
    custom_metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Reading analytics (StoryGraph-style)
    reading_sessions: List[Dict[str, Any]] = field(default_factory=list)
    pace: Optional[str] = None  # "slow", "medium", "fast"
    character_driven: Optional[bool] = None
    moods: List[str] = field(default_factory=list)
    
    # Source tracking
    source: str = "manual"  # "manual", "goodreads", "storygraph", "admin_assigned"
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ReadingLog:
    """Daily reading log entry."""
    user_id: str
    book_id: str
    date: date
    id: Optional[str] = None
    pages_read: int = 0
    minutes_read: int = 0
    notes: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ImportTask:
    """Background task for import operations."""
    user_id: str
    task_type: str  # "goodreads_import", "storygraph_import", "simple_csv"
    id: Optional[str] = None
    status: str = "pending"  # "pending", "running", "completed", "failed"
    progress: int = 0  # 0-100
    total_items: int = 0
    processed_items: int = 0
    
    # Task data
    file_path: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    # Results
    results: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
