"""
Domain models for the graph database migration.

These models represent the core business entities independent of persistence concerns.
They define the structure and relationships according to the planning document.
"""

from dataclasses import dataclass, field, InitVar
from dataclasses import fields as dataclass_fields
from dataclasses import MISSING
from datetime import datetime, date, timezone
from typing import Optional, List, Dict, Any, cast
from enum import Enum
import json

from app.utils.password_policy import (
    get_password_requirements as get_policy_password_requirements,
    resolve_min_password_length,
)


def now_utc() -> datetime:
    """Timezone-aware UTC now for default timestamps (avoid datetime.utcnow deprecation)."""
    return datetime.now(timezone.utc)


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
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)


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
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)

    def to_dict(self):
        """Convert template to a dictionary for database storage."""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "name": self.name,
            "description": self.description,
            "source_type": self.source_type,
            "sample_headers": json.dumps(self.sample_headers),
            "field_mappings": json.dumps(self.field_mappings),
            "times_used": self.times_used,
            "last_used": self.last_used.isoformat() if self.last_used else None,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict):
        """Create a template from a dictionary (e.g., from database)."""
        # Decode byte strings if needed
        decoded_data = {k.decode('utf-8') if isinstance(k, bytes) else k: v.decode('utf-8') if isinstance(v, bytes) else v for k, v in data.items()}
        
        return cls(
            id=decoded_data.get("id"),
            user_id=decoded_data.get("user_id") or "",
            name=decoded_data.get("name") or "",
            description=decoded_data.get("description"),
            source_type=decoded_data.get("source_type") or "",
            sample_headers=json.loads(decoded_data.get("sample_headers", '[]')),
            field_mappings=json.loads(decoded_data.get("field_mappings", '{}')),
            times_used=int(decoded_data.get("times_used", 0)),
            last_used=datetime.fromisoformat(decoded_data["last_used"]) if decoded_data.get("last_used") else None,
            created_at=datetime.fromisoformat(decoded_data["created_at"]),
            updated_at=datetime.fromisoformat(decoded_data["updated_at"])
        )


@dataclass
class Author:
    """Author domain model."""
    id: Optional[str] = None
    name: str = ""
    normalized_name: str = ""  # For fuzzy matching
    birth_year: Optional[int] = None
    death_year: Optional[int] = None
    bio: Optional[str] = None
    created_at: datetime = field(default_factory=now_utc)
    
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
    created_at: datetime = field(default_factory=now_utc)
    
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
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)


@dataclass
class Series:
    """Book series domain model."""
    id: Optional[str] = None
    name: str = ""
    normalized_name: str = ""
    description: Optional[str] = None
    # user_cover: explicit custom upload provided by user (preferred if present)
    user_cover: Optional[str] = None
    # cover_url: stored first-book cover reference (auto-maintained)
    cover_url: Optional[str] = None
    custom_cover: bool = False  # legacy flag; considered True iff user_cover is not None
    generated_placeholder: bool = False  # retained for backward compatibility; not used in new logic
    created_at: datetime = field(default_factory=now_utc)
    
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
    color: Optional[str] = None  # Hex color for visual organization
    icon: Optional[str] = None  # Bootstrap icon or emoji for display
    aliases: List[str] = field(default_factory=list)  # Alternative names/spellings
    
    # Hierarchy relationships (populated by service layer)
    parent: Optional['Category'] = None
    children: List['Category'] = field(default_factory=list)
    
    # Usage statistics (populated by service layer)
    book_count: int = 0
    user_book_count: int = 0  # For specific user
    
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)
    
    def __post_init__(self):
        if not self.normalized_name and self.name:
            self.normalized_name = self._normalize_name(self.name)
    
    @staticmethod
    def _normalize_name(name: str) -> str:
        """Normalize category name for matching (handle variations)."""
        return name.strip().lower()
    
    @property
    def full_path(self) -> str:
        """Get full hierarchical path (e.g., 'Fiction > Science Fiction > Dystopian')."""
        if self.parent:
            return f"{self.parent.full_path} > {self.name}"
        return self.name
    
    @property
    def breadcrumbs(self) -> List['Category']:
        """Get list of categories from root to this category."""
        if self.parent:
            return self.parent.breadcrumbs + [self]
        return [self]
    
    def is_descendant_of(self, category: 'Category') -> bool:
        """Check if this category is a descendant of another category."""
        if not self.parent:
            return False
        if self.parent.id == category.id:
            return True
        return self.parent.is_descendant_of(category)
    
    def get_all_ancestors(self) -> List['Category']:
        """Get all ancestor categories up to root."""
        if not self.parent:
            return []
        return [self.parent] + self.parent.get_all_ancestors()
    
    def get_ancestors(self) -> List['Category']:
        """Alias for get_all_ancestors() for template compatibility."""
        return self.get_all_ancestors()
    
    def get_all_descendants(self) -> List['Category']:
        """Get all descendant categories recursively."""
        descendants = []
        for child in self.children:
            descendants.append(child)
            descendants.extend(child.get_all_descendants())
        return descendants
    
    @property
    def is_root(self) -> bool:
        """Check if this is a root category (no parent)."""
        return self.parent_id is None
    
    @property
    def is_leaf(self) -> bool:
        """Check if this is a leaf category (no children)."""
        return len(self.children) == 0
    
    def matches_name_or_alias(self, name: str) -> bool:
        """Check if name matches this category's name or any alias."""
        normalized_name = self._normalize_name(name)
        if normalized_name == self.normalized_name:
            return True
        normalized_aliases = [self._normalize_name(alias) for alias in self.aliases]
        return normalized_name in normalized_aliases


class ContributionType(Enum):
    """Types of contributions a person can make to a book."""
    AUTHORED = "authored"
    EDITED = "edited"
    TRANSLATED = "translated"
    ILLUSTRATED = "illustrated"
    NARRATED = "narrated"  # For audiobooks
    GAVE_FOREWORD = "gave_foreword"
    GAVE_INTRODUCTION = "gave_introduction"
    GAVE_AFTERWORD = "gave_afterword"
    COMPILED = "compiled"
    CONTRIBUTED = "contributed"  # Generic contribution
    CO_AUTHORED = "co_authored"
    GHOST_WROTE = "ghost_wrote"

@dataclass
class Person:
    """Person domain model - represents contributors (authors, narrators, editors, etc.)."""
    id: Optional[str] = None
    name: str = ""
    normalized_name: str = ""  # For fuzzy matching
    
    # Optional biographical information
    birth_date: Optional[str] = None  # Full birth date string from OpenLibrary
    death_date: Optional[str] = None  # Full death date string from OpenLibrary
    birth_year: Optional[int] = None
    death_year: Optional[int] = None
    birth_place: Optional[str] = None
    bio: Optional[str] = None
    website: Optional[str] = None
    
    # External service IDs
    openlibrary_id: Optional[str] = None
    wikidata_id: Optional[str] = None
    imdb_id: Optional[str] = None
    
    # Additional metadata
    fuller_name: Optional[str] = None  # Full name with titles, etc.
    title: Optional[str] = None  # Professional title (e.g., "Dr.", "Professor")
    alternate_names: Optional[str] = None  # JSON string of alternate names
    official_links: Optional[str] = None  # JSON string of official links
    
    # Media
    image_url: Optional[str] = None
    
    # Timestamps
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)
    
    def __post_init__(self):
        if not self.normalized_name and self.name:
            self.normalized_name = self._normalize_name(self.name)
    
    @staticmethod
    def _normalize_name(name: str) -> str:
        """Normalize person name for matching (handle variations like 'Smith, John' vs 'John Smith')."""
        # Handle "Last, First" format
        if ',' in name:
            parts = [part.strip() for part in name.split(',')]
            if len(parts) == 2:
                # Reverse "Last, First" to "First Last"
                name = f"{parts[1]} {parts[0]}"
        
        return name.strip().lower()


@dataclass
class BookContribution:
    """Represents a contribution relationship between a Person and a Book."""
    person_id: str = ""
    book_id: str = ""
    contribution_type: ContributionType = ContributionType.AUTHORED
    order: Optional[int] = None  # For ordering multiple contributors of same type
    notes: Optional[str] = None  # Additional context about the contribution
    
    # For display purposes
    person: Optional[Person] = None
    
    # Timestamps
    created_at: datetime = field(default_factory=now_utc)


@dataclass(init=False)
class Book:
    """Core book domain model - represents global book data shared across users."""
    id: Optional[str] = None
    # Note: We intentionally do NOT declare an InitVar named 'author' here because
    # a property with the same name exists below. We'll handle 'author' and 'name'
    # in a custom __init__ to keep a clean API for tests while preserving the property.
    title: str = ""
    normalized_title: str = ""
    subtitle: Optional[str] = None
    isbn13: Optional[str] = None
    isbn10: Optional[str] = None
    asin: Optional[str] = None
    description: Optional[str] = None
    published_date: Optional[date] = None
    page_count: Optional[int] = None
    language: str = "en"
    cover_url: Optional[str] = None
    google_books_id: Optional[str] = None
    openlibrary_id: Optional[str] = None
    opds_source_id: Optional[str] = None
    opds_source_updated_at: Optional[str] = None
    opds_source_entry_hash: Optional[str] = None
    
    # Global metadata (not user-specific)
    average_rating: Optional[float] = None
    rating_count: Optional[int] = None
    
    # Custom metadata fields (global, shared across all users)
    custom_metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Timestamps
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)
    
    # Relationships (will be resolved via repository)
    contributors: List[BookContribution] = field(default_factory=list)
    publisher: Optional[Publisher] = None
    series: Optional[Series] = None
    series_volume: Optional[str] = None
    series_order: Optional[int] = None
    categories: List[Category] = field(default_factory=list)
    
    # Raw category data from API/CSV (temporary field for processing)
    raw_categories: Optional[Any] = None

    # Core format field (physical, ebook, audiobook, kindle, etc.) now global
    media_type: Optional[str] = None
    
    # Quantity field to track number of copies owned
    quantity: int = 1

    def __init__(self, author: Optional[str] = None, name: Optional[str] = None, **kwargs):
        """Custom initializer to accept legacy-friendly aliases.
        Accepts:
        - author: primary author name (creates a contributor if none provided)
        - name: alias for title
        - all other dataclass fields as keyword args
        """
        # First, set dataclass fields from kwargs or their defaults
        for f in dataclass_fields(self.__class__):
            if f.name in kwargs:
                setattr(self, f.name, kwargs.pop(f.name))
            else:
                if f.default is not MISSING:
                    setattr(self, f.name, f.default)
                elif f.default_factory is not MISSING:  # type: ignore[attr-defined]
                    setattr(self, f.name, f.default_factory())  # type: ignore[misc]
                else:
                    # No default; initialize with None for Optional fields, else sensible baseline
                    setattr(self, f.name, None)

        # Map provided 'name' to title if title wasn't explicitly set
        if name and not getattr(self, 'title', None):
            self.title = name

        # Normalize title
        if not self.normalized_title and self.title:
            self.normalized_title = self._normalize_title(self.title)

        # Normalize/parse published_date
        if self.published_date:
            if isinstance(self.published_date, str):
                date_string = self.published_date
                try:
                    self.published_date = datetime.fromisoformat(date_string).date()
                except ValueError:
                    try:
                        self.published_date = datetime.strptime(date_string, '%Y-%m-%d').date()
                    except ValueError:
                        try:
                            if len(date_string) == 4 and date_string.isdigit():
                                self.published_date = datetime.strptime(f"{date_string}-01-01", '%Y-%m-%d').date()
                            else:
                                print(f"[BOOK_MODEL][WARN] Could not parse published_date string: {date_string}")
                                self.published_date = None
                        except ValueError:
                            print(f"[BOOK_MODEL][WARN] Could not parse published_date string: {date_string}")
                            self.published_date = None
            elif isinstance(self.published_date, datetime):
                self.published_date = self.published_date.date()
            elif isinstance(self.published_date, date):
                pass

        # Create a contributor from 'author' if provided and no contributors present
        if author and not self.contributors:
            person = Person(name=author)
            contribution = BookContribution(
                person=person,
                contribution_type=ContributionType.AUTHORED,
                order=0
            )
            self.contributors = [contribution]
    
    @staticmethod
    def _normalize_title(title: str) -> str:
        """Normalize title for fuzzy matching."""
        return title.strip().lower()
    
    @property
    def authors(self) -> List['Author']:
        """Get authors from contributors for backward compatibility."""
        author_contributors = [c for c in self.contributors if c.contribution_type == ContributionType.AUTHORED]
        return [Author(id=c.person.id, name=c.person.name, normalized_name=c.person.normalized_name) 
                for c in author_contributors if c.person]
    
    @property
    def narrators(self) -> List['Person']:
        """Get narrators from contributors."""
        narrator_contributors = [c for c in self.contributors if c.contribution_type == ContributionType.NARRATED]
        return [c.person for c in narrator_contributors if c.person]
    
    @property
    def editors(self) -> List['Person']:
        """Get editors from contributors."""
        editor_contributors = [c for c in self.contributors if c.contribution_type == ContributionType.EDITED]
        return [c.person for c in editor_contributors if c.person]
    
    @property
    def translators(self) -> List['Person']:
        """Get translators from contributors."""
        translator_contributors = [c for c in self.contributors if c.contribution_type == ContributionType.TRANSLATED]
        return [c.person for c in translator_contributors if c.person]
    
    @property
    def illustrators(self) -> List['Person']:
        """Get illustrators from contributors."""
        illustrator_contributors = [c for c in self.contributors if c.contribution_type == ContributionType.ILLUSTRATED]
        return [c.person for c in illustrator_contributors if c.person]
    
    def get_contributors_by_type(self, contribution_type: ContributionType) -> List['Person']:
        """Get contributors by contribution type."""
        type_contributors = [c for c in self.contributors if c.contribution_type == contribution_type]
        return [c.person for c in type_contributors if c.person]

    def get_contributors_by_type_str(self, contribution_type_str: str) -> List['Person']:
        """Get contributors by contribution type string (for template compatibility)."""
        try:
            contribution_type = ContributionType(contribution_type_str.lower())
            return self.get_contributors_by_type(contribution_type)
        except ValueError:
            return []

    @property
    def author(self) -> str:
        """Get primary author name for backward compatibility."""
        if self.authors:
            return self.authors[0].name
        return ""
    
    @property  
    def author_names(self) -> str:
        """Get comma-separated author names for backward compatibility."""
        if self.authors:
            return ', '.join(author.name for author in self.authors)
        return ""

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
        elif self.title and self.contributors:
            # Get author contributors for deduplication
            author_contributors = [c for c in self.contributors if c.contribution_type == ContributionType.AUTHORED]
            if author_contributors:
                author_names = [c.person.normalized_name if c.person else "" for c in author_contributors]
                author_names = [name for name in author_names if name]  # Filter out empty names
                return f"title_author:{self.normalized_title}:{':'.join(sorted(author_names))}"
        
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
    
    # Timezone setting
    timezone: str = "UTC"
    
    # User metadata (future enhancement)
    display_name: Optional[str] = None
    bio: Optional[str] = None
    location: Optional[str] = None
    website: Optional[str] = None
    
    # System fields
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)
    
    # Flask-Login compatibility methods
    def is_authenticated(self) -> bool:
        """Required by Flask-Login."""
        return True
    
    def is_anonymous(self) -> bool:
        """Required by Flask-Login."""
        return False
    
    def get_id(self) -> str:
        """Required by Flask-Login."""
        return self.id or ""
    
    def set_password(self, password: str):
        """Set password hash using werkzeug."""
        from werkzeug.security import generate_password_hash
        self.password_hash = generate_password_hash(password)
        self.password_changed_at = now_utc()
        self.password_must_change = False
    
    def check_password(self, password: str) -> bool:
        """Check password using werkzeug."""
        from werkzeug.security import check_password_hash
        return check_password_hash(self.password_hash, password)
    
    @staticmethod
    def is_password_strong(password: str) -> bool:
        """
        Check if password meets security requirements:
        - Meets the configured minimum length
        - Contains at least one letter (upper or lower case)
        - Contains at least one number OR special character
        - Not in common password blacklist
        """
        import re

        min_length = cast(int, resolve_min_password_length())
        if len(password) < min_length:
            return False
        
        # Must contain at least one letter
        if not re.search(r'[A-Za-z]', password):
            return False
        
        # Must contain at least one number OR special character (more flexible)
        has_number = bool(re.search(r'\d', password))
        has_special = bool(re.search(r'[!@#$%^&*()_+\-=\[\]{};\':"\\|,.<>\/?]', password))
        
        if not (has_number or has_special):
            return False
        
        # Common password blacklist (more comprehensive but still reasonable)
        common_passwords = {
            'password', 'password123', 'password1234', 'admin123', 'administrator',
            'qwerty123', 'welcome123', 'letmein123', 'password!', 'admin', 'qwerty',
            '123456', '12345678', 'welcome', 'letmein', 'monkey', 'dragon'
        }
        
        if password.lower() in common_passwords:
            return False
        
        return True
    
    @staticmethod
    def get_password_requirements() -> List[str]:
        """Return a list of password requirements for display to users"""
        return get_policy_password_requirements()
    
    def is_locked(self) -> bool:
        """Check if the user account is currently locked."""
        if self.locked_until is None:
            return False
        # Use timezone-aware comparison; coerce naive locked_until to UTC if needed
        now = now_utc()
        if self.locked_until is not None and self.locked_until.tzinfo is None:
            locked_until = self.locked_until.replace(tzinfo=timezone.utc)
        else:
            locked_until = self.locked_until
        return now < locked_until if locked_until is not None else False
    
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
            self.locked_until = now_utc() + timedelta(minutes=30)
    
    def get_reading_streak(self) -> int:
        """Get the user's current reading streak with their personal offset."""
        from app.utils.user_utils import calculate_reading_streak  # Local import to avoid circulars
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
    # Default empty reading status in overlays; keep enum type optional via Optional[str] in storage layers
    reading_status: ReadingStatus = ReadingStatus.PLAN_TO_READ
    date_added: datetime = field(default_factory=now_utc)
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
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)


@dataclass
class ReadingLog:
    """Daily reading log entry."""
    user_id: str
    date: date
    id: Optional[str] = None
    book_id: Optional[str] = None  # Now optional to allow general reading logs
    pages_read: int = 0
    minutes_read: int = 0
    notes: Optional[str] = None
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)


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
    created_at: datetime = field(default_factory=now_utc)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None