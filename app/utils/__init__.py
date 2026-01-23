# Utils package for Bibliotheca

# Import functions from the book_utils module
from .book_utils import (
    fetch_author_data,
    fetch_book_data,
    generate_month_review_image,
    get_google_books_cover,
    normalize_goodreads_value,
    search_author_by_name,
    search_book_by_title_author,
    search_google_books_by_title_author,
    search_multiple_books_by_title_author,
)

# Unified metadata aggregation
from .unified_metadata import (
    fetch_unified_by_isbn,
    fetch_unified_by_title,
)

# Import functions from the user_utils module
from .user_utils import calculate_reading_streak, get_reading_streak

__all__ = [
    "fetch_book_data",
    "get_google_books_cover",
    "fetch_author_data",
    "generate_month_review_image",
    "normalize_goodreads_value",
    "search_author_by_name",
    "search_book_by_title_author",
    "search_multiple_books_by_title_author",
    "search_google_books_by_title_author",
    "calculate_reading_streak",
    "get_reading_streak",
    "fetch_unified_by_isbn",
    "fetch_unified_by_title",
]
