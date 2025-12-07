"""
Book Management API Endpoints

Provides RESTful CRUD operations for books using the dual-write service layer.
Focuses on graph database functionality while maintaining SQLite compatibility.
Uses secure API token authentication to bypass CSRF for legitimate API calls.
"""

from flask import Blueprint, request, jsonify, current_app
from flask_login import current_user
from datetime import datetime
import traceback

from ..api_auth import api_token_required, api_auth_optional
from ..services import book_service
from ..services.kuzu_service_facade import KuzuServiceFacade as KuzuBookService
from ..domain.models import Book as DomainBook, Author, Publisher, BookContribution, ContributionType
from ..utils.unified_metadata import fetch_unified_by_isbn, fetch_unified_by_title

# Create API blueprint
books_api = Blueprint('books_api', __name__, url_prefix='/api/v1/books')

# Initialize the correct services
kuzu_book_service = KuzuBookService()
# Note: KuzuUserBookService was removed - functionality is now part of the facade


def serialize_book(domain_book):
    """Convert domain book to API response format."""
    def format_date(date_obj):
        """Helper function to format date objects to ISO string."""
        if date_obj and hasattr(date_obj, 'isoformat'):
            return date_obj.isoformat()
        return None
    
    # Handle both domain objects and dictionaries
    if hasattr(domain_book, '__dict__'):
        # Domain object
        return {
            'id': getattr(domain_book, 'id', None),
            'title': getattr(domain_book, 'title', ''),
            'subtitle': getattr(domain_book, 'subtitle', None),
            'isbn13': getattr(domain_book, 'isbn13', None),
            'isbn10': getattr(domain_book, 'isbn10', None),
            'asin': getattr(domain_book, 'asin', None),
            'authors': [author.name for author in getattr(domain_book, 'authors', [])] if hasattr(domain_book, 'authors') else [],
            'publisher': getattr(domain_book.publisher, 'name', None) if getattr(domain_book, 'publisher', None) else None,
            'published_date': format_date(getattr(domain_book, 'published_date', None)),
            'page_count': getattr(domain_book, 'page_count', None),
            'language': getattr(domain_book, 'language', 'en'),
            'description': getattr(domain_book, 'description', None),
            'cover_url': getattr(domain_book, 'cover_url', None),
            'google_books_id': getattr(domain_book, 'google_books_id', None),
            'openlibrary_id': getattr(domain_book, 'openlibrary_id', None),
            'categories': getattr(domain_book, 'categories', []),
            'average_rating': getattr(domain_book, 'average_rating', None),
            'rating_count': getattr(domain_book, 'rating_count', None),
            'series': getattr(domain_book, 'series', None),
            'series_volume': getattr(domain_book, 'series_volume', None),
            'series_order': getattr(domain_book, 'series_order', None),
            'created_at': format_date(getattr(domain_book, 'created_at', None)),
            'updated_at': format_date(getattr(domain_book, 'updated_at', None))
        }
    else:
        # Dictionary - return as is with some processing
        return {
            'id': domain_book.get('id'),
            'title': domain_book.get('title', ''),
            'subtitle': domain_book.get('subtitle'),
            'isbn13': domain_book.get('isbn13'),
            'isbn10': domain_book.get('isbn10'),
            'asin': domain_book.get('asin'),
            'authors': [author.get('name') if isinstance(author, dict) else str(author) for author in domain_book.get('authors', [])],
            'publisher': domain_book.get('publisher', {}).get('name') if isinstance(domain_book.get('publisher'), dict) else domain_book.get('publisher'),
            'published_date': domain_book.get('published_date'),
            'page_count': domain_book.get('page_count'),
            'language': domain_book.get('language', 'en'),
            'description': domain_book.get('description'),
            'cover_url': domain_book.get('cover_url'),
            'google_books_id': domain_book.get('google_books_id'),
            'openlibrary_id': domain_book.get('openlibrary_id'),
            'categories': domain_book.get('categories', []),
            'average_rating': domain_book.get('average_rating'),
            'rating_count': domain_book.get('rating_count'),
            'series': domain_book.get('series'),
            'series_volume': domain_book.get('series_volume'),
            'series_order': domain_book.get('series_order'),
            'created_at': domain_book.get('created_at'),
            'updated_at': domain_book.get('updated_at')
        }


def parse_book_data(data):
    """Parse JSON data into domain book object."""
    # Parse contributors (authors)
    contributors = []
    if 'authors' in data and data['authors']:
        if isinstance(data['authors'], list):
            for i, name in enumerate(data['authors']):
                if name.strip():
                    # Create Person for author
                    person = Publisher(name=name.strip())  # Temporary use of Publisher as Person-like
                    # Create BookContribution
                    contribution = BookContribution(
                        person_id=str(person),  # Temporary
                        book_id="",  # Will be set later
                        contribution_type=ContributionType.AUTHORED,
                        order=i
                    )
                    contributors.append(contribution)
        else:
            # Single author
            person = Publisher(name=data['authors'].strip())  # Temporary
            contribution = BookContribution(
                person_id=str(person),
                book_id="",
                contribution_type=ContributionType.AUTHORED,
                order=0
            )
            contributors.append(contribution)
    
    # Parse publisher
    publisher = None
    if 'publisher' in data and data['publisher']:
        name = str(data['publisher']).strip()
        if name.startswith('"') and name.endswith('"') and len(name) >= 2:
            name = name[1:-1].strip()
        publisher = Publisher(name=name)
    
    # Parse dates
    published_date = None
    if 'published_date' in data and data['published_date']:
        try:
            published_date = datetime.fromisoformat(data['published_date']).date()
        except (ValueError, TypeError):
            pass
    
    # Create domain book with all fields from comprehensive documentation
    domain_book = DomainBook(
        id=str(data.get('id', '')),
        title=data.get('title', '').strip(),
        normalized_title=data.get('title', '').strip().lower(),
        subtitle=data.get('subtitle', '').strip() or None,
        isbn13=data.get('isbn13', '').strip() or None,
        isbn10=data.get('isbn10', '').strip() or None,
        asin=data.get('asin', '').strip() or None,
        description=data.get('description', '').strip() or None,
        published_date=published_date,
        page_count=data.get('page_count') or None,
        language=data.get('language', '').strip() or 'en',
        cover_url=data.get('cover_url', '').strip() or None,
        google_books_id=data.get('google_books_id', '').strip() or None,
        openlibrary_id=data.get('openlibrary_id', '').strip() or None,
        average_rating=data.get('average_rating') or None,
        rating_count=data.get('rating_count') or None,
        # Series information
        series_volume=data.get('series_volume', '').strip() or None,
        series_order=data.get('series_order') or None,
        # Raw categories for processing
        raw_categories=data.get('categories'),
        # Relationships
        contributors=contributors,
        publisher=publisher,
        # Timestamps
        created_at=datetime.now(),
        updated_at=datetime.now()
    )
    
    return domain_book


@books_api.route('', methods=['GET'])
@api_token_required
def get_books():
    """Get all books for the current user."""
    try:
        # Use service layer with global book visibility
        domain_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
        
        # Convert to API response format
        books_data = [serialize_book(book) for book in domain_books]
        
        return jsonify({
            'status': 'success',
            'data': books_data,
            'count': len(books_data)
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Error getting books: {e}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': 'Failed to retrieve books',
            'error': str(e)
        }), 500


@books_api.route('/<book_id>', methods=['GET'])
@api_token_required
def get_book(book_id):
    """Get a specific book by ID."""
    try:
        # Use KuzuBookService to get book for user
        domain_book = kuzu_book_service.get_book_by_uid_sync(book_id, current_user.id)
        
        if not domain_book:
            return jsonify({
                'status': 'error',
                'message': 'Book not found'
            }), 404
        
        return jsonify({
            'status': 'success',
            'data': serialize_book(domain_book)
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Error getting book {book_id}: {e}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': 'Failed to retrieve book',
            'error': str(e)
        }), 500


@books_api.route('', methods=['POST'])
@api_token_required
def create_book():
    """Create a new book."""
    try:
        if not request.json:
            return jsonify({
                'status': 'error',
                'message': 'JSON data required'
            }), 400
        
        data = request.json
        
        # Validate required fields
        if not data.get('title'):
            return jsonify({
                'status': 'error',
                'message': 'Title is required'
            }), 400
        
        # Parse book data
        domain_book = parse_book_data(data)
        
        # Create book using KuzuBookService
        created_book = kuzu_book_service.create_book_sync(domain_book, current_user.id)
        
        if not created_book:
            return jsonify({
                'status': 'error',
                'message': 'Failed to create book'
            }), 500
        
        return jsonify({
            'status': 'success',
            'message': 'Book created successfully',
            'data': serialize_book(created_book)
        }), 201
        
    except Exception as e:
        current_app.logger.error(f"Error creating book: {e}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': 'Failed to create book',
            'error': str(e)
        }), 500


@books_api.route('/<book_id>', methods=['PUT'])
@api_token_required
def update_book(book_id):
    """Update an existing book."""
    try:
        if not request.json:
            return jsonify({
                'status': 'error',
                'message': 'JSON data required'
            }), 400
        
        data = request.json
        data['id'] = book_id  # Ensure ID is set
        
        # Get the updates as a dictionary (excluding domain-specific parsing for now)
        updates = {k: v for k, v in data.items() if k != 'id'}
        
        # Update book using KuzuBookService
        updated_book = kuzu_book_service.update_book_sync(book_id, current_user.id, **updates)
        
        if not updated_book:
            return jsonify({
                'status': 'error',
                'message': 'Book not found or update failed'
            }), 404
        
        return jsonify({
            'status': 'success',
            'message': 'Book updated successfully',
            'data': serialize_book(updated_book)
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Error updating book {book_id}: {e}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': 'Failed to update book',
            'error': str(e)
        }), 500


@books_api.route('/<book_id>', methods=['DELETE'])
@api_token_required
def delete_book(book_id):
    """Delete a book."""
    try:
        # Delete book using KuzuBookService
        success = kuzu_book_service.delete_book_sync(book_id, current_user.id)
        
        if not success:
            return jsonify({
                'status': 'error',
                'message': 'Book not found or deletion failed'
            }), 404
        
        return jsonify({
            'status': 'success',
            'message': 'Book deleted successfully'
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Error deleting book {book_id}: {e}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': 'Failed to delete book',
            'error': str(e)
        }), 500


@books_api.route('/search', methods=['GET'])
@api_token_required
def search_books():
    """Search books with various filters."""
    try:
        # Get query parameters
        query = request.args.get('q', '').strip()
        category = request.args.get('category', '').strip()
        publisher = request.args.get('publisher', '').strip()
        language = request.args.get('language', '').strip()
        author = request.args.get('author', '').strip()
        
        # Use KuzuBookService to get user's books
        books = kuzu_book_service.get_user_books_sync(str(current_user.id))
        
        # Apply filters
        filtered_books = []
        for book in books:
            matches = True
            
            # Text search in title, description, authors
            if query:
                search_text = f"{book.title} {book.description or ''}"
                if book.contributors:
                    # Get author names from contributors
                    author_names = []
                    for contrib in book.contributors:
                        if contrib.person and contrib.person.name:
                            author_names.append(contrib.person.name)
                    if author_names:
                        search_text += " " + " ".join(author_names)
                if query.lower() not in search_text.lower():
                    matches = False
            
            # Category filter
            if category:
                book_categories = [cat.name if hasattr(cat, 'name') else str(cat) for cat in book.categories]
                if category not in book_categories:
                    matches = False
            
            # Publisher filter
            if publisher:
                publisher_name = ""
                if book.publisher:
                    if hasattr(book.publisher, 'name'):
                        publisher_name = book.publisher.name
                    else:
                        publisher_name = str(book.publisher)
                
                if publisher.lower() != publisher_name.lower():
                    matches = False
            
            # Language filter
            if language:
                book_language = book.language or 'en'
                if language.lower() != book_language.lower():
                    matches = False
            
            # Author filter
            if author:
                author_found = False
                for contrib in book.contributors:
                    if contrib.person and contrib.person.name:
                        if author.lower() in contrib.person.name.lower():
                            author_found = True
                            break
                if not author_found:
                    matches = False
            
            if matches:
                filtered_books.append(book)
        
        # Convert to API response format
        books_data = [serialize_book(book) for book in filtered_books]
        
        return jsonify({
            'status': 'success',
            'data': books_data,
            'count': len(books_data),
            'query': {
                'q': query,
                'category': category,
                'publisher': publisher,
                'language': language,
                'author': author
            }
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Error searching books: {e}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': 'Search failed',
            'error': str(e)
        }), 500


@books_api.route('/user-search', methods=['GET'])
@api_auth_optional
def search_user_books():
    """Search user's personal book collection."""
    try:
        if not current_user.is_authenticated:
            return jsonify({
                'status': 'error',
                'message': 'Authentication required'
            }), 401
        
        # Get query parameters
        query = request.args.get('q', '').strip()
        limit = min(int(request.args.get('limit', 10)), 50)  # Max 50 results
        
        if not query or len(query) < 2:
            return jsonify({
                'status': 'success',
                'books': [],
                'count': 0
            })
        
        # Search user's books using the book service
        results = book_service.search_user_books_sync(current_user.id, query, limit=limit)
        
        if not results:
            return jsonify({
                'status': 'success',
                'books': [],
                'count': 0
            })
        
        # Format results for frontend
        books = []
        for book in results:
            books.append({
                'id': book.get('id'),
                'uid': book.get('uid'),
                'title': book.get('title', ''),
                'authors_text': book.get('authors_text', 'Unknown Author'),
                'cover_url': book.get('cover_url'),
                'page_count': book.get('page_count'),
                'published_date': book.get('published_date'),
                'reading_status': book.get('reading_status')
            })
        
        return jsonify({
            'status': 'success',
            'books': books,
            'count': len(books),
            'query': query
        })
        
    except Exception as e:
        current_app.logger.error(f"Error searching user books: {e}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': 'Search failed',
            'error': str(e)
        }), 500


@books_api.route('/external-search', methods=['GET'])
@api_token_required
def search_external_books():
    """
    Search for books across Google Books and OpenLibrary APIs by title.
    
    This endpoint searches external APIs (not the user's collection) and ranks results
    by title similarity. Returns comprehensive book data for potential addition to library.
    
    Query Parameters:
        - title: Book title to search for (required)
        - limit: Maximum number of results (default: 10, max: 20)
        - isbn_required: Only return books with ISBN (default: false)
    
    Returns:
        JSON with search results containing:
        - Display fields: title, author, publication_year, page_count
        - Full book data for each result (stored in full_data field)
        - Search metadata and similarity scoring
    """
    try:
        # Get query parameters
        title = request.args.get('title', '').strip()
        author = request.args.get('author', '').strip()
        limit = min(int(request.args.get('limit', 10)), 20)  # Max 20 results
        isbn_required = request.args.get('isbn_required', 'false').lower() == 'true'

        if not title or len(title) < 2:
            return jsonify({
                'status': 'error',
                'message': 'Title parameter is required (minimum 2 characters)'
            }), 400

        # Import the search function
        from app.utils.book_search import search_books_with_display_fields

        # Perform the search with author-aware ranking
        search_results = search_books_with_display_fields(title, limit, isbn_required=isbn_required, author=author or None)

        return jsonify({
            'status': 'success',
            'data': search_results['results'],
            'metadata': search_results['metadata'],
            'count': len(search_results['results'])
        }), 200

    except ImportError as e:
        current_app.logger.error(f"Error importing book search module: {e}")
        return jsonify({
            'status': 'error',
            'message': 'Book search functionality not available',
            'error': str(e)
        }), 500

    except Exception as e:
        current_app.logger.error(f"Error searching external books: {e}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': 'External search failed',
            'error': str(e)
        }), 500


@books_api.route('/unified-metadata', methods=['GET'])
@api_token_required
def unified_metadata_lookup():
    """
    Unified metadata lookup by ISBN or title.

    Query parameters:
      - isbn: Lookup a single book by ISBN, merging Google Books and OpenLibrary
      - title: Search by title across Google Books and OpenLibrary

    If both are provided, ISBN takes precedence.
    """
    try:
        isbn = (request.args.get('isbn') or '').strip()
        title = (request.args.get('title') or '').strip()
        author = (request.args.get('author') or '').strip()

        if not isbn and not title:
            return jsonify({
                'status': 'error',
                'message': 'Provide either isbn or title'
            }), 400

        if isbn:
            from app.utils.unified_metadata import fetch_unified_by_isbn_detailed
            data, errors = fetch_unified_by_isbn_detailed(isbn)
            
            # Extract metadata warnings for the UI
            warnings = data.pop('_metadata_warnings', [])
            isbn_mismatch = data.pop('_isbn_mismatch', False)
            requested_isbn = data.pop('_requested_isbn', isbn)
            
            response_data = {
                'status': 'success',
                'mode': 'isbn',
                'isbn': isbn,
                'data': data
            }
            
            # Add warnings if present
            if warnings:
                response_data['warnings'] = warnings
            if isbn_mismatch:
                response_data['isbn_mismatch'] = True
            if errors:
                response_data['provider_errors'] = errors
            
            return jsonify(response_data), 200

        # title search
        results = fetch_unified_by_title(
            title,
            max_results=min(int(request.args.get('limit', 10)), 20),
            author=author or None,
        )
        return jsonify({
            'status': 'success',
            'mode': 'title',
            'title': title,
            'author': author if author else None,
            'count': len(results),
            'results': results
        }), 200

    except Exception as e:
        current_app.logger.error(f"Error in unified metadata lookup: {e}")
        current_app.logger.error(traceback.format_exc())
        return jsonify({
            'status': 'error',
            'message': 'Unified metadata lookup failed',
            'error': str(e)
        }), 500
