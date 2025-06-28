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
from ..domain.models import Book as DomainBook, Author, Publisher

# Create API blueprint
books_api = Blueprint('books_api', __name__, url_prefix='/api/v1/books')


def serialize_book(domain_book):
    """Convert domain book to API response format."""
    return {
        'id': domain_book.id,
        'title': domain_book.title,
        'isbn13': domain_book.isbn13,
        'isbn10': domain_book.isbn10,
        'authors': [author.name for author in domain_book.authors] if domain_book.authors else [],
        'publisher': domain_book.publisher.name if domain_book.publisher else None,
        'published_date': domain_book.published_date.isoformat() if domain_book.published_date else None,
        'page_count': domain_book.page_count,
        'language': domain_book.language,
        'description': domain_book.description,
        'cover_url': domain_book.cover_url,
        'categories': domain_book.categories,
        'average_rating': domain_book.average_rating,
        'rating_count': domain_book.rating_count,
        'created_at': domain_book.created_at.isoformat() if domain_book.created_at else None,
        'updated_at': domain_book.updated_at.isoformat() if domain_book.updated_at else None
    }


def parse_book_data(data):
    """Parse JSON data into domain book object."""
    # Parse authors
    authors = []
    if 'authors' in data and data['authors']:
        if isinstance(data['authors'], list):
            authors = [Author(name=name.strip()) for name in data['authors'] if name.strip()]
        else:
            authors = [Author(name=data['authors'].strip())]
    
    # Parse publisher
    publisher = None
    if 'publisher' in data and data['publisher']:
        publisher = Publisher(name=data['publisher'].strip())
    
    # Parse dates
    published_date = None
    if 'published_date' in data and data['published_date']:
        try:
            published_date = datetime.fromisoformat(data['published_date']).date()
        except (ValueError, TypeError):
            pass
    
    # Create domain book
    domain_book = DomainBook(
        id=str(data.get('id', '')),
        title=data.get('title', '').strip(),
        isbn13=data.get('isbn13', '').strip() or None,
        isbn10=data.get('isbn10', '').strip() or None,
        authors=authors,
        publisher=publisher,
        published_date=published_date,
        page_count=data.get('page_count') or None,
        language=data.get('language', '').strip() or 'en',
        description=data.get('description', '').strip() or None,
        cover_url=data.get('cover_url', '').strip() or None,
        raw_categories=data.get('categories'),  # Use raw_categories for automatic processing
        average_rating=data.get('average_rating') or None,
        rating_count=data.get('rating_count') or None,
        created_at=datetime.now(),
        updated_at=datetime.now()
    )
    
    return domain_book


@books_api.route('', methods=['GET'])
@api_token_required
def get_books():
    """Get all books for the current user."""
    try:
        # Use service layer to get user books
        domain_books = book_service.get_user_books_sync(current_user.id)
        
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
        # Use service layer to get book
        domain_book = book_service.get_book_by_uid_sync(book_id, current_user.id)
        
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
        
        # Create book using service layer
        created_book = book_service.find_or_create_book_sync(domain_book)
        
        # Add to user's library
        if created_book:
            from ..domain.models import ReadingStatus
            book_service.add_book_to_user_library_sync(
                user_id=current_user.id,
                book_id=created_book.id,
                reading_status=ReadingStatus.PLAN_TO_READ
            )
        
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
        
        # Parse book data
        domain_book = parse_book_data(data)
        
        # Update book using service layer
        updated_book = book_service.update_book_sync(domain_book, current_user.id)
        
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
        # Delete book using service layer
        success = book_service.delete_book_sync(book_id, current_user.id)
        
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
        
        # For now, use the existing get_user_books and filter in Python
        # TODO: Implement proper search in service layer
        domain_books = book_service.get_user_books_sync(current_user.id)
        
        # Apply filters
        filtered_books = []
        for book in domain_books:
            matches = True
            
            # Text search in title, description, authors
            if query:
                search_text = f"{book.title} {book.description or ''}"
                if book.authors:
                    search_text += " " + " ".join(author.name for author in book.authors)
                if query.lower() not in search_text.lower():
                    matches = False
            
            # Category filter
            if category and category not in (book.categories or []):
                matches = False
            
            # Publisher filter
            if publisher and (not book.publisher or publisher.lower() != book.publisher.name.lower()):
                matches = False
            
            # Language filter
            if language and language.lower() != (book.language or 'en').lower():
                matches = False
            
            # Author filter
            if author:
                if not book.authors or not any(author.lower() in auth.name.lower() for auth in book.authors):
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
