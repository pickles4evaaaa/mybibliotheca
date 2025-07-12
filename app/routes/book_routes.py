"""
Core book management routes for the Bibliotheca application.
Handles book CRUD operations, library views, and book-specific actions.
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, jsonify, abort, make_response, send_file
from flask_login import login_required, current_user
from datetime import datetime, date
import uuid
import traceback
import requests
import re
import csv
import io
from pathlib import Path
from io import BytesIO

from app.services import book_service, reading_log_service, custom_field_service, user_service
from app.simplified_book_service import SimplifiedBookService, SimplifiedBook
from app.utils import fetch_book_data, get_google_books_cover, fetch_author_data, generate_month_review_image
from app.domain.models import Book as DomainBook

# Create book blueprint
book_bp = Blueprint('book', __name__)

def _convert_published_date_to_date(published_date_str):
    """Convert published_date string to date object using enhanced date parser."""
    if not published_date_str or not isinstance(published_date_str, str):
        return None
    
    try:
        # Simple date parsing - handle common formats
        from datetime import datetime
        # Try common date formats
        formats = ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y', '%Y']
        for fmt in formats:
            try:
                return datetime.strptime(published_date_str.strip(), fmt).date()
            except ValueError:
                continue
        return None
    except Exception as e:
        current_app.logger.warning(f"Failed to parse published date '{published_date_str}': {e}")
        return None

def _safe_date_to_isoformat(date_obj):
    """Safely convert date object to ISO format string."""
    if date_obj and hasattr(date_obj, 'isoformat'):
        return date_obj.isoformat()
    return None

@book_bp.route('/fetch_book/<isbn>', methods=['GET'])
def fetch_book(isbn):
    book_data = fetch_book_data(isbn) or {}
    
    # Get comprehensive data from Google Books including description
    google_data = get_google_books_cover(isbn, fetch_title_author=True)
    if google_data:
        # Merge Google Books data, prioritizing existing data
        for key, value in google_data.items():
            if key not in book_data or not book_data[key]:
                book_data[key] = value
        # Ensure cover field is set correctly
        if google_data.get('cover'):
            book_data['cover'] = google_data['cover']
    
    # If neither source provides a cover, set a default
    if not book_data.get('cover'):
        book_data['cover'] = url_for('serve_static', filename='bookshelf.png')
    return jsonify(book_data), 200 if book_data else 404

@book_bp.route('/')
@login_required
def index():
    """Redirect to library page as the main landing page"""
    return redirect(url_for('main.library'))

@book_bp.route('/add', methods=['GET', 'POST'])
@login_required
def add_book():
    """Add a new book to the library"""
    if request.method == 'GET':
        # Load existing custom fields for the user
        try:
            personal_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=False)
            global_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=True)
        except Exception as e:
            current_app.logger.error(f"Error loading custom fields for add book: {e}")
            personal_fields = []
            global_fields = []
        
        return render_template('add_book_new.html', 
                             personal_fields=personal_fields,
                             global_fields=global_fields)
    
    # Handle POST request for adding book
    # Forward to the manual add handler
    return add_book_manual()

@book_bp.route('/add/image', methods=['POST'])
@login_required
def add_book_from_image():
        """Handle image upload for ISBN extraction and book addition"""
        try:
            # Check if file was uploaded
            if 'image' not in request.files:
                return jsonify({'error': 'No image file provided'}), 400
            
            file = request.files['image']
            if file.filename == '':
                return jsonify({'error': 'No file selected'}), 400
            
            # TODO: Implement image processing and ISBN extraction
            # This would involve:
            # 1. Save uploaded image temporarily
            # 2. Use OCR to extract text from image
            # 3. Parse extracted text to find ISBN
            # 4. Look up book by ISBN
            # 5. Add book to user's library
            
            return jsonify({'error': 'Image upload feature not implemented yet'}), 501
            
        except Exception as e:
            print(f"❌ [IMAGE_UPLOAD] Error: {e}")
            return jsonify({'error': 'Failed to process image'}), 500

@book_bp.route('/search', methods=['GET', 'POST'])
@login_required
def search_books():
    """Redirect to library page where search functionality is now integrated"""
    flash('Search functionality has been moved to the Library page for a better experience.', 'info')
    return redirect(url_for('main.library'))

@book_bp.route('/month_wrapup')
@login_required
def month_wrapup():
    """Redirect to stats page where month wrap-up is now integrated"""
    flash('Month Wrap-up has been moved to the Stats page along with other reading statistics.', 'info')
    return redirect(url_for('main.stats'))

@book_bp.route('/community_activity')
@login_required
def community_activity():
    """Redirect to stats page where community activity is now integrated"""
    flash('Community activity has been moved to the Stats page along with your personal reading statistics.', 'info')
    return redirect(url_for('main.stats'))

@book_bp.route('/bulk_import', methods=['GET', 'POST'])
@login_required
def bulk_import():
    """Redirect to new import interface."""
    flash('Book import has been upgraded! You can now map CSV fields and track progress in real-time.', 'info')
    return redirect(url_for('main.import_books'))

# Legacy route removed - all book views now use enhanced view directly

@book_bp.route('/book/<uid>/log', methods=['POST'])
@login_required
def log_reading(uid):
    # Get book through service layer
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
    
    log_date_str = request.form.get('log_date')
    log_date = datetime.strptime(log_date_str, '%Y-%m-%d').date() if log_date_str else date.today()
    
    # Check for existing log using service layer
    book_id = user_book.get('id') if isinstance(user_book, dict) else getattr(user_book, 'id', None)
    if book_id:
        existing_log = reading_log_service.get_existing_log_sync(book_id, current_user.id, log_date)
        if existing_log:
            flash('You have already logged reading for this day.')
        else:
            # Create reading log using service layer
            reading_log_service.create_reading_log_sync(book_id, current_user.id, log_date)
            flash('Reading day logged.')
    else:
        flash('Error: Book ID not found.', 'error')
    return redirect(url_for('book.view_book_enhanced', uid=uid))

@book_bp.route('/book/<uid>/delete', methods=['POST'])
@login_required
def delete_book(uid):
    # Delete book through service layer
    success = book_service.delete_book_sync(uid, str(current_user.id))
    
    if success:
        flash('Book deleted from your library.')
    else:
        flash('Failed to delete book.', 'error')
        
    return redirect(url_for('main.library'))

@book_bp.route('/book/<uid>/toggle_finished', methods=['POST'])
@login_required
def toggle_finished(uid):
    # Get book through service layer
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
    
    finish_date = user_book.get('finish_date') if isinstance(user_book, dict) else getattr(user_book, 'finish_date', None)
    if finish_date:
        # Mark as currently reading
        book_service.update_book_sync(uid, str(current_user.id), finish_date=None)
        flash('Book marked as currently reading.')
    else:
        # Mark as finished
        book_service.update_book_sync(uid, str(current_user.id), finish_date=date.today())
        flash('Book marked as finished.')
    
    return redirect(url_for('book.view_book_enhanced', uid=uid))

@book_bp.route('/book/<uid>/start_reading', methods=['POST'])
@login_required
def start_reading(uid):
    # Get book through service layer
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
    
    update_data = {'want_to_read': False}
    start_date = user_book.get('start_date') if isinstance(user_book, dict) else getattr(user_book, 'start_date', None)
    if not start_date:
        update_data['start_date'] = datetime.today().date()  # type: ignore
    
    book_service.update_book_sync(uid, str(current_user.id), **update_data)
    title = user_book.get('title', 'Unknown Title') if isinstance(user_book, dict) else getattr(user_book, 'title', 'Unknown Title')
    flash(f'Started reading "{title}".')
    return redirect(url_for('main.library'))

@book_bp.route('/book/<uid>/update_status', methods=['POST'])
@login_required
def update_status(uid):
    # Get book through service layer
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
    
    # Set status based on checkboxes
    want_to_read = 'want_to_read' in request.form
    library_only = 'library_only' in request.form
    finished = 'finished' in request.form
    currently_reading = 'currently_reading' in request.form

    update_data = {
        'want_to_read': want_to_read,
        'library_only': library_only
    }

    if finished:
        update_data.update({  # type: ignore
            'finish_date': datetime.now().date(),
            'want_to_read': False,
            'library_only': False
        })
    elif currently_reading:
        update_data.update({  # type: ignore
            'finish_date': None,
            'want_to_read': False,
            'library_only': False
        })
        start_date = user_book.get('start_date') if isinstance(user_book, dict) else getattr(user_book, 'start_date', None)
        if not start_date:
            update_data['start_date'] = datetime.now().date()  # type: ignore
    elif want_to_read:
        update_data.update({  # type: ignore
            'finish_date': None,
            'library_only': False
        })
    elif library_only:
        update_data.update({  # type: ignore
            'finish_date': None,
            'want_to_read': False
        })

    book_service.update_book_sync(uid, str(current_user.id), **update_data)
    flash('Book status updated.')
    return redirect(url_for('book.view_book_enhanced', uid=uid))

@book_bp.route('/library')
@login_required
def library():
    # Get filter parameters from URL - default to "all" to show all books
    status_filter = request.args.get('status_filter', 'all')  # Changed from 'reading' to 'all'
    category_filter = request.args.get('category', '')
    publisher_filter = request.args.get('publisher', '')
    language_filter = request.args.get('language', '')
    location_filter = request.args.get('location', '')
    search_query = request.args.get('search', '')
    sort_option = request.args.get('sort', 'title_asc')  # Default to title A-Z

    # Use service layer with global book visibility
    user_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
    
    # Add location debugging via debug system
    from app.debug_system import debug_log
    debug_log(f"Retrieved {len(user_books)} books for user {current_user.id}", "LIBRARY")
    books_with_locations = 0
    books_without_locations = 0
    location_counts = {}
    
    for book in user_books:
        # Handle both dict and object formats for compatibility
        book_title = book.get('title') if isinstance(book, dict) else getattr(book, 'title', 'Unknown Title')
        book_locations = book.get('locations') if isinstance(book, dict) else getattr(book, 'locations', None)
        
        if book_locations:
            books_with_locations += 1
            for location in book_locations:
                # Extract location ID from location object/dict
                loc_id = location.get('id') if isinstance(location, dict) else getattr(location, 'id', location)
                location_counts[loc_id] = location_counts.get(loc_id, 0) + 1
            debug_log(f"Book '{book_title}' has locations: {book_locations}", "LIBRARY")
        else:
            books_without_locations += 1
            debug_log(f"Book '{book_title}' has NO locations", "LIBRARY")
    
    debug_log(f"Summary: {books_with_locations} books WITH locations, {books_without_locations} books WITHOUT locations", "LIBRARY")
    debug_log(f"Location distribution: {location_counts}", "LIBRARY")
    
    # Calculate statistics for filter buttons - handle both dict and object formats
    def get_reading_status(book):
        if isinstance(book, dict):
            return book.get('ownership', {}).get('reading_status')
        return getattr(book, 'reading_status', None)
    
    def get_ownership_status(book):
        if isinstance(book, dict):
            return book.get('ownership', {}).get('ownership_status')
        return getattr(book, 'ownership_status', None)
    
    stats = {
        'total_books': len(user_books),
        'books_read': len([b for b in user_books if get_reading_status(b) == 'read']),
        'currently_reading': len([b for b in user_books if get_reading_status(b) == 'reading']),
        'want_to_read': len([b for b in user_books if get_reading_status(b) == 'plan_to_read']),
        'on_hold': len([b for b in user_books if get_reading_status(b) == 'on_hold']),
        'wishlist': len([b for b in user_books if get_ownership_status(b) == 'wishlist']),
        # Add location stats
        'books_with_locations': books_with_locations,
        'books_without_locations': books_without_locations,
        'location_counts': location_counts
    }
    
    # Apply status filter first
    filtered_books = user_books
    if status_filter and status_filter != 'all':
        if status_filter == 'wishlist':
            filtered_books = [book for book in filtered_books if get_ownership_status(book) == 'wishlist']
        else:
            filtered_books = [book for book in filtered_books if get_reading_status(book) == status_filter]
    
    # Apply other filters
    if search_query:
        search_lower = search_query.lower()
        filtered_books = [
            book for book in filtered_books 
            if (search_lower in (book.get('title', '') if isinstance(book, dict) else getattr(book, 'title', '')).lower()) or
               (search_lower in (book.get('author', '') if isinstance(book, dict) else getattr(book, 'author', '')).lower()) or
               (search_lower in (book.get('description', '') if isinstance(book, dict) else getattr(book, 'description', '')).lower())
        ]
    
    if publisher_filter:
        filtered_books = [
            book for book in filtered_books 
            if (book.get('publisher') if isinstance(book, dict) else getattr(book, 'publisher', None)) and 
               publisher_filter.lower() in (book.get('publisher', '') if isinstance(book, dict) else getattr(book, 'publisher', '')).lower()
        ]
    
    if language_filter:
        filtered_books = [
            book for book in filtered_books 
            if (book.get('language') if isinstance(book, dict) else getattr(book, 'language', None)) == language_filter
        ]
    
    if location_filter:
        # Handle locations which are now returned as strings (location names) from KuzuIntegrationService
        filtered_books = [
            book for book in filtered_books 
            if (book.get('locations') if isinstance(book, dict) else getattr(book, 'locations', None)) and any(
                location_filter.lower() in (loc.lower() if isinstance(loc, str) else 
                                           (loc.get('name', '') if isinstance(loc, dict) else getattr(loc, 'name', '')).lower())
                for loc in (book.get('locations', []) if isinstance(book, dict) else getattr(book, 'locations', []))
            )
        ]
    
    if category_filter:
        filtered_books = [
            book for book in filtered_books 
            if (book.get('categories') if isinstance(book, dict) else getattr(book, 'categories', None)) and any(
                category_filter.lower() in (cat.get('name', '') if isinstance(cat, dict) else getattr(cat, 'name', '')).lower() 
                for cat in (book.get('categories', []) if isinstance(book, dict) else getattr(book, 'categories', []))
            )
        ]

    # Apply sorting
    def get_author_name(book):
        """Helper function to get author name safely"""
        if isinstance(book, dict):
            authors = book.get('authors', [])
            author = book.get('author', '')
            if authors and isinstance(authors, list) and len(authors) > 0:
                first_author = authors[0]
                if isinstance(first_author, dict):
                    return first_author.get('name', 'Unknown Author')
                elif hasattr(first_author, 'name'):
                    return first_author.name
                else:
                    return str(first_author)
            elif author:
                return author
            return "Unknown Author"
        else:
            # Handle object format
            if hasattr(book, 'authors') and book.authors:
                # Handle list of Author objects
                if isinstance(book.authors, list) and len(book.authors) > 0:
                    author = book.authors[0]
                    if hasattr(author, 'name'):
                        return author.name
                    elif hasattr(author, 'first_name') and hasattr(author, 'last_name'):
                        return f"{author.first_name} {author.last_name}".strip()
                    else:
                        return str(author)
                else:
                    return str(book.authors)
            elif hasattr(book, 'author') and book.author:
                return book.author
            return "Unknown Author"
    
    def get_author_last_first(book):
        """Helper function to get author name in Last, First format"""
        author_name = get_author_name(book)
        if ',' in author_name:
            return author_name  # Already in Last, First format
        name_parts = author_name.split()
        if len(name_parts) >= 2:
            last_name = name_parts[-1]
            first_names = ' '.join(name_parts[:-1])
            return f"{last_name}, {first_names}"
        return author_name
    
    if sort_option == 'title_asc':
        filtered_books.sort(key=lambda x: (x.get('title', '') if isinstance(x, dict) else getattr(x, 'title', '')).lower())
    elif sort_option == 'title_desc':
        filtered_books.sort(key=lambda x: (x.get('title', '') if isinstance(x, dict) else getattr(x, 'title', '')).lower(), reverse=True)
    elif sort_option == 'author_first_asc':
        filtered_books.sort(key=lambda x: get_author_name(x).lower())
    elif sort_option == 'author_first_desc':
        filtered_books.sort(key=lambda x: get_author_name(x).lower(), reverse=True)
    elif sort_option == 'author_last_asc':
        filtered_books.sort(key=lambda x: get_author_last_first(x).lower())
    elif sort_option == 'author_last_desc':
        filtered_books.sort(key=lambda x: get_author_last_first(x).lower(), reverse=True)
    else:
        # Default to title A-Z
        filtered_books.sort(key=lambda x: (x.get('title', '') if isinstance(x, dict) else getattr(x, 'title', '')).lower())

    # Books are already in the right format for the template
    books = filtered_books

    # Convert dictionary books to object-like structures for template compatibility
    converted_books = []
    for book in books:
        if isinstance(book, dict):
            # Create an object-like structure that the template can work with
            class BookObj:
                def __init__(self, data):
                    for key, value in data.items():
                        setattr(self, key, value)
                    # Ensure common attributes have defaults
                    # Note: authors property is derived from contributors, don't set directly
                    if not hasattr(self, 'contributors'):
                        self.contributors = []
                    if not hasattr(self, 'categories'):
                        self.categories = []
                    if not hasattr(self, 'publisher'):
                        self.publisher = None
                    if not hasattr(self, 'series'):
                        self.series = None
                    if not hasattr(self, 'locations'):
                        self.locations = []
                    # Handle ownership data
                    ownership = data.get('ownership', {})
                    for key, value in ownership.items():
                        setattr(self, key, value)
                
                def get_contributors_by_type(self, contribution_type):
                    """Get contributors by type for template compatibility."""
                    if hasattr(self, 'contributors') and self.contributors:
                        return [c for c in self.contributors if getattr(c, 'contribution_type', None) == contribution_type]
                    return []
            
            converted_books.append(BookObj(book))
        else:
            converted_books.append(book)
    
    books = converted_books

    # Get distinct values for filter dropdowns (from all books, not filtered)
    all_books = user_books
    
    categories = set()
    publishers = set()
    languages = set()
    locations = set()

    for book in all_books:
        # Handle categories
        book_categories = book.get('categories', []) if isinstance(book, dict) else getattr(book, 'categories', [])
        if book_categories:
            # book.categories is a list of Category objects, not a string
            for cat in book_categories:
                if isinstance(cat, dict):
                    categories.add(cat.get('name', ''))
                elif hasattr(cat, 'name'):
                    categories.add(cat.name)
                else:
                    categories.add(str(cat))
        
        # Handle publisher
        book_publisher = book.get('publisher') if isinstance(book, dict) else getattr(book, 'publisher', None)
        if book_publisher:
            # Handle Publisher domain object or string
            if isinstance(book_publisher, dict):
                publisher_name = book_publisher.get('name', str(book_publisher))
            elif hasattr(book_publisher, 'name'):
                publisher_name = book_publisher.name
            else:
                publisher_name = str(book_publisher)
            publishers.add(publisher_name)
        
        # Handle language
        book_language = book.get('language') if isinstance(book, dict) else getattr(book, 'language', None)
        if book_language:
            languages.add(book_language)
        
        # Handle locations - they are now returned as strings (location names) from KuzuIntegrationService
        book_locations = book.get('locations', []) if isinstance(book, dict) else getattr(book, 'locations', [])
        if book_locations:
            for loc in book_locations:
                if isinstance(loc, str):
                    # Location is already a string (location name)
                    locations.add(loc)
                elif isinstance(loc, dict):
                    locations.add(loc.get('name', ''))
                elif hasattr(loc, 'name'):
                    locations.add(loc.name)
                else:
                    locations.add(str(loc))

    # Get users through Kuzu service layer
    domain_users = user_service.get_all_users_sync() or []
    
    # Convert domain users to simple objects for template compatibility
    users = []
    for domain_user in domain_users:
        user_data = {
            'id': domain_user.id,
            'username': domain_user.username,
            'email': domain_user.email
        }
        users.append(type('User', (), user_data))

    return render_template(
        'library_enhanced.html',
        books=books,
        stats=stats,
        categories=sorted(categories),
        publishers=sorted(publishers),
        languages=sorted(languages),
        locations=sorted(locations),
        current_status_filter=status_filter,
        current_category=category_filter,
        current_publisher=publisher_filter,
        current_language=language_filter,
        current_location=location_filter,
        current_search=search_query,
        current_sort=sort_option,
        users=users
    )

@book_bp.route('/public-library')
def public_library():
    filter_status = request.args.get('filter', 'all')
    
    # Use Kuzu service to get all books from all users
    # TODO: Implement public library functionality in Kuzu service
    # For now, return empty list
    books = []
    
    return render_template('public_library.html', books=books, filter_status=filter_status)

@book_bp.route('/book/<uid>/edit', methods=['GET', 'POST'])
@login_required
def edit_book(uid):
    try:
        # Get book through service layer
        user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
        if not user_book:
            abort(404)
        
        if request.method == 'POST':
            is_personal_data_only = False  # Initialize variable
            is_book_metadata_only = False  # Initialize variable
            is_mixed_form = False  # Initialize variable
            try:
                # Add debugging for the incoming request
                print(f"🔍 [EDIT_BOOK] Processing POST request for book {uid}")
                print(f"🔍 [EDIT_BOOK] Form data keys: {list(request.form.keys())}")
                print(f"🔍 [EDIT_BOOK] Request content type: {request.content_type}")
                print(f"🔍 [EDIT_BOOK] Request content length: {request.content_length}")
                
                # Check for CSRF token
                csrf_token = request.form.get('csrf_token')
                print(f"🔍 [EDIT_BOOK] CSRF token present: {bool(csrf_token)}")
                if csrf_token:
                    print(f"🔍 [EDIT_BOOK] CSRF token (first 10 chars): {csrf_token[:10]}")
                    
                # Check what type of form submission this is
                form_keys = set(request.form.keys())
                
                # Expanded set of user-specific fields that can be updated independently
                personal_data_keys = {
                    'csrf_token', 'personal_notes', 'review', 'user_rating', 
                    'reading_status', 'ownership_status', 'media_type'
                }
                
                # Set of book metadata fields that can be updated independently  
                book_metadata_keys = {
                    'csrf_token', 'publisher', 'isbn13', 'isbn10', 'published_date', 
                    'language', 'asin', 'google_books_id', 'openlibrary_id', 
                    'average_rating', 'rating_count'
                }
                
                # Set of mixed form fields (combination of book metadata and user data)
                mixed_form_keys = book_metadata_keys.union(personal_data_keys)
                
                is_personal_data_only = form_keys.issubset(personal_data_keys)
                is_book_metadata_only = form_keys.issubset(book_metadata_keys)
                is_mixed_form = form_keys.issubset(mixed_form_keys) and not is_personal_data_only and not is_book_metadata_only
                
                print(f"🔍 [EDIT_BOOK] Is personal data only submission: {is_personal_data_only}")
                print(f"🔍 [EDIT_BOOK] Is book metadata only submission: {is_book_metadata_only}")
                print(f"🔍 [EDIT_BOOK] Is mixed form submission: {is_mixed_form}")
                print(f"🔍 [EDIT_BOOK] Form keys: {form_keys}")
                print(f"🔍 [EDIT_BOOK] Personal data keys: {personal_data_keys}")
                print(f"🔍 [EDIT_BOOK] Book metadata keys: {book_metadata_keys}")
                print(f"🔍 [EDIT_BOOK] Mixed form keys: {mixed_form_keys}")
                
                # Check which fields are actually present for each category
                personal_fields_present = form_keys.intersection(personal_data_keys - {'csrf_token'})
                book_fields_present = form_keys.intersection(book_metadata_keys - {'csrf_token'})
                unknown_fields = form_keys - mixed_form_keys
                
                print(f"🔍 [EDIT_BOOK] Personal fields present: {personal_fields_present}")
                print(f"🔍 [EDIT_BOOK] Book fields present: {book_fields_present}")
                print(f"🔍 [EDIT_BOOK] Unknown fields: {unknown_fields}")
                
            except Exception as debug_error:
                print(f"❌ [EDIT_BOOK] Debug error: {debug_error}")
                # Continue processing despite debug errors
                
            # Initialize variables outside try block to avoid scope issues
            personal_fields_present = set()
            book_fields_present = set()
            try:
                # Re-analyze form if debug failed
                form_keys = set(request.form.keys())
                personal_data_keys = {
                    'csrf_token', 'personal_notes', 'review', 'user_rating', 
                    'reading_status', 'ownership_status', 'media_type'
                }
                book_metadata_keys = {
                    'csrf_token', 'publisher', 'isbn13', 'isbn10', 'published_date', 
                    'language', 'asin', 'google_books_id', 'openlibrary_id', 
                    'average_rating', 'rating_count'
                }
                personal_fields_present = form_keys.intersection(personal_data_keys - {'csrf_token'})
                book_fields_present = form_keys.intersection(book_metadata_keys - {'csrf_token'})
            except:
                pass
                
            # Improved detection logic - handle mixed forms with unknown fields
            has_personal_fields = len(personal_fields_present) > 0
            has_book_fields = len(book_fields_present) > 0
            has_only_personal = has_personal_fields and not has_book_fields
            has_only_book = has_book_fields and not has_personal_fields
            has_mixed = has_personal_fields and has_book_fields
            
            # Handle personal data only submission (notes, review, rating, status, etc.)
            if has_only_personal:
                print(f"🔍 [EDIT_BOOK] Handling personal data only submission")
                
                # Debug all form fields
                for key, value in request.form.items():
                    print(f"🔍 [EDIT_BOOK] Form field '{key}': '{value}'")
                
                # Extract all user-specific fields from the form
                update_data = {}
                
                # Personal notes and review
                personal_notes = request.form.get('personal_notes', '').strip() or None
                if 'personal_notes' in request.form:
                    update_data['personal_notes'] = personal_notes
                    
                review = request.form.get('review', '').strip() or None
                if 'review' in request.form:
                    update_data['review'] = review
                
                print(f"🔍 [EDIT_BOOK] Review field - raw value: '{request.form.get('review', '')}', processed: '{review}'")
                print(f"🔍 [EDIT_BOOK] Personal notes field - raw value: '{request.form.get('personal_notes', '')}', processed: '{personal_notes}'")
                
                # User rating
                user_rating = request.form.get('user_rating', '').strip()
                if 'user_rating' in request.form:
                    if user_rating:
                        try:
                            update_data['user_rating'] = float(user_rating)
                        except ValueError:
                            print(f"⚠️ [EDIT_BOOK] Invalid user rating: {user_rating}")
                    else:
                        update_data['user_rating'] = None  # Clear rating
                
                # Reading status
                reading_status = request.form.get('reading_status', '').strip()
                if 'reading_status' in request.form and reading_status:
                    update_data['reading_status'] = reading_status
                
                # Ownership status
                ownership_status = request.form.get('ownership_status', '').strip()
                if 'ownership_status' in request.form and ownership_status:
                    update_data['ownership_status'] = ownership_status
                
                # Media type
                media_type = request.form.get('media_type', '').strip()
                if 'media_type' in request.form and media_type:
                    update_data['media_type'] = media_type
                
                print(f"🔍 [EDIT_BOOK] Personal data update fields: {list(update_data.keys())}")
                print(f"🔍 [EDIT_BOOK] Update data values: {update_data}")
                
                try:
                    success = book_service.update_book_sync(uid, str(current_user.id), **update_data)
                    print(f"🔍 [EDIT_BOOK] Personal data update result: {success}")
                    
                    if success:
                        # Create appropriate success message based on what was updated
                        updated_fields = []
                        if 'personal_notes' in update_data:
                            updated_fields.append('personal notes')
                        if 'review' in update_data:
                            updated_fields.append('review')
                        if 'user_rating' in update_data:
                            updated_fields.append('rating')
                        if 'reading_status' in update_data:
                            updated_fields.append('reading status')
                        if 'ownership_status' in update_data:
                            updated_fields.append('ownership status')
                        if 'media_type' in update_data:
                            updated_fields.append('media type')
                        
                        if updated_fields:
                            flash(f"Updated {', '.join(updated_fields)} successfully.", 'success')
                        else:
                            flash('Personal data updated successfully.', 'success')
                    else:
                        flash('Failed to update personal data.', 'error')
                    return redirect(url_for('book.view_book_enhanced', uid=uid))
                    
                except Exception as e:
                    print(f"❌ [EDIT_BOOK] Exception during personal data update: {type(e).__name__}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    flash(f'Error updating personal data: {str(e)}', 'error')
                    return redirect(url_for('book.view_book_enhanced', uid=uid))
            
            # Handle book metadata only submission (publisher, rating, etc.)
            elif has_only_book:
                print(f"🔍 [EDIT_BOOK] Handling book metadata only submission")
                
                # Debug all form fields
                for key, value in request.form.items():
                    print(f"🔍 [EDIT_BOOK] Form field '{key}': '{value}'")
                
                # Extract book metadata fields from the form
                update_data = {}
                
                # Publisher
                publisher = request.form.get('publisher', '').strip() or None
                if 'publisher' in request.form:
                    update_data['publisher'] = publisher
                
                # ISBN fields
                isbn13 = request.form.get('isbn13', '').strip() or None
                if 'isbn13' in request.form:
                    update_data['isbn13'] = isbn13
                    
                isbn10 = request.form.get('isbn10', '').strip() or None
                if 'isbn10' in request.form:
                    update_data['isbn10'] = isbn10
                
                # Published date
                published_date_str = request.form.get('published_date', '').strip()
                if 'published_date' in request.form and published_date_str:
                    update_data['published_date'] = _convert_published_date_to_date(published_date_str)
                
                # Language
                language = request.form.get('language', '').strip() or None
                if 'language' in request.form:
                    update_data['language'] = language
                
                # External IDs
                asin = request.form.get('asin', '').strip() or None
                if 'asin' in request.form:
                    update_data['asin'] = asin
                    
                google_books_id = request.form.get('google_books_id', '').strip() or None
                if 'google_books_id' in request.form:
                    update_data['google_books_id'] = google_books_id
                    
                openlibrary_id = request.form.get('openlibrary_id', '').strip() or None
                if 'openlibrary_id' in request.form:
                    update_data['openlibrary_id'] = openlibrary_id
                
                # Rating fields
                average_rating = request.form.get('average_rating', '').strip()
                if 'average_rating' in request.form:
                    if average_rating:
                        try:
                            update_data['average_rating'] = float(average_rating)
                        except ValueError:
                            print(f"⚠️ [EDIT_BOOK] Invalid average rating: {average_rating}")
                    else:
                        update_data['average_rating'] = None
                
                rating_count = request.form.get('rating_count', '').strip()
                if 'rating_count' in request.form:
                    if rating_count:
                        try:
                            update_data['rating_count'] = int(rating_count)
                        except ValueError:
                            print(f"⚠️ [EDIT_BOOK] Invalid rating count: {rating_count}")
                    else:
                        update_data['rating_count'] = None
                
                print(f"🔍 [EDIT_BOOK] Book metadata update fields: {list(update_data.keys())}")
                
                try:
                    success = book_service.update_book_sync(uid, str(current_user.id), **update_data)
                    print(f"🔍 [EDIT_BOOK] Book metadata update result: {success}")
                    
                    if success:
                        # Create appropriate success message based on what was updated
                        updated_fields = []
                        if 'publisher' in update_data:
                            updated_fields.append('publisher')
                        if 'average_rating' in update_data:
                            updated_fields.append('average rating')
                        if 'rating_count' in update_data:
                            updated_fields.append('rating count')
                        if 'isbn13' in update_data:
                            updated_fields.append('ISBN-13')
                        if 'isbn10' in update_data:
                            updated_fields.append('ISBN-10')
                        if 'published_date' in update_data:
                            updated_fields.append('published date')
                        if 'language' in update_data:
                            updated_fields.append('language')
                        if 'asin' in update_data:
                            updated_fields.append('ASIN')
                        if 'google_books_id' in update_data:
                            updated_fields.append('Google Books ID')
                        if 'openlibrary_id' in update_data:
                            updated_fields.append('OpenLibrary ID')
                        
                        if updated_fields:
                            flash(f"Updated {', '.join(updated_fields)} successfully.", 'success')
                        else:
                            flash('Book metadata updated successfully.', 'success')
                    else:
                        flash('Failed to update book metadata.', 'error')
                    return redirect(url_for('book.view_book_enhanced', uid=uid))
                    
                except Exception as e:
                    print(f"❌ [EDIT_BOOK] Exception during book metadata update: {type(e).__name__}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    flash(f'Error updating book metadata: {str(e)}', 'error')
                    return redirect(url_for('book.view_book_enhanced', uid=uid))
            
            # Handle mixed form submission (both book metadata and personal data)
            elif has_mixed:
                print(f"🔍 [EDIT_BOOK] Handling mixed form submission")
                
                # Debug all form fields
                for key, value in request.form.items():
                    print(f"🔍 [EDIT_BOOK] Form field '{key}': '{value}'")
                
                # Split fields into book metadata and personal data
                book_update_data = {}
                personal_update_data = {}
                
                # Process book metadata fields
                for field in ['publisher', 'isbn13', 'isbn10', 'published_date', 'language', 
                             'asin', 'google_books_id', 'openlibrary_id', 'average_rating', 'rating_count']:
                    if field in request.form:
                        value = request.form.get(field, '').strip() or None
                        if field == 'published_date' and value:
                            book_update_data[field] = _convert_published_date_to_date(value)
                        elif field in ['average_rating'] and value:
                            try:
                                book_update_data[field] = float(value)
                            except ValueError:
                                print(f"⚠️ [EDIT_BOOK] Invalid {field}: {value}")
                        elif field in ['rating_count'] and value:
                            try:
                                book_update_data[field] = int(value)
                            except ValueError:
                                print(f"⚠️ [EDIT_BOOK] Invalid {field}: {value}")
                        else:
                            book_update_data[field] = value
                
                # Process personal data fields
                for field in ['personal_notes', 'review', 'user_rating', 'reading_status', 
                             'ownership_status', 'media_type']:
                    if field in request.form:
                        value = request.form.get(field, '').strip() or None
                        if field == 'user_rating' and value:
                            try:
                                personal_update_data[field] = float(value)
                            except ValueError:
                                print(f"⚠️ [EDIT_BOOK] Invalid {field}: {value}")
                        else:
                            personal_update_data[field] = value
                
                print(f"🔍 [EDIT_BOOK] Book metadata update fields: {list(book_update_data.keys())}")
                print(f"🔍 [EDIT_BOOK] Personal data update fields: {list(personal_update_data.keys())}")
                
                try:
                    # Update book metadata if any book fields present
                    book_success = True
                    if book_update_data:
                        book_success = book_service.update_book_sync(uid, str(current_user.id), **book_update_data)
                        print(f"🔍 [EDIT_BOOK] Book metadata update result: {book_success}")
                    
                    # Update personal data if any personal fields present  
                    personal_success = True
                    if personal_update_data:
                        personal_success = book_service.update_book_sync(uid, str(current_user.id), **personal_update_data)
                        print(f"🔍 [EDIT_BOOK] Personal data update result: {personal_success}")
                    
                    if book_success and personal_success:
                        # Create appropriate success message
                        updated_fields = []
                        
                        # Add book field names
                        if 'publisher' in book_update_data:
                            updated_fields.append('publisher')
                        if 'average_rating' in book_update_data:
                            updated_fields.append('average rating')
                        if 'rating_count' in book_update_data:
                            updated_fields.append('rating count')
                        
                        # Add personal field names
                        if 'review' in personal_update_data:
                            updated_fields.append('review')
                        if 'user_rating' in personal_update_data:
                            updated_fields.append('your rating')
                        if 'personal_notes' in personal_update_data:
                            updated_fields.append('personal notes')
                        
                        if updated_fields:
                            flash(f"Updated {', '.join(updated_fields)} successfully.", 'success')
                        else:
                            flash('Data updated successfully.', 'success')
                    else:
                        flash('Failed to update some data.', 'error')
                    return redirect(url_for('book.view_book_enhanced', uid=uid))
                    
                except Exception as e:
                    print(f"❌ [EDIT_BOOK] Exception during mixed form update: {type(e).__name__}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    flash(f'Error updating data: {str(e)}', 'error')
                    return redirect(url_for('book.view_book_enhanced', uid=uid))
            
            # Handle full form submission (if not personal data only, book metadata only, or mixed form)
            print(f"🔍 [EDIT_BOOK] Handling full form submission")
        
        new_isbn13 = request.form.get('isbn13', '').strip() or None
        new_isbn10 = request.form.get('isbn10', '').strip() or None
        
        # Process contributors
        contributors = []
        contributor_data = {}
        
        # Parse contributor form data
        for key, value in request.form.items():
            if key.startswith('contributors[') and '][' in key:
                # Extract index and field from key like "contributors[0][name]"
                parts = key.split('][')
                if len(parts) == 2:
                    index_part = parts[0].replace('contributors[', '')
                    field = parts[1].replace(']', '')
                    
                    if index_part not in contributor_data:
                        contributor_data[index_part] = {}
                    contributor_data[index_part][field] = value
        
        # Process categories
        categories = []
        category_data = {}
        
        # Parse category form data
        for key, value in request.form.items():
            if key.startswith('categories[') and '][' in key:
                # Extract index and field from key like "categories[0][name]"
                parts = key.split('][')
                if len(parts) == 2:
                    index_part = parts[0].replace('categories[', '')
                    field = parts[1].replace(']', '')
                    
                    if index_part not in category_data:
                        category_data[index_part] = {}
                    category_data[index_part][field] = value
        
        # Create category list for processing
        for cat_data in category_data.values():
            if cat_data.get('name'):
                categories.append(cat_data['name'])  # Just pass the name, service will handle creation/linking
        
        # Create BookContribution objects
        for contrib in contributor_data.values():
            if contrib.get('name'):
                from app.domain.models import Person, BookContribution, ContributionType
                
                person_name = contrib['name']
                
                try:
                    # Always use find_or_create approach - the most reliable method
                    from app.debug_system import debug_log
                    debug_log(f"Finding or creating person: {person_name}", "PERSON_CREATION")
                    
                    # Use the same logic as the repository's find_or_create_person method
                    from app.infrastructure.kuzu_graph import get_graph_storage
                    storage = get_graph_storage()
                    
                    # Search for existing person by name (same as repository method)
                    normalized_name = person_name.strip().lower()
                    all_persons = storage.find_nodes_by_type('person')
                    person = None
                    
                    for person_data in all_persons:
                        existing_name = person_data.get('name', '').strip().lower()
                        existing_normalized = person_data.get('normalized_name', '').strip().lower()
                        
                        if existing_name == normalized_name or existing_normalized == normalized_name:
                            debug_log(f"Found existing person: {person_data.get('name')} (ID: {person_data.get('id')})", "PERSON_CREATION")
                            # Convert back to Person object
                            person = Person(
                                id=person_data.get('id'),
                                name=person_data.get('name', ''),
                                normalized_name=person_data.get('normalized_name', ''),
                                birth_year=person_data.get('birth_year'),
                                death_year=person_data.get('death_year'),
                                birth_place=person_data.get('birth_place'),
                                bio=person_data.get('bio'),
                                website=person_data.get('website'),
                                created_at=datetime.now(),  # Set defaults for dates
                                updated_at=datetime.now()
                            )
                            break
                    
                    # If not found, create new person using clean repository (with auto-fetch)
                    if not person:
                        debug_log(f"Creating new person via clean repository: {person_name}", "PERSON_CREATION")
                        from app.infrastructure.kuzu_repositories import KuzuPersonRepository
                        from app.infrastructure.kuzu_graph import get_kuzu_connection
                        
                        try:
                            # Use clean repository with auto-fetch capability
                            person_repo = KuzuPersonRepository()
                            
                            # Create person using repository (will auto-fetch OpenLibrary metadata)
                            person_dict = {
                                'id': str(uuid.uuid4()),
                                'name': person_name,
                                'normalized_name': normalized_name,
                                'created_at': datetime.now().isoformat(),
                                'updated_at': datetime.now().isoformat()
                            }
                            
                            created_person = person_repo.create(person_dict)
                            if created_person:
                                # Convert back to Person object for the rest of the workflow
                                person = Person(
                                    id=created_person.get('id'),
                                    name=created_person.get('name', ''),
                                    normalized_name=created_person.get('normalized_name', ''),
                                    birth_year=created_person.get('birth_year'),
                                    death_year=created_person.get('death_year'),
                                    birth_place=created_person.get('birth_place'),
                                    bio=created_person.get('bio'),
                                    website=created_person.get('website'),
                                    openlibrary_id=created_person.get('openlibrary_id'),
                                    image_url=created_person.get('image_url'),
                                    created_at=datetime.now(),
                                    updated_at=datetime.now()
                                )
                                debug_log(f"Created new person with auto-fetch: {person.name} (ID: {person.id})", "PERSON_CREATION")
                            else:
                                debug_log(f"Failed to create person via repository: {person_name}", "PERSON_CREATION_ERROR")
                                continue
                        except Exception as repo_error:
                            debug_log(f"Repository creation failed for {person_name}: {repo_error}", "PERSON_CREATION_ERROR")
                            continue
                    
                    # Validate person has valid ID
                    if not person or not person.id:
                        debug_log(f"Person has invalid ID: {person}", "PERSON_CREATION_ERROR")
                        continue
                    
                except Exception as e:
                    print(f"❌ [DEBUG] Error processing person {person_name}: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
                
                # Map contribution type
                contrib_type_map = {
                    'authored': ContributionType.AUTHORED,
                    'edited': ContributionType.EDITED,
                    'translated': ContributionType.TRANSLATED,
                    'illustrated': ContributionType.ILLUSTRATED,
                    'narrated': ContributionType.NARRATED,
                    'gave_foreword': ContributionType.GAVE_FOREWORD,
                    'gave_introduction': ContributionType.GAVE_INTRODUCTION,
                    'gave_afterword': ContributionType.GAVE_AFTERWORD,
                    'compiled': ContributionType.COMPILED,
                    'contributed': ContributionType.CONTRIBUTED,
                    'co_authored': ContributionType.CO_AUTHORED,
                    'ghost_wrote': ContributionType.GHOST_WROTE
                }
                
                contrib_type = contrib_type_map.get(contrib.get('type', 'authored'), ContributionType.AUTHORED)
                
                contribution = BookContribution(
                    person=person,
                    contribution_type=contrib_type,
                    created_at=datetime.now()
                )
                contributors.append(contribution)
        
        update_data = {
            'title': request.form['title'],
            'subtitle': request.form.get('subtitle', '').strip() or None,
            'description': request.form.get('description', '').strip() or None,
            'published_date': _convert_published_date_to_date(request.form.get('published_date', '').strip()) if request.form.get('published_date', '').strip() else None,
            'page_count': int(page_count_str) if (page_count_str := request.form.get('page_count', '').strip()) else None,
            'language': request.form.get('language', '').strip() or 'en',
            'cover_url': request.form.get('cover_url', '').strip() or None,
            'isbn13': new_isbn13,
            'isbn10': new_isbn10,
            'series': request.form.get('series', '').strip() or None,
            'series_volume': request.form.get('series_volume', '').strip() or None,
            'series_order': int(series_order_str) if (series_order_str := request.form.get('series_order', '').strip()) else None,
            'contributors': contributors,
            'raw_categories': ','.join(categories) if categories else None,
            # Additional metadata fields - these are Book properties, not user-specific
            'publisher': request.form.get('publisher', '').strip() or None,
            'asin': request.form.get('asin', '').strip() or None,
            'google_books_id': request.form.get('google_books_id', '').strip() or None,
            'openlibrary_id': request.form.get('openlibrary_id', '').strip() or None,
            'average_rating': float(avg_rating_str) if (avg_rating_str := request.form.get('average_rating', '').strip()) else None,
            'rating_count': int(rating_count_str) if (rating_count_str := request.form.get('rating_count', '').strip()) else None,
            # User-specific fields
            'reading_status': request.form.get('reading_status', '').strip() or None,
            'ownership_status': request.form.get('ownership_status', '').strip() or None,
            'media_type': request.form.get('media_type', '').strip() or None,
            'personal_notes': request.form.get('personal_notes', '').strip() or None,
            'review': request.form.get('review', '').strip() or None,
        }
        
        # Handle user rating
        user_rating = request.form.get('user_rating', '').strip()
        if user_rating:
            try:
                update_data['user_rating'] = float(user_rating)
            except ValueError:
                pass  # Invalid rating, skip it
        
        # Remove None values except for specific fields that can be null
        filtered_data = {}
        for k, v in update_data.items():
            if k in ['contributors', 'raw_categories', 'series', 'series_volume', 'series_order', 'publisher', 'asin', 'google_books_id', 'openlibrary_id', 'average_rating', 'rating_count'] or v is not None:
                filtered_data[k] = v
        
        try:
            print(f"🔍 [EDIT_BOOK] Calling book_service.update_book_sync with filtered_data: {filtered_data}")
            success = book_service.update_book_sync(uid, str(current_user.id), **filtered_data)
            print(f"🔍 [EDIT_BOOK] Book update result: {success}")
        except Exception as e:
            print(f"❌ [EDIT_BOOK] Exception during book update: {type(e).__name__}: {str(e)}")
            import traceback
            traceback.print_exc()
            flash(f'Error updating book: {str(e)}', 'error')
            return redirect(url_for('book.view_book_enhanced', uid=uid))
        
        # Handle location update separately
        location_id = request.form.get('location_id', '').strip()
        print(f"🔍 [EDIT_BOOK] location_id from form: '{location_id}' (type: {type(location_id)})")
        if location_id is not None:  # Allow empty string to clear location
            print(f"🔍 [EDIT_BOOK] Proceeding with location update...")
            # Use the location service to update the book location
            try:
                from app.location_service import LocationService
                from app.infrastructure.kuzu_graph import get_kuzu_connection
                
                db = get_kuzu_connection()
                connection = db.connect()
                location_service = LocationService(connection)
                
                # Convert empty string to None for clearing location
                location_success = location_service.set_book_location(
                    uid, 
                    location_id if location_id else None, 
                    str(current_user.id)
                )
                print(f"🔍 [EDIT_BOOK] Location update result: {location_success}")
                if not location_success:
                    print(f"⚠️ Failed to update location for book {uid}")
            except Exception as e:
                print(f"❌ [EDIT_BOOK] Error updating location: {e}")
        else:
            print(f"🔍 [EDIT_BOOK] Skipping location update (location_id is None)")
        
        if success:
            flash('Book updated successfully.', 'success')
        else:
            flash('Failed to update book.', 'error')
        return redirect(url_for('book.view_book_enhanced', uid=uid))
    
        # GET request handling
        # Get book categories for editing
        book_categories = []
        try:
            book_id = user_book.get('id') if isinstance(user_book, dict) else getattr(user_book, 'id', None)
            if book_id:
                book_categories = book_service.get_book_categories_sync(book_id)
        except Exception as e:
            print(f"❌ [EDIT] Error loading book categories: {e}")
        
        # Convert dictionary to object-like structure for template compatibility
        if isinstance(user_book, dict):
            class BookObj:
                def __init__(self, data):
                    for key, value in data.items():
                        setattr(self, key, value)
                    # Ensure common attributes have defaults
                    # Note: authors property is derived from contributors, don't set directly
                    if not hasattr(self, 'contributors'):
                        self.contributors = []
                    if not hasattr(self, 'categories'):
                        self.categories = []
                    if not hasattr(self, 'publisher'):
                        self.publisher = None
                    # Handle ownership data
                    ownership = data.get('ownership', {})
                    for key, value in ownership.items():
                        setattr(self, key, value)
                
                def get_contributors_by_type(self, contribution_type):
                    """Get contributors filtered by type."""
                    if not hasattr(self, 'contributors') or not self.contributors:
                        return []
                    
                    # Handle both string and enum contribution types
                    type_str = contribution_type.upper() if isinstance(contribution_type, str) else str(contribution_type).upper()
                    
                    filtered = []
                    for contributor in self.contributors:
                        if isinstance(contributor, dict):
                            role = contributor.get('role', '').upper()
                            if role == type_str or role == type_str.replace('ED', ''):  # Handle past tense
                                filtered.append(contributor)
                        else:
                            # Handle object-like contributors
                            role = getattr(contributor, 'role', '').upper()
                            if role == type_str or role == type_str.replace('ED', ''):
                                filtered.append(contributor)
                    
                    return filtered
            
            user_book = BookObj(user_book)
        
        return render_template('edit_book_enhanced.html', book=user_book, book_categories=book_categories)

    except Exception as e:
        print(f"❌ [EDIT_BOOK] Uncaught exception in edit_book: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error processing request: {str(e)}', 'error')
        return redirect(url_for('book.view_book_enhanced', uid=uid))


@book_bp.route('/book/<uid>/enhanced')
@login_required
def view_book_enhanced(uid):
    """Enhanced book view with new status system."""
    from app.debug_system import debug_log, debug_book_details, debug_service_call, debug_template_data
    
    debug_log(f"🔍 [VIEW] Starting enhanced book view for UID: {uid}, User: {current_user.id}", "BOOK_VIEW")
    
    # Service call debugging
    debug_service_call("book_service", "get_book_by_uid_sync", {"uid": uid, "user_id": str(current_user.id)}, None, "BEFORE")
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    debug_service_call("book_service", "get_book_by_uid_sync", {"uid": uid, "user_id": str(current_user.id)}, user_book, "AFTER")
    
    if not user_book:
        debug_log(f"❌ [VIEW] Book {uid} not found for user {current_user.id}", "BOOK_VIEW")
        abort(404)

    # Enhanced book debugging
    debug_book_details(user_book, uid, str(current_user.id), "VIEW")

    title = user_book.get('title', 'Unknown Title') if isinstance(user_book, dict) else getattr(user_book, 'title', 'Unknown Title')
    debug_log(f"✅ [VIEW] Found book: {title}", "BOOK_VIEW")

    # Convert dictionary to object-like structure for template compatibility
    if isinstance(user_book, dict):
        debug_log(f"🔄 [VIEW] Converting dict to object for template compatibility", "BOOK_VIEW")
        # Create an object-like structure that the template can work with
        class BookObj:
            def __init__(self, data):
                for key, value in data.items():
                    setattr(self, key, value)
                # Ensure common attributes have defaults
                # Note: authors property is derived from contributors, don't set directly
                if not hasattr(self, 'contributors'):
                    self.contributors = []
                if not hasattr(self, 'categories'):
                    self.categories = []
                if not hasattr(self, 'publisher'):
                    self.publisher = None
                if not hasattr(self, 'series'):
                    self.series = None
                if not hasattr(self, 'custom_metadata'):
                    self.custom_metadata = {}
                # DEBUG: Log all cover-related fields
                cover_url_value = getattr(self, 'cover_url', None)
                debug_log(f"🖼️ [VIEW] Cover URL field - cover_url: {cover_url_value}", "BOOK_VIEW")
                
                # Cover URL field is already correct (cover_url is the database field)
                # No field mapping needed since we're using the correct field name everywhere
                
                # DEBUG: Log all ISBN-related fields
                isbn13_value = getattr(self, 'isbn13', None)
                isbn10_value = getattr(self, 'isbn10', None)
                isbn_value = getattr(self, 'isbn', None)
                debug_log(f"📚 [VIEW] ISBN fields - isbn13: {isbn13_value}, isbn10: {isbn10_value}, isbn: {isbn_value}", "BOOK_VIEW")
                
                # Handle ISBN field consistency - Template expects isbn13/isbn10 specifically
                if not hasattr(self, 'isbn') and (hasattr(self, 'isbn13') or hasattr(self, 'isbn10')):
                    self.isbn = getattr(self, 'isbn13', None) or getattr(self, 'isbn10', None)
                    debug_log(f"📚 [VIEW] Mapped primary ISBN: {self.isbn}", "BOOK_VIEW")
                
                # REVERSE NORMALIZATION: If we have 'isbn' but not isbn13/isbn10, normalize it
                isbn_value = getattr(self, 'isbn', None)
                if isbn_value and (not hasattr(self, 'isbn13') or not getattr(self, 'isbn13')):
                    import re
                    # Clean and normalize the ISBN
                    clean_isbn = re.sub(r'[^0-9X]', '', str(isbn_value).upper())
                    if len(clean_isbn) == 13:
                        self.isbn13 = clean_isbn
                        debug_log(f"📚 [VIEW] Normalized generic ISBN to isbn13: {clean_isbn}", "BOOK_VIEW")
                    elif len(clean_isbn) == 10:
                        self.isbn10 = clean_isbn
                        debug_log(f"📚 [VIEW] Normalized generic ISBN to isbn10: {clean_isbn}", "BOOK_VIEW")
                
                # Ensure both fields exist for template (even if None)
                if not hasattr(self, 'isbn13'):
                    self.isbn13 = None
                if not hasattr(self, 'isbn10'):
                    self.isbn10 = None
                
                # DEBUG: Log all category-related fields
                categories_value = getattr(self, 'categories', None)
                genre_value = getattr(self, 'genre', None)
                genres_value = getattr(self, 'genres', None)
                debug_log(f"🏷️ [VIEW] Category fields - categories: {categories_value}, genre: {genre_value}, genres: {genres_value}", "BOOK_VIEW")
                
                # Handle category field consistency
                if not hasattr(self, 'categories') or not getattr(self, 'categories', None):
                    if hasattr(self, 'genres') and getattr(self, 'genres', None):
                        self.categories = getattr(self, 'genres')
                        debug_log(f"🏷️ [VIEW] Mapped genres to categories: {self.categories}", "BOOK_VIEW")
                    elif hasattr(self, 'genre') and getattr(self, 'genre', None):
                        genre_val = getattr(self, 'genre')
                        self.categories = [genre_val] if isinstance(genre_val, str) else genre_val
                        debug_log(f"🏷️ [VIEW] Mapped genre to categories: {self.categories}", "BOOK_VIEW")
                
                # Final debug
                final_cover_url = getattr(self, 'cover_url', None)
                final_cover_url_alt = getattr(self, 'cover_url', None)
                debug_log(f"🖼️ [VIEW] Final cover URLs - cover_url: {final_cover_url}, cover_url_alt: {final_cover_url_alt}", "BOOK_VIEW")
                
                # Handle location field - if location_id exists but locations is empty, get location name
                location_id = getattr(self, 'location_id', None)
                if hasattr(self, 'location_id') and location_id and (not hasattr(self, 'locations') or not getattr(self, 'locations', None)):
                    try:
                        from app.location_service import LocationService
                        from app.infrastructure.kuzu_graph import get_kuzu_connection
                        
                        kuzu_connection = get_kuzu_connection()
                        location_service = LocationService(kuzu_connection.connect())
                        
                        # Get current user ID properly
                        user_id_for_location = str(current_user.id) if 'current_user' in globals() and hasattr(current_user, 'id') else str(data.get('user_id', ''))
                        if user_id_for_location:
                            # Get all available locations, not just those with books
                            user_locations = location_service.get_all_locations()
                            
                            # Find the location object by ID
                            for user_loc in user_locations:
                                if hasattr(user_loc, 'id') and str(user_loc.id) == str(location_id):
                                    self.locations = [{'id': user_loc.id, 'name': user_loc.name}]
                                    break
                            
                            # If location not found by ID, check if location_id is actually a name
                            if not hasattr(self, 'locations') or not self.locations:
                                for user_loc in user_locations:
                                    if hasattr(user_loc, 'name') and str(user_loc.name) == str(location_id):
                                        self.locations = [{'id': user_loc.id, 'name': user_loc.name}]
                                        break
                                        
                    except Exception as e:
                        print(f"Error populating location data: {e}")
                        # Fallback: treat location_id as the name
                        if location_id:
                            self.locations = [{'id': location_id, 'name': location_id}]
                        # Fallback: treat location_id as the name
                        if location_id:
                            self.locations = [{'id': location_id, 'name': location_id}]
                
                # Handle ownership data
                ownership = data.get('ownership', {})
                for key, value in ownership.items():
                    setattr(self, key, value)
            
            def get_contributors_by_type(self, contribution_type):
                """Get contributors filtered by type."""
                if not hasattr(self, 'contributors') or not self.contributors:
                    return []
                
                # Handle both string and enum contribution types
                type_str = contribution_type.upper() if isinstance(contribution_type, str) else str(contribution_type).upper()
                
                filtered = []
                for contributor in self.contributors:
                    if isinstance(contributor, dict):
                        role = contributor.get('role', '').upper()
                        if role == type_str or role == type_str.replace('ED', ''):  # Handle past tense
                            filtered.append(contributor)
                    else:
                        # Handle object-like contributors
                        role = getattr(contributor, 'role', '').upper()
                        if role == type_str or role == type_str.replace('ED', ''):
                            filtered.append(contributor)
                
                return filtered
        
        user_book = BookObj(user_book)
        debug_log(f"✅ [VIEW] Converted dictionary to object for template compatibility", "BOOK_VIEW")
        
        # Debug ISBN fields specifically and ensure final normalization
        isbn13 = getattr(user_book, 'isbn13', None)
        isbn10 = getattr(user_book, 'isbn10', None) 
        isbn_generic = getattr(user_book, 'isbn', None)
        cover_url = getattr(user_book, 'cover_url', None)
        
        debug_log(f"🔍 [VIEW] ISBN Debug - ISBN13: {isbn13}, ISBN10: {isbn10}, Generic ISBN: {isbn_generic}", "BOOK_VIEW")
        debug_log(f"🔍 [VIEW] Cover Debug - Cover URL: {cover_url}", "BOOK_VIEW")
        debug_log(f"🔍 [VIEW] Available book attributes: {[attr for attr in dir(user_book) if not attr.startswith('_')]}", "BOOK_VIEW")
        
        # FINAL NORMALIZATION: Ensure ISBN fields are available for template
        if not isbn13 and not isbn10 and isbn_generic:
            import re
            clean_isbn = re.sub(r'[^0-9X]', '', str(isbn_generic).upper())
            if len(clean_isbn) == 13:
                user_book.isbn13 = clean_isbn
                debug_log(f"📚 [VIEW] Final: Normalized generic ISBN to isbn13: {clean_isbn}", "BOOK_VIEW")
            elif len(clean_isbn) == 10:
                user_book.isbn10 = clean_isbn
                debug_log(f"📚 [VIEW] Final: Normalized generic ISBN to isbn10: {clean_isbn}", "BOOK_VIEW")
        
        # Ensure both fields exist (template expects them)
        if not hasattr(user_book, 'isbn13'):
            user_book.isbn13 = isbn13
        if not hasattr(user_book, 'isbn10'):
            user_book.isbn10 = isbn10
        if not hasattr(user_book, 'isbn'):
            user_book.isbn = isbn13 or isbn10  # Fallback for any other template expectations

    # Get book authors
    try:
        book_id = getattr(user_book, 'id', None)
        debug_log(f"🔍 [VIEW] Getting authors for book ID: {book_id}", "BOOK_VIEW")
        if book_id and (hasattr(user_book, 'contributors') and not user_book.contributors):
            # Fetch authors from database using the same pattern as categories
            from app.infrastructure.kuzu_graph import get_kuzu_connection
            from app.domain.models import BookContribution, Person, ContributionType
            kuzu_connection = get_kuzu_connection()
            
            query = """
            MATCH (p:Person)-[rel:AUTHORED]->(b:Book {id: $book_id})
            RETURN p.name as name, p.id as id, rel.role as role, rel.order_index as order_index
            ORDER BY rel.order_index ASC
            """
            
            results = kuzu_connection.query(query, {"book_id": book_id})
            
            contributors = []
            for result in results:
                person_id = result.get('col_1', '')
                person_name = result.get('col_0', '')
                if person_name and person_id:  # Ensure both name and id exist
                    person = Person(
                        id=person_id,
                        name=person_name,
                        normalized_name=person_name.lower()
                    )
                    
                    contribution = BookContribution(
                        person_id=person_id,
                        book_id=book_id,
                        contribution_type=ContributionType.AUTHORED,
                        order=result.get('col_3', 0),
                        person=person
                    )
                    
                    contributors.append(contribution)
            
            # Update the book object with the fetched contributors
            user_book.contributors = contributors
            
            # Authors property is automatically derived from contributors
            # No need to set it manually since it's a read-only property
                    
            debug_log(f"✅ [VIEW] Found and populated {len(contributors)} authors as contributors", "BOOK_VIEW")
            debug_log(f"✅ [VIEW] Authors will be derived automatically from contributors", "BOOK_VIEW")
        else:
            debug_log(f"ℹ️ [VIEW] Book already has contributors or no book ID", "BOOK_VIEW")
            
            # Even if contributors exist, make sure they have the proper structure
            # Authors property is automatically derived from contributors
            if hasattr(user_book, 'contributors') and user_book.contributors:
                debug_log(f"✅ [VIEW] Using existing {len(user_book.contributors)} contributors for authors", "BOOK_VIEW")
    except Exception as e:
        current_app.logger.error(f"Error loading book authors: {e}")
        debug_log(f"❌ [VIEW] Error loading book authors: {e}", "BOOK_VIEW")

    # Get book categories
    book_categories = []
    try:
        book_id = getattr(user_book, 'id', None)
        debug_log(f"🔍 [VIEW] Getting categories for book ID: {book_id}", "BOOK_VIEW")
        if book_id:
            debug_service_call("book_service", "get_book_categories_sync", {"book_id": book_id}, None, "BEFORE")
            book_categories = book_service.get_book_categories_sync(book_id)
            debug_service_call("book_service", "get_book_categories_sync", {"book_id": book_id}, book_categories, "AFTER")
            debug_log(f"✅ [VIEW] Found {len(book_categories)} categories", "BOOK_VIEW")
        else:
            debug_log(f"❌ [VIEW] No book ID found for category lookup", "BOOK_VIEW")
    except Exception as e:
        current_app.logger.error(f"Error loading book categories: {e}")
        debug_log(f"❌ [VIEW] Error loading book categories: {e}", "BOOK_VIEW")

    # Get custom metadata for display
    global_metadata_display = []
    personal_metadata_display = []  # Initialize as empty list
    
    # Get available custom fields for edit mode
    personal_fields = []
    global_fields = []
    current_metadata = {}
    
    try:
        debug_log(f"🔍 [VIEW] Processing custom metadata", "BOOK_VIEW")
        # Get custom metadata using the custom field service
        custom_metadata = custom_field_service.get_custom_metadata_sync(uid, str(current_user.id))
        current_metadata = custom_metadata or {}
        
        if custom_metadata:
            debug_log(f"✅ [VIEW] Combined metadata found: {custom_metadata}", "BOOK_VIEW")
            
            # Separate global and personal metadata for display
            global_metadata = {}
            personal_metadata = {}
            
            # Separate fields based on their definitions
            for field_name, field_value in custom_metadata.items():
                if field_value is not None and field_value != '':
                    field_def = custom_field_service._get_field_definition(field_name)
                    if field_def and field_def.get('is_global', False):
                        global_metadata[field_name] = field_value
                        debug_log(f"🌍 [VIEW] Field '{field_name}' classified as GLOBAL", "BOOK_VIEW")
                    else:
                        personal_metadata[field_name] = field_value
                        debug_log(f"👤 [VIEW] Field '{field_name}' classified as PERSONAL", "BOOK_VIEW")
            
            # Convert to display format separately
            global_metadata_display = custom_field_service.get_custom_metadata_for_display(
                global_metadata
            ) or []
            personal_metadata_display = custom_field_service.get_custom_metadata_for_display(
                personal_metadata
            ) or []
            
            debug_log(f"✅ [VIEW] Converted to {len(global_metadata_display)} global and {len(personal_metadata_display)} personal display items", "BOOK_VIEW")
        else:
            debug_log(f"ℹ️ [VIEW] No metadata found", "BOOK_VIEW")
            
        # Get available custom fields for the edit mode
        personal_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=False) or []
        global_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=True) or []
        debug_log(f"✅ [VIEW] Found {len(personal_fields)} personal fields and {len(global_fields)} global fields", "BOOK_VIEW")
        
    except Exception as e:
        current_app.logger.error(f"Error loading custom metadata for display: {e}")
        debug_log(f"❌ [VIEW] Error loading custom metadata for display: {e}", "BOOK_VIEW")
    
    # Get user locations for the location dropdown and debug info
    user_locations = []
    try:
        debug_log(f"🔍 [VIEW] Getting user locations", "BOOK_VIEW")
        from app.location_service import LocationService
        from app.infrastructure.kuzu_graph import get_kuzu_connection
        from config import Config
        
        kuzu_connection = get_kuzu_connection()
        location_service = LocationService(kuzu_connection.connect())
        # Get all available locations, not just those with books
        user_locations = location_service.get_all_locations()
        debug_log(f"✅ [VIEW] Found {len(user_locations)} locations for user {current_user.id}", "BOOK_VIEW")
    except Exception as e:
        current_app.logger.error(f"Error loading user locations: {e}")
        debug_log(f"❌ [VIEW] Error loading user locations: {e}", "BOOK_VIEW")
    
    # Prepare template data
    template_data = {
        'book': user_book,
        'book_categories': book_categories,
        'global_metadata_display': global_metadata_display,
        'personal_metadata_display': personal_metadata_display,
        'user_locations': user_locations,
        'personal_fields': personal_fields,
        'global_fields': global_fields,
        'current_metadata': current_metadata
    }
    
    debug_template_data('view_book_enhanced.html', template_data, "VIEW")
    debug_log(f"🎨 [VIEW] Rendering template with {len(global_metadata_display)} global and {len(personal_metadata_display)} personal metadata items", "BOOK_VIEW")
    
    return render_template(
        'view_book_enhanced.html', 
        book=user_book,
        book_categories=book_categories,
        global_metadata_display=global_metadata_display,
        personal_metadata_display=personal_metadata_display,
        user_locations=user_locations,
        personal_fields=personal_fields,
        global_fields=global_fields,
        current_metadata=current_metadata
    )


@book_bp.route('/book/<uid>/update_details', methods=['POST'])
@login_required
def update_book_details(uid):
    """Update book details including new status system."""
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
    
    update_data = {}
    
    # Reading status
    reading_status = request.form.get('reading_status')
    if reading_status:
        update_data['reading_status'] = reading_status
    
    # Ownership status
    ownership_status = request.form.get('ownership_status')
    if ownership_status:
        update_data['ownership_status'] = ownership_status
    
    # Media type
    media_type = request.form.get('media_type')
    if media_type:
        update_data['media_type'] = media_type
    
    # Location (handle separately as it's a relationship property, not a book property)
    location_id = request.form.get('location_id')
    
    # Borrowing details
    borrowed_from = request.form.get('borrowed_from', '').strip()
    borrowed_due_date = request.form.get('borrowed_due_date')
    if ownership_status == 'borrowed':
        update_data['borrowed_from'] = borrowed_from if borrowed_from else None
        if borrowed_due_date:
            update_data['borrowed_due_date'] = datetime.strptime(borrowed_due_date, '%Y-%m-%d').date()
    
    # Loaning details
    loaned_to = request.form.get('loaned_to', '').strip()
    loaned_due_date = request.form.get('loaned_due_date')
    if ownership_status == 'loaned':
        update_data['loaned_to'] = loaned_to if loaned_to else None
        if loaned_due_date:
            update_data['loaned_due_date'] = datetime.strptime(loaned_due_date, '%Y-%m-%d').date()
    
    # Update reading dates based on status
    start_date = user_book.get('start_date') if isinstance(user_book, dict) else getattr(user_book, 'start_date', None)
    finish_date = user_book.get('finish_date') if isinstance(user_book, dict) else getattr(user_book, 'finish_date', None)
    
    if reading_status == 'reading' and not start_date:
        update_data['start_date'] = date.today()
    elif reading_status == 'read' and not finish_date:
        update_data['finish_date'] = date.today()
        if not start_date:
            update_data['start_date'] = date.today()
    
    # Use service layer to update
    try:
        print(f"🔍 [EDIT_BOOK] Calling book_service.update_book_sync with update_data: {update_data}")
        updated_book = book_service.update_book_sync(uid, str(current_user.id), **update_data)
        
        # Handle location update separately
        print(f"🔍 [EDIT_BOOK] location_id from form: '{location_id}' (type: {type(location_id)})")
        if location_id is not None:  # Allow empty string to clear location
            print(f"🔍 [EDIT_BOOK] Proceeding with location update...")
            # Use the location service to update the book location
            try:
                from app.location_service import LocationService
                from app.infrastructure.kuzu_graph import get_kuzu_connection
                
                db = get_kuzu_connection()
                connection = db.connect()
                location_service = LocationService(connection)
                
                # Convert empty string to None for clearing location
                location_success = location_service.set_book_location(
                    uid, 
                    location_id if location_id.strip() else None, 
                    str(current_user.id)
                )
                print(f"🔍 [EDIT_BOOK] Location update result: {location_success}")
                if not location_success:
                    print(f"⚠️ Failed to update location for book {uid}")
            except Exception as e:
                print(f"❌ [EDIT_BOOK] Error updating location: {e}")
        else:
            print(f"🔍 [EDIT_BOOK] Skipping location update (location_id is None)")
        
        if updated_book is not None:
            flash('Book details updated successfully.', 'success')
            
            # Redirect back to library with appropriate filter if requested
            redirect_to_library = request.form.get('redirect_to_library')
            if redirect_to_library:
                # Determine the appropriate filter based on the new reading status
                if reading_status:
                    if reading_status == 'wishlist':
                        return redirect(url_for('main.library', status_filter='wishlist'))
                    else:
                        return redirect(url_for('main.library', status_filter=reading_status))
                elif ownership_status == 'wishlist':
                    return redirect(url_for('main.library', status_filter='wishlist'))
                else:
                    return redirect(url_for('main.library'))
        else:
            flash('Failed to update book details.', 'error')
    except Exception as e:
        flash(f'Error updating book: {str(e)}', 'error')
    
    return redirect(url_for('book.view_book_enhanced', uid=uid))


@book_bp.route('/book/<uid>/update_reading_dates', methods=['POST'])
@login_required
def update_reading_dates(uid):
    """Update reading start and finish dates."""
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
    
    update_data = {}
    
    start_date_str = request.form.get('start_date', '').strip()
    finish_date_str = request.form.get('finish_date', '').strip()
    
    if start_date_str:
        update_data['start_date'] = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    
    if finish_date_str:
        update_data['finish_date'] = datetime.strptime(finish_date_str, '%Y-%m-%d').date()
    
    try:
        success = book_service.update_book_sync(uid, str(current_user.id), **update_data)
        if success:
            flash('Reading dates updated successfully.', 'success')
        else:
            flash('Failed to update reading dates.', 'error')
    except Exception as e:
        flash(f'Error updating dates: {str(e)}', 'error')
    
    return redirect(url_for('book.view_book_enhanced', uid=uid))


@book_bp.route('/book/<uid>/update_notes', methods=['POST'])
@login_required
def update_book_notes(uid):
    """Update personal notes and rating."""
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
    
    update_data = {}
    
    personal_notes = request.form.get('personal_notes', '').strip()
    user_rating = request.form.get('user_rating', '').strip()
    
    update_data['personal_notes'] = personal_notes if personal_notes else None
    
    if user_rating:
        try:
            update_data['user_rating'] = float(user_rating)
        except ValueError:
            flash('Invalid rating value.', 'error')
            return redirect(url_for('book.view_book_enhanced', uid=uid))
    
    try:
        success = book_service.update_book_sync(uid, str(current_user.id), **update_data)
        if success:
            flash('Notes and rating updated successfully.', 'success')
        else:
            flash('Failed to update notes and rating.', 'error')
    except Exception as e:
        flash(f'Error updating notes: {str(e)}', 'error')
    
    return redirect(url_for('book.view_book_enhanced', uid=uid))


@book_bp.route('/book/<uid>/custom_metadata', methods=['GET', 'POST'])
@login_required
def edit_book_custom_metadata(uid):
    """Edit custom metadata for a book."""
    from app.debug_system import debug_log, debug_metadata_operation, debug_service_call, debug_template_data
    
    try:
        debug_log(f"🔍 [EDIT_META] Starting custom metadata edit for book {uid}, user {current_user.id}", "METADATA")
        
        # Get user book with relationship data (includes custom metadata)
        debug_service_call("book_service", "get_book_by_uid_sync", {"uid": uid, "user_id": str(current_user.id)}, None, "BEFORE")
        user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
        debug_service_call("book_service", "get_book_by_uid_sync", {"uid": uid, "user_id": str(current_user.id)}, user_book, "AFTER")
        
        if not user_book:
            debug_log(f"❌ [EDIT_META] User book {uid} not found for user {current_user.id}", "METADATA")
            flash('Book not found in your library.', 'error')
            return redirect(url_for('main.library'))
        
        # Enhanced metadata debugging
        book_id = getattr(user_book, 'id', 'NO_ID')
        existing_metadata = getattr(user_book, 'custom_metadata', {})
        debug_metadata_operation(book_id, uid, str(current_user.id), existing_metadata, "LOAD")
        
        title = user_book.get('title', 'Unknown Title') if isinstance(user_book, dict) else getattr(user_book, 'title', 'Unknown Title')
        debug_log(f"✅ [EDIT_META] Found user book: {title}", "METADATA")
        debug_log(f"📊 [EDIT_META] User book custom metadata: {getattr(user_book, 'custom_metadata', 'NO ATTR')}", "METADATA")
        
        if request.method == 'POST':
            debug_log(f"🔍 [EDIT_META] Processing POST request", "METADATA")
            debug_log(f"🔍 [EDIT_META] Form data keys: {list(request.form.keys())}", "METADATA")
            debug_log(f"🔍 [EDIT_META] Full form data: {dict(request.form)}", "METADATA")
            
            # Process form data for custom metadata
            # Note: In current architecture, we're storing everything as personal metadata
            personal_metadata = {}
            
            # Get available fields (treating all as personal for now)
            debug_service_call("custom_field_service", "get_available_fields_sync", {"user_id": current_user.id, "is_global": False}, None, "BEFORE")
            personal_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=False)
            debug_service_call("custom_field_service", "get_available_fields_sync", {"user_id": current_user.id, "is_global": False}, personal_fields, "AFTER")
            
            debug_service_call("custom_field_service", "get_available_fields_sync", {"user_id": current_user.id, "is_global": True}, None, "BEFORE")
            global_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=True)
            debug_service_call("custom_field_service", "get_available_fields_sync", {"user_id": current_user.id, "is_global": True}, global_fields, "AFTER")
            
            # Ensure we have lists, not None
            personal_fields = personal_fields or []
            global_fields = global_fields or []
            all_fields = personal_fields + global_fields
            
            debug_log(f"🔍 [EDIT_META] Found {len(personal_fields)} personal fields, {len(global_fields)} global fields", "METADATA")
            
            # Process all fields as personal metadata
            for field in all_fields:
                # Check both global_ and personal_ prefixes for backward compatibility
                field_name = field.get('name', '')
                personal_key = f'personal_{field_name}'
                global_key = f'global_{field_name}'
                
                personal_value = request.form.get(personal_key, '').strip()
                global_value = request.form.get(global_key, '').strip()
                
                print(f"🔍 [EDIT_META] Field {field_name}: personal_key='{personal_key}' value='{personal_value}', global_key='{global_key}' value='{global_value}'")
                
                value = personal_value or global_value
                if value:
                    personal_metadata[field_name] = value
                    print(f"✅ [EDIT_META] Added to metadata: {field_name} = {value}")
            
            print(f"📝 [EDIT_META] Final processed metadata: {personal_metadata}")
            
            # Validate metadata - TODO: Implement validation
            valid, errors = True, []  # custom_field_service.validate_and_save_metadata(personal_metadata, current_user.id, is_global=False)
            
            if valid:
                print(f"📝 [EDIT_META] Updating personal metadata: {personal_metadata}")
                
                # Update user book relationship with personal metadata using graph storage directly
                try:
                    from app.infrastructure.kuzu_graph import get_graph_storage
                    storage = get_graph_storage()
                    
                    # Store custom metadata using the custom field service
                    success = custom_field_service.save_custom_metadata_sync(
                        uid, str(current_user.id), personal_metadata
                    )
                    
                    if success:
                        print(f"✅ [EDIT_META] Updated user book personal metadata")
                        flash('Custom metadata updated successfully!', 'success')
                        return redirect(url_for('book.view_book_enhanced', uid=uid))
                    else:
                        print(f"❌ [EDIT_META] Failed to update custom metadata")
                        flash('Failed to update custom metadata.', 'error')
                except Exception as e:
                    print(f"❌ [EDIT_META] Exception updating metadata: {e}")
                    flash('Failed to update custom metadata.', 'error')
            else:
                # Show validation errors
                for error in errors:
                    flash(f'Validation error: {error}', 'error')
        
        # Get display data for template
        # For now, treat all fields as personal since that's how they're stored
        personal_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=False) or []
        global_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=True) or []
        
        print(f"🔍 [EDIT_META] Template data preparation:")
        print(f"   📋 [EDIT_META] Personal fields count: {len(personal_fields)}")
        print(f"   📋 [EDIT_META] Global fields count: {len(global_fields)}")
        
        # Get existing custom metadata using the custom field service
        existing_metadata = custom_field_service.get_custom_metadata_sync(uid, str(current_user.id))
        print(f"   📊 [EDIT_META] Existing metadata: {existing_metadata}")
        
        # Prepare template data
        global_metadata = {}  # Empty since we're storing everything as personal
        personal_metadata = existing_metadata
        
        print(f"   📤 [EDIT_META] Passing to template:")
        print(f"      🌐 global_fields: {[f.get('name', '') for f in global_fields]}")
        print(f"      👤 personal_fields: {[f.get('name', '') for f in personal_fields]}")
        print(f"      🌐 global_metadata: {global_metadata}")
        print(f"      👤 personal_metadata: {personal_metadata}")
        
        return render_template(
            'edit_book_custom_metadata.html',
            book=user_book,
            user_book=user_book,
            global_fields=global_fields,
            personal_fields=personal_fields,
            global_metadata=global_metadata,  # Empty since we're storing everything as personal
            personal_metadata=personal_metadata,
            global_metadata_display=[],
            personal_metadata_display=custom_field_service.get_custom_metadata_for_display(existing_metadata)
        )
        
    except Exception as e:
        print(f"❌ [EDIT_META] Exception in edit_book_custom_metadata: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        flash(f'Error loading custom metadata: {str(e)}', 'error')
        return redirect(url_for('book.view_book_enhanced', uid=uid))


@book_bp.route('/month_review/<int:year>/<int:month>.jpg')
@login_required  
def month_review(year, month):
    # Query books finished in the given month/year by current user using global book visibility
    user_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
    
    # Filter books finished in the specified month/year
    start_date = datetime(year, month, 1).date()
    if month == 12:
        end_date = datetime(year + 1, 1, 1).date()
    else:
        end_date = datetime(year, month + 1, 1).date()
    
    books = [
        book for book in user_books 
        if (finish_date := getattr(book, 'finish_date', None)) and start_date <= finish_date < end_date
    ]
    
    if not books:
        # This should only be accessed if there are books (from month_wrapup)
        return "No books found", 404

    # Convert to format expected by generate_month_review_image
    # The function likely expects objects with title, author, cover_url attributes
    book_objects = []
    for book in books:
        # Create a simple object with the expected attributes
        book_obj = type('Book', (), {
            'title': book.get('title', '') if isinstance(book, dict) else getattr(book, 'title', ''),
            'author': book.get('author', '') if isinstance(book, dict) else getattr(book, 'author', ''),
            'cover_url': book.get('cover_url', '') if isinstance(book, dict) else getattr(book, 'cover_url', ''),
            'finish_date': book.get('finish_date') if isinstance(book, dict) else getattr(book, 'finish_date', None)
        })()
        book_objects.append(book_obj)

    img = generate_month_review_image(book_objects, month, year)
    buf = BytesIO()
    img.save(buf, format='JPEG')
    buf.seek(0)
    return send_file(buf, mimetype='image/jpeg', as_attachment=True, download_name=f"month_review_{year}_{month}.jpg")

@book_bp.route('/add_book_from_search', methods=['POST'])
@login_required
def add_book_from_search():
    """Add a book from search results - redirect to manual add for consistency."""
    # All book addition now goes through the unified manual add route
    return add_book_manual()


@book_bp.route('/download_db', methods=['GET'])
@login_required
def download_db():
    """Export user data from Kuzu to CSV format."""
    try:
        # Get all user books with global visibility
        user_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
        
        # Create CSV export
        import io
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow(['Title', 'Author', 'ISBN', 'Reading Status', 'Start Date', 'Finish Date', 'Rating', 'Notes'])
        
        # Write book data
        for book in user_books:
            # Handle both dict and object book types
            if isinstance(book, dict):
                author_names = ', '.join([author.get('name', '') for author in book.get('authors', [])]) if book.get('authors') else ''
                isbn = book.get('isbn13', '') or book.get('isbn10', '') or ''
                start_date = book.get('start_date', '').isoformat() if book.get('start_date') and hasattr(book.get('start_date'), 'isoformat') else ''
                finish_date = book.get('finish_date', '').isoformat() if book.get('finish_date') and hasattr(book.get('finish_date'), 'isoformat') else ''
                title = book.get('title', '')
            else:
                author_names = ', '.join([author.name for author in getattr(book, 'authors', [])]) if hasattr(book, 'authors') and getattr(book, 'authors', []) else ''
                isbn = getattr(book, 'isbn13', '') or getattr(book, 'isbn10', '') or ''
                start_date_val = getattr(book, 'start_date', None)
                start_date = start_date_val.isoformat() if start_date_val and hasattr(start_date_val, 'isoformat') else ''
                finish_date_val = getattr(book, 'finish_date', None)
                finish_date = finish_date_val.isoformat() if finish_date_val and hasattr(finish_date_val, 'isoformat') else ''
                title = getattr(book, 'title', '')
            
            rating = getattr(book, 'user_rating', '') or ''
            notes = getattr(book, 'personal_notes', '') or ''
            reading_status = getattr(book, 'reading_status', '') or ''
            writer.writerow([title, author_names, isbn, reading_status, start_date, finish_date, rating, notes])
        
        # Create response
        output.seek(0)
        response = make_response(output.getvalue())
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = f'attachment; filename=bibliotheca_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        
        return response
    except Exception as e:
        current_app.logger.error(f"Error exporting data: {e}")
        flash('Error exporting data.', 'danger')
        return redirect(url_for('main.reading_history'))

@book_bp.route('/community_activity/active_readers')
@login_required
def community_active_readers():
    """Redirect to stats page where community activity is now integrated"""
    flash('Community activity has been moved to the Stats page.', 'info')
    return redirect(url_for('main.stats'))

@book_bp.route('/community_activity/books_this_month')
@login_required
def community_books_this_month():
    """Redirect to stats page where community activity is now integrated"""
    flash('Community activity has been moved to the Stats page.', 'info')
    return redirect(url_for('main.stats'))

@book_bp.route('/community_activity/currently_reading')
@login_required
def community_currently_reading():
    """Redirect to stats page where community activity is now integrated"""
    flash('Community activity has been moved to the Stats page.', 'info')
    return redirect(url_for('main.stats'))

@book_bp.route('/community_activity/recent_activity')
@login_required
def community_recent_activity():
    """Redirect to stats page where community activity is now integrated"""
    flash('Community activity has been moved to the Stats page.', 'info')
    return redirect(url_for('main.stats'))

@book_bp.route('/user/<string:user_id>/profile')
@login_required
def user_profile(user_id):
    """Show public profile for a user if they're sharing"""
    try:
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            abort(404)
        
        # Check if user allows profile viewing
        if not user.share_reading_activity:
            flash('This user has not enabled profile sharing.', 'warning')
            return redirect(url_for('main.stats'))
        
        # Get user's books with global visibility
        all_user_books = book_service.get_all_books_with_user_overlay_sync(str(user_id))
        
        # Calculate statistics from the book list
        current_year = datetime.now().year
        current_month = datetime.now().date().replace(day=1)
        
        # Helper function to safely get date from finish_date (handles both date and datetime objects)
        def get_finish_date_safe(book):
            finish_date = getattr(book, 'finish_date', None)
            if finish_date is None:
                return None
            # Convert datetime to date if needed
            if isinstance(finish_date, datetime):
                return finish_date.date()
            return finish_date
        
        total_books = len([book for book in all_user_books if get_finish_date_safe(book)])
        
        books_this_year = len([book for book in all_user_books 
                              if (finish_date := get_finish_date_safe(book)) and 
                              finish_date >= date(current_year, 1, 1)])
        
        books_this_month = len([book for book in all_user_books 
                               if (finish_date := get_finish_date_safe(book)) and 
                               finish_date >= current_month])
        
        currently_reading = [book for book in all_user_books 
                           if getattr(book, 'start_date', None) and not getattr(book, 'finish_date', None)] if user.share_current_reading else []
        
        recent_finished = sorted([book for book in all_user_books if get_finish_date_safe(book)],
                                key=lambda x: get_finish_date_safe(x) or date.min, reverse=True)[:10]
        
        # Get reading logs count from service
        reading_logs_count = reading_log_service.get_user_logs_count_sync(user_id)
        
        return render_template('user_profile.html',
                             profile_user=user,
                             total_books=total_books,
                             books_this_year=books_this_year,
                             books_this_month=books_this_month,
                             currently_reading=currently_reading,
                             recent_finished=recent_finished,
                             reading_logs_count=reading_logs_count)
    
    except Exception as e:
        current_app.logger.error(f"Error loading user profile {user_id}: {e}")
        flash('Error loading user profile.', 'danger')
        return redirect(url_for('main.stats'))

@book_bp.route('/book/<uid>/assign', methods=['POST'])
@login_required
def assign_book(uid):
    try:
        book = book_service.get_book_by_uid_sync(uid, current_user.id)
        if not book:
            abort(404)
            
        if not current_user.is_admin:
            flash('Only admins can assign books.', 'danger')
            return redirect(url_for('main.library'))

        user_id = request.form.get('user_id')
        if not user_id:
            flash('No user selected.', 'danger')
            return redirect(url_for('main.library'))
            
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            flash('Invalid user selected.', 'danger')
            return redirect(url_for('main.library'))

        # For Kuzu, this would involve transferring the book to another user
        # This is a complex operation that would need to be implemented in the service layer
        flash('Book assignment feature needs to be implemented for Kuzu backend.', 'warning')
        return redirect(url_for('main.library'))
    except Exception as e:
        current_app.logger.error(f"Error assigning book: {e}")
        flash('Error assigning book.', 'danger')
        return redirect(url_for('main.library'))

@book_bp.route('/books/bulk_delete', methods=['POST'])
@login_required
def bulk_delete_books():
    """Delete multiple books selected from the library view."""
    print(f"Bulk delete route called by user {current_user.id}")
    print(f"Request method: {request.method}")
    print(f"Request form keys: {list(request.form.keys())}")
    print(f"Full form data: {dict(request.form)}")
    
    # Try both possible field names
    selected_uids = request.form.getlist('selected_books')
    if not selected_uids:
        selected_uids = request.form.getlist('book_ids')
    
    # If still empty, try getting as single values
    if not selected_uids:
        single_value = request.form.get('selected_books') or request.form.get('book_ids')
        if single_value:
            # Split by comma or newline if multiple values in single field
            selected_uids = [uid.strip() for uid in single_value.replace(',', '\n').split('\n') if uid.strip()]
    
    # Filter out empty strings
    selected_uids = [uid for uid in selected_uids if uid and uid.strip()]
    
    print(f"Selected UIDs after filtering: {selected_uids}")
    
    if not selected_uids:
        print("No books selected for deletion")
        flash('No books selected for deletion.', 'warning')
        return redirect(url_for('main.library'))
    
    deleted_count = 0
    failed_count = 0
    
    for uid in selected_uids:
        try:
            print(f"Attempting to delete book {uid} for user {current_user.id}")
            # Use the regular delete_book_sync method which handles global deletion
            # if no other users have the book
            success = book_service.delete_book_sync(uid, str(current_user.id))
            print(f"Delete result for {uid}: {success}")
            if success:
                deleted_count += 1
            else:
                failed_count += 1
        except Exception as e:
            print(f"Error deleting book {uid}: {e}")
            import traceback
            traceback.print_exc()
            failed_count += 1
    
    print(f"Bulk delete completed: {deleted_count} deleted, {failed_count} failed")
    
    if deleted_count > 0:
        flash(f'Successfully deleted {deleted_count} book(s) from your library.', 'success')
    if failed_count > 0:
        flash(f'Failed to delete {failed_count} book(s).', 'error')
    
    return redirect(url_for('main.library'))

@book_bp.route('/csrf-guide')
def csrf_guide():
    """Demo page showing CSRF protection implementation."""
    return render_template('csrf_guide.html')

@book_bp.route('/stats')
@login_required
def stats():
    """Redirect to new stats page."""
    return redirect(url_for('stats.index'))

@book_bp.route('/search_books_in_library', methods=['POST'])
@login_required
def search_books_in_library():
    """Search for books to add from the library page"""
    query = request.form.get('query', '')
    results = []
    
    if query:
        # Google Books API search
        resp = requests.get(
            'https://www.googleapis.com/books/v1/volumes',
            params={'q': query, 'maxResults': 10}
        )
        data = resp.json()
        for item in data.get('items', []):
            volume_info = item.get('volumeInfo', {})
            image = volume_info.get('imageLinks', {}).get('thumbnail')
            isbn = None
            for iden in volume_info.get('industryIdentifiers', []):
                if iden['type'] in ('ISBN_13', 'ISBN_10'):
                    isbn = iden['identifier']
                    break
            results.append({
                'title': volume_info.get('title'),
                'authors': ', '.join(volume_info.get('authors', [])),
                'image': image,
                'isbn': isbn
            })
    
    # Get all the data needed for library page
    # Get filter parameters from URL
    category_filter = request.args.get('category', '')
    publisher_filter = request.args.get('publisher', '')
    language_filter = request.args.get('language', '')
    location_filter = request.args.get('location', '')
    search_query = request.args.get('search', '')

    # Use service layer with global book visibility
    user_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
    
    # Apply filters in Python (Kuzu doesn't have complex querying like SQL)
    filtered_books = user_books
    
    if search_query:
        search_lower = search_query.lower()
        filtered_books = [
            book for book in filtered_books 
            if (search_lower in (book.get('title', '') if isinstance(book, dict) else getattr(book, 'title', '')).lower()) or
               (search_lower in (book.get('author', '') if isinstance(book, dict) else getattr(book, 'author', '')).lower()) or
               (search_lower in (book.get('description', '') if isinstance(book, dict) else getattr(book, 'description', '')).lower())
        ]
    if publisher_filter:
        filtered_books = [
            book for book in filtered_books 
            if (book.get('publisher', '') if isinstance(book, dict) else getattr(book, 'publisher', '')) and publisher_filter.lower() in (book.get('publisher', '') if isinstance(book, dict) else getattr(book, 'publisher', '')).lower()
        ]
    if language_filter:
        filtered_books = [
            book for book in filtered_books 
            if (book.get('language', '') if isinstance(book, dict) else getattr(book, 'language', '')) == language_filter
        ]
    if location_filter:
        filtered_books = [
            book for book in filtered_books 
            if (book.get('locations', []) if isinstance(book, dict) else getattr(book, 'locations', [])) and any(
                location_filter.lower() in (loc.get('name', '') if isinstance(loc, dict) else getattr(loc, 'name', '')).lower() 
                for loc in (book.get('locations', []) if isinstance(book, dict) else getattr(book, 'locations', []))
                if (loc.get('name', '') if isinstance(loc, dict) else getattr(loc, 'name', ''))
            )
        ]
    if category_filter:
        filtered_books = [
            book for book in filtered_books 
            if (book.get('categories', []) if isinstance(book, dict) else getattr(book, 'categories', [])) and any(
                category_filter.lower() in (cat.get('name', '') if isinstance(cat, dict) else getattr(cat, 'name', '')).lower() 
                for cat in (book.get('categories', []) if isinstance(book, dict) else getattr(book, 'categories', []))
                if (cat.get('name', '') if isinstance(cat, dict) else getattr(cat, 'name', ''))
            )
        ]

    # Books are already in the right format for the template
    books = filtered_books

    # Get distinct values for filter dropdowns
    all_books = user_books  # Use same data
    
    categories = set()
    publishers = set()
    languages = set()
    locations = set()

    for book in all_books:
        book_categories = book.get('categories', []) if isinstance(book, dict) else getattr(book, 'categories', [])
        if book_categories:
            # book.categories is a list of Category objects, not a string
            categories.update([
                cat.get('name', '') if isinstance(cat, dict) else getattr(cat, 'name', '')
                for cat in book_categories 
                if (cat.get('name', '') if isinstance(cat, dict) else getattr(cat, 'name', ''))
            ])
        
        book_publisher = book.get('publisher', None) if isinstance(book, dict) else getattr(book, 'publisher', None)
        if book_publisher:
            # Handle Publisher domain object or string
            if isinstance(book_publisher, dict):
                publisher_name = book_publisher.get('name', str(book_publisher))
            else:
                publisher_name = getattr(book_publisher, 'name', str(book_publisher))
            publishers.add(publisher_name)
        
        book_language = book.get('language', '') if isinstance(book, dict) else getattr(book, 'language', '')
        if book_language:
            languages.add(book_language)
        
        book_locations = book.get('locations', []) if isinstance(book, dict) else getattr(book, 'locations', [])
        if book_locations:
            # book.locations is a list of Location objects
            locations.update([
                loc.get('name', '') if isinstance(loc, dict) else getattr(loc, 'name', '')
                for loc in book_locations 
                if (loc.get('name', '') if isinstance(loc, dict) else getattr(loc, 'name', ''))
            ])

    # Get users through Kuzu service layer
    domain_users = user_service.get_all_users_sync() or []
    
    # Convert domain users to simple objects for template compatibility
    users = []
    for domain_user in domain_users:
        user_data = {
            'id': domain_user.id,
            'username': domain_user.username,
            'email': domain_user.email
        }
        users.append(type('User', (), user_data))

    # Calculate statistics for filter buttons
    stats = {
        'total_books': len(user_books),
        'books_read': len([b for b in user_books if getattr(b, 'reading_status', None) == 'read']),
        'currently_reading': len([b for b in user_books if getattr(b, 'reading_status', None) == 'reading']),
        'want_to_read': len([b for b in user_books if getattr(b, 'reading_status', None) == 'plan_to_read']),
        'on_hold': len([b for b in user_books if getattr(b, 'reading_status', None) == 'on_hold']),
        'wishlist': len([b for b in user_books if getattr(b, 'ownership_status', None) == 'wishlist']),
    }

    return render_template(
        'library_enhanced.html',
        books=books,
        stats=stats,
        categories=sorted(categories),
        publishers=sorted(publishers),
        languages=sorted(languages),
        locations=sorted(locations),
        current_status_filter='all',
        current_category=category_filter,
        current_publisher=publisher_filter,
        current_language=language_filter,
        current_location=location_filter,
        current_search=search_query,
        current_sort='title_asc',
        users=users,
        search_results=results,
        search_query=query
    )

@book_bp.route('/add_book_manual', methods=['POST'])
@login_required
def add_book_manual():
    """Add a book manually from the library page"""
    # 🔥 SIMPLIFIED ARCHITECTURE INTERCEPT
    # Use simplified service to avoid complex transaction issues
    try:
        print(f"📚 [INTERCEPT] Using simplified architecture for manual book addition")
        
        # Get form data
        title = request.form['title'].strip()
        if not title:
            flash('Error: Title is required to add a book.', 'danger')
            return redirect(url_for('main.library'))
        
        author = request.form.get('author', '').strip()
        isbn = request.form.get('isbn', '').strip()
        subtitle = request.form.get('subtitle', '').strip()
        publisher_name = request.form.get('publisher', '').strip()
        description = request.form.get('description', '').strip()
        page_count_str = request.form.get('page_count', '').strip()
        language = request.form.get('language', '').strip() or 'en'
        cover_url = request.form.get('cover_url', '').strip()
        published_date_str = request.form.get('published_date', '').strip()
        series = request.form.get('series', '').strip()
        series_volume = request.form.get('series_volume', '').strip()
        
        # Parse page count
        page_count = None
        if page_count_str:
            try:
                page_count = int(page_count_str)
            except ValueError:
                pass
        
        # Parse categories from manual_categories field
        categories = []
        manual_cats = request.form.get('manual_categories')
        if manual_cats:
            categories = [cat.strip() for cat in manual_cats.split(',') if cat.strip()]
        
        # Get location
        location_id = request.form.get('location_id')
        
        # Get ownership details
        reading_status = request.form.get('reading_status', 'plan_to_read')
        ownership_status = request.form.get('ownership_status', 'owned')
        media_type = request.form.get('media_type', 'physical')
        
        # Enhanced ISBN processing with comprehensive field mapping
        isbn13 = None
        isbn10 = None
        api_data = None
        cached_cover_url = None
        
        if isbn:
            # Import required modules
            import re
            import requests
            import uuid
            from pathlib import Path
            
            # Normalize ISBN with proper conversion
            clean_isbn = re.sub(r'[^0-9X]', '', str(isbn).upper())
            
            if len(clean_isbn) == 10:
                # ISBN10 - validate and convert to ISBN13
                isbn10 = clean_isbn
                # Convert ISBN10 to ISBN13
                isbn13_base = "978" + clean_isbn[:9]
                check_sum = 0
                for i, digit in enumerate(isbn13_base):
                    check_sum += int(digit) * (1 if i % 2 == 0 else 3)
                check_digit = (10 - (check_sum % 10)) % 10
                isbn13 = isbn13_base + str(check_digit)
                print(f"📚 [MANUAL] Converted ISBN10 {isbn10} to ISBN13 {isbn13}")
            elif len(clean_isbn) == 13:
                # ISBN13 - try to convert to ISBN10 if it starts with 978
                isbn13 = clean_isbn
                if clean_isbn.startswith('978'):
                    isbn10_base = clean_isbn[3:12]
                    check_sum = 0
                    for i, digit in enumerate(isbn10_base):
                        check_sum += int(digit) * (10 - i)
                    check_digit = (11 - (check_sum % 11)) % 11
                    if check_digit == 10:
                        check_digit = 'X'
                    elif check_digit == 11:
                        check_digit = '0'
                    isbn10 = isbn10_base + str(check_digit)
                    print(f"📚 [MANUAL] Converted ISBN13 {isbn13} to ISBN10 {isbn10}")
            
            # Enhanced API lookup with both Google Books and OpenLibrary
            normalized_isbn = isbn13 or isbn10
            if normalized_isbn:
                print(f"🔍 [MANUAL] Performing enhanced API lookup for ISBN: {normalized_isbn}")
                
                try:
                    # Google Books lookup
                    google_data = None
                    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{normalized_isbn}"
                    response = requests.get(url, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        items = data.get("items")
                        if items:
                            volume_info = items[0]["volumeInfo"]
                            google_data = {
                                'title': volume_info.get('title', ''),
                                'subtitle': volume_info.get('subtitle', ''),
                                'description': volume_info.get('description', ''),
                                'authors_list': volume_info.get('authors', []),
                                'publisher': volume_info.get('publisher', ''),
                                'published_date': volume_info.get('publishedDate', ''),
                                'page_count': volume_info.get('pageCount'),
                                'language': volume_info.get('language', 'en'),
                                'categories': volume_info.get('categories', []),
                                'isbn10': isbn10,
                                'isbn13': isbn13,
                                'cover_url': None
                            }
                            
                            # Get best quality cover image
                            image_links = volume_info.get("imageLinks", {})
                            for size in ['extraLarge', 'large', 'medium', 'thumbnail', 'smallThumbnail']:
                                if size in image_links:
                                    google_data['cover_url'] = image_links[size].replace('http://', 'https://')
                                    break
                            
                            print(f"✅ [MANUAL] Google Books data retrieved")
                    
                    # OpenLibrary lookup
                    openlibrary_data = None
                    url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{normalized_isbn}&format=json&jscmd=data"
                    response = requests.get(url, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        book_key = f"ISBN:{normalized_isbn}"
                        if book_key in data:
                            book = data[book_key]
                            
                            # Extract categories/subjects
                            subjects = book.get('subjects', [])
                            ol_categories = []
                            for subject in subjects[:10]:  # Limit to 10
                                if isinstance(subject, dict):
                                    ol_categories.append(subject.get('name', ''))
                                else:
                                    ol_categories.append(str(subject))
                            ol_categories = [cat for cat in ol_categories if cat]
                            
                            openlibrary_data = {
                                'categories': ol_categories,
                                'description': book.get('notes', {}).get('value', '') if isinstance(book.get('notes'), dict) else str(book.get('notes', '')),
                                'cover_url': book.get('cover', {}).get('large') or book.get('cover', {}).get('medium') or book.get('cover', {}).get('small')
                            }
                            print(f"✅ [MANUAL] OpenLibrary data retrieved")
                    
                    # Merge API data
                    if google_data or openlibrary_data:
                        api_data = google_data or {}
                        if openlibrary_data:
                            # Merge categories
                            google_cats = set(api_data.get('categories', []))
                            ol_cats = set(openlibrary_data.get('categories', []))
                            api_data['categories'] = list(google_cats.union(ol_cats))
                            
                            # Use OpenLibrary description if Google doesn't have one
                            if not api_data.get('description') and openlibrary_data.get('description'):
                                api_data['description'] = openlibrary_data['description']
                        
                        print(f"🎯 [MANUAL] Merged API data: {len(api_data.get('categories', []))} categories, cover: {bool(api_data.get('cover_url'))}")
                        
                        # Use API data to fill in missing form fields
                        if not cover_url and api_data.get('cover_url'):
                            cover_url = api_data['cover_url']
                        if not description and api_data.get('description'):
                            description = api_data['description']
                        if not publisher_name and api_data.get('publisher'):
                            publisher_name = api_data['publisher']
                        if not page_count and api_data.get('page_count'):
                            page_count = api_data['page_count']
                        if not published_date_str and api_data.get('published_date'):
                            published_date_str = api_data['published_date']
                        if not categories and api_data.get('categories'):
                            categories = api_data['categories']
                        
                        # Download and cache cover image
                        if cover_url:
                            try:
                                book_temp_id = str(uuid.uuid4())
                                
                                # Use persistent covers directory
                                import os
                                covers_dir = Path('/app/static/covers')
                                
                                # Fallback to local development path if Docker path doesn't exist
                                if not covers_dir.exists():
                                    static_folder = current_app.static_folder or 'app/static'
                                    covers_dir = Path(static_folder) / 'covers'
                                
                                covers_dir.mkdir(parents=True, exist_ok=True)
                                
                                # Generate filename
                                file_extension = '.jpg'
                                if cover_url.lower().endswith('.png'):
                                    file_extension = '.png'
                                
                                filename = f"{book_temp_id}{file_extension}"
                                filepath = covers_dir / filename
                                
                                # Download the image
                                response = requests.get(cover_url, timeout=10, stream=True)
                                response.raise_for_status()
                                
                                with open(filepath, 'wb') as f:
                                    for chunk in response.iter_content(chunk_size=8192):
                                        f.write(chunk)
                                
                                cached_cover_url = f"/static/covers/{filename}"
                                cover_url = cached_cover_url
                                print(f"🖼️ [MANUAL] Cover cached: {cached_cover_url}")
                                
                            except Exception as e:
                                print(f"⚠️ [MANUAL] Failed to cache cover: {e}")
                                # If caching fails, use the original URL
                                print(f"🔗 [MANUAL] Using original cover URL: {cover_url}")
                
                except Exception as e:
                    print(f"⚠️ [MANUAL] API lookup failed: {e}")
        
        # Create simplified book data with enhanced API fields
        book_data = SimplifiedBook(
            title=title,
            author=author or "Unknown Author",
            isbn13=isbn13,
            isbn10=isbn10,
            subtitle=subtitle,
            description=description,
            publisher=publisher_name,
            published_date=published_date_str,
            page_count=page_count,
            language=language,
            cover_url=cover_url,  # This will be the cached URL if available
            series=series,
            series_volume=series_volume,
            categories=categories  # Enhanced with API data
        )
        
        # Enhanced debugging for ISBN and cover
        print(f"🔍 [MANUAL] Final book data:")
        print(f"   Title: {book_data.title}")
        print(f"   Author: {book_data.author}")
        print(f"   ISBN13: {book_data.isbn13}")
        print(f"   ISBN10: {book_data.isbn10}")
        print(f"   Cover URL: {book_data.cover_url}")
        print(f"   Categories: {book_data.categories}")
        print(f"   Description: {book_data.description[:100] if book_data.description else 'None'}...")
        print(f"   Publisher: {book_data.publisher}")
        print(f"   Page count: {book_data.page_count}")
        
        # Use simplified service
        service = SimplifiedBookService()
        success = service.add_book_to_user_library_sync(
            book_data=book_data,
            user_id=current_user.id,
            reading_status=reading_status,
            ownership_status=ownership_status,
            media_type=media_type,
            location_id=location_id
        )
        
        if success:
            flash(f'Successfully added "{title}" to your library!', 'success')
            print(f"✅ [INTERCEPT] Successfully added book using simplified architecture")
        else:
            flash('Failed to add book. Please try again.', 'danger')
            print(f"❌ [INTERCEPT] Failed to add book using simplified architecture")
            
        return redirect(url_for('main.library'))
        
    except Exception as e:
        print(f"❌ [INTERCEPT] Simplified architecture failed: {e}")
        flash('Error adding book. Please try again.', 'danger')
        return redirect(url_for('main.library'))
    
    # ORIGINAL FUNCTION BELOW (kept as fallback, but should never be reached)
    # Validate required fields
    title = request.form['title'].strip()
    if not title:
        flash('Error: Title is required to add a book.', 'danger')
        return redirect(url_for('main.library'))

    # Extract all form fields
    isbn = request.form.get('isbn', '').strip()
    author = request.form.get('author', '').strip()
    subtitle = request.form.get('subtitle', '').strip()
    publisher_name = request.form.get('publisher', '').strip()
    page_count_str = request.form.get('page_count', '').strip()
    language = request.form.get('language', '').strip() or 'en'
    cover_url = request.form.get('cover_url', '').strip()
    published_date_str = request.form.get('published_date', '').strip()
    series = request.form.get('series', '').strip()
    series_volume = request.form.get('series_volume', '').strip()
    series_order_str = request.form.get('series_order', '').strip()
    genres = request.form.get('genres', '').strip()  # Changed from 'genre' to 'genres'
    reading_status = request.form.get('reading_status', '').strip()
    ownership_status = request.form.get('ownership_status', '').strip()
    media_type = request.form.get('media_type', '').strip()
    location_id = request.form.get('location_id', '').strip()
    user_rating_str = request.form.get('user_rating', '').strip()
    personal_notes = request.form.get('personal_notes', '').strip()
    user_tags = request.form.get('user_tags', '').strip()
    description = request.form.get('description', '').strip()
    
    # Convert numeric fields
    page_count = None
    if page_count_str:
        try:
            page_count = int(page_count_str)
        except ValueError:
            pass
    
    series_order = None
    if series_order_str:
        try:
            series_order = int(series_order_str)
        except ValueError:
            pass
    
    user_rating = None
    if user_rating_str:
        try:
            user_rating = float(user_rating_str)
        except ValueError:
            pass
    
    # Convert date fields
    published_date = None
    if published_date_str:
        try:
            published_date = datetime.strptime(published_date_str, '%Y-%m-%d').date()
        except ValueError:
            pass
    
    # Enhanced ISBN processing with proper normalization
    isbn10 = None
    isbn13 = None
    normalized_isbn = None
    
    if isbn:
        # Enhanced ISBN normalization
        import re
        clean_isbn = re.sub(r'[^0-9X]', '', str(isbn).upper())
        
        if len(clean_isbn) == 10:
            # ISBN10 - validate and convert to ISBN13
            isbn10 = clean_isbn
            # Convert ISBN10 to ISBN13
            isbn13_base = "978" + clean_isbn[:9]
            check_sum = 0
            for i, digit in enumerate(isbn13_base):
                check_sum += int(digit) * (1 if i % 2 == 0 else 3)
            check_digit = (10 - (check_sum % 10)) % 10
            isbn13 = isbn13_base + str(check_digit)
            normalized_isbn = isbn13  # Prefer ISBN13 for API lookups
        elif len(clean_isbn) == 13:
            # ISBN13 - try to convert to ISBN10 if it starts with 978
            isbn13 = clean_isbn
            normalized_isbn = isbn13
            if clean_isbn.startswith('978'):
                isbn10_base = clean_isbn[3:12]
                check_sum = 0
                for i, digit in enumerate(isbn10_base):
                    check_sum += int(digit) * (10 - i)
                check_digit = (11 - (check_sum % 11)) % 11
                if check_digit == 10:
                    check_digit = 'X'
                elif check_digit == 11:
                    check_digit = '0'
                isbn10 = isbn10_base + str(check_digit)
        
        print(f"📚 [MANUAL] Enhanced ISBN processing: {isbn} -> ISBN10: {isbn10}, ISBN13: {isbn13}")
    
    # Process manual genre input
    manual_categories = []
    if genres:
        # Split by comma and clean up
        manual_categories = [cat.strip() for cat in genres.split(',') if cat.strip()]
        print(f"📚 [MANUAL] Manual categories: {manual_categories}")
    
    start_date_str = request.form.get('start_date') or None
    finish_date_str = request.form.get('finish_date') or None
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
    finish_date = datetime.strptime(finish_date_str, '%Y-%m-%d').date() if finish_date_str else None

    # Process custom metadata - only allow selection of existing fields
    custom_metadata = {}
    
    # Get all submitted custom field selections
    for key, value in request.form.items():
        if key.startswith('custom_field_') and value and value.strip():
            # Extract field name from the form key (custom_field_<field_name>)
            field_name = key.replace('custom_field_', '')
            field_value = value.strip()
            
            print(f"📋 [MANUAL] Selected existing custom field: {field_name} = {field_value}")
            custom_metadata[field_name] = field_value

    print(f"📊 [MANUAL] Final custom metadata: {custom_metadata}")

    # Enhanced API lookup with comprehensive field mapping
    api_data = None
    final_cover_url = cover_url
    cached_cover_url = None
    
    if normalized_isbn:
        print(f"🔍 [MANUAL] Performing enhanced API lookup for ISBN: {normalized_isbn}")
        
        # Enhanced Google Books API lookup
        def enhanced_google_books_lookup(isbn):
            url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
            try:
                import requests
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                items = data.get("items")
                if not items:
                    return None
                
                volume_info = items[0]["volumeInfo"]
                
                # Extract ISBN information from industryIdentifiers
                api_isbn10 = None
                api_isbn13 = None
                asin = None
                
                industry_identifiers = volume_info.get('industryIdentifiers', [])
                for identifier in industry_identifiers:
                    id_type = identifier.get('type', '')
                    id_value = identifier.get('identifier', '')
                    
                    if id_type == 'ISBN_10':
                        api_isbn10 = id_value
                    elif id_type == 'ISBN_13':
                        api_isbn13 = id_value
                    elif id_type == 'OTHER' and 'ASIN' in id_value:
                        asin = id_value
                
                # Get best quality cover image
                image_links = volume_info.get("imageLinks", {})
                cover_url = None
                for size in ['extraLarge', 'large', 'medium', 'thumbnail', 'smallThumbnail']:
                    if size in image_links:
                        cover_url = image_links[size]
                        break
                
                # Force HTTPS for cover URLs
                if cover_url and cover_url.startswith('http://'):
                    cover_url = cover_url.replace('http://', 'https://')
                
                return {
                    'title': volume_info.get('title', ''),
                    'subtitle': volume_info.get('subtitle', ''),
                    'description': volume_info.get('description', ''),
                    'authors_list': volume_info.get('authors', []),
                    'publisher': volume_info.get('publisher', ''),
                    'published_date': volume_info.get('publishedDate', ''),
                    'page_count': volume_info.get('pageCount'),
                    'language': volume_info.get('language', 'en'),
                    'average_rating': volume_info.get('averageRating'),
                    'rating_count': volume_info.get('ratingsCount'),
                    'categories': volume_info.get('categories', []),
                    'isbn10': api_isbn10,
                    'isbn13': api_isbn13,
                    'asin': asin,
                    'cover_url': cover_url,
                    'google_books_id': items[0].get('id', ''),
                    'source': 'google_books'
                }
            except Exception as e:
                print(f"❌ Google Books API error: {e}")
                return None
        
        # Enhanced OpenLibrary API lookup
        def enhanced_openlibrary_lookup(isbn):
            url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
            try:
                import requests
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                book_key = f"ISBN:{isbn}"
                if book_key not in data:
                    return None
                
                book = data[book_key]
                
                # Extract authors
                authors_list = []
                authors_data = book.get('authors', [])
                for author in authors_data:
                    if isinstance(author, dict) and 'name' in author:
                        authors_list.append(author['name'])
                    elif isinstance(author, str):
                        authors_list.append(author)
                
                # Extract publisher
                publishers = book.get('publishers', [])
                publisher = ''
                if publishers:
                    if isinstance(publishers[0], dict):
                        publisher = publishers[0].get('name', '')
                    else:
                        publisher = str(publishers[0])
                
                # Extract categories/subjects
                subjects = book.get('subjects', [])
                categories = []
                for subject in subjects[:10]:  # Limit to 10 categories
                    if isinstance(subject, dict):
                        categories.append(subject.get('name', ''))
                    else:
                        categories.append(str(subject))
                categories = [cat for cat in categories if cat]
                
                # Extract cover image
                cover_data = book.get('cover', {})
                cover_url = None
                for size in ['large', 'medium', 'small']:
                    if size in cover_data:
                        cover_url = cover_data[size]
                        break
                
                # Description handling
                description = book.get('notes', {})
                if isinstance(description, dict):
                    description = description.get('value', '')
                elif not isinstance(description, str):
                    description = ''
                
                return {
                    'title': book.get('title', ''),
                    'subtitle': book.get('subtitle', ''),
                    'description': description,
                    'authors_list': authors_list,
                    'publisher': publisher,
                    'published_date': book.get('publish_date', ''),
                    'page_count': book.get('number_of_pages'),
                    'language': 'en',  # Default, could be enhanced
                    'categories': categories,
                    'cover_url': cover_url,
                    'openlibrary_id': book.get('key', '').replace('/books/', '') if book.get('key') else '',
                    'source': 'openlibrary'
                }
            except Exception as e:
                print(f"❌ OpenLibrary API error: {e}")
                return None
        
        # Perform API lookups
        google_data = enhanced_google_books_lookup(normalized_isbn)
        ol_data = enhanced_openlibrary_lookup(normalized_isbn)
        
        # Merge API data with priority to Google Books
        if google_data or ol_data:
            if google_data and ol_data:
                # Merge strategy: Google Books for most fields, OpenLibrary for fallbacks
                api_data = google_data.copy()
                
                # Use OpenLibrary data for missing fields
                if not api_data.get('description') and ol_data.get('description'):
                    api_data['description'] = ol_data['description']
                if not api_data.get('publisher') and ol_data.get('publisher'):
                    api_data['publisher'] = ol_data['publisher']
                if not api_data.get('page_count') and ol_data.get('page_count'):
                    api_data['page_count'] = ol_data['page_count']
                
                # Merge categories
                google_cats = set(api_data.get('categories', []))
                ol_cats = set(ol_data.get('categories', []))
                api_data['categories'] = list(google_cats.union(ol_cats))
                
                # Add OpenLibrary ID
                if ol_data.get('openlibrary_id'):
                    api_data['openlibrary_id'] = ol_data['openlibrary_id']
                
                api_data['sources'] = ['google_books', 'openlibrary']
                
            elif google_data:
                api_data = google_data
            elif ol_data:
                api_data = ol_data
            
            print(f"✅ [MANUAL] API data retrieved from {api_data.get('sources', api_data.get('source', 'unknown'))}")
            
            # Enhanced ISBN handling from API
            if not isbn13 and api_data.get('isbn13'):
                isbn13 = api_data['isbn13']
            if not isbn10 and api_data.get('isbn10'):
                isbn10 = api_data['isbn10']
            
            # Enhanced cover image handling with caching
            if api_data.get('cover_url') and not cover_url:  # Only if no manual cover URL provided
                final_cover_url = api_data['cover_url']
                
                # Download and cache cover image
                try:
                    import requests
                    from pathlib import Path
                    
                    # Use persistent covers directory
                    import os
                    covers_dir = Path('/app/static/covers')
                    
                    # Fallback to local development path if Docker path doesn't exist
                    if not covers_dir.exists():
                        static_folder = current_app.static_folder or 'app/static'
                        covers_dir = Path(static_folder) / 'covers'
                    
                    covers_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Generate filename
                    book_temp_id = str(uuid.uuid4())
                    file_extension = '.jpg'
                    if final_cover_url.lower().endswith('.png'):
                        file_extension = '.png'
                    elif final_cover_url.lower().endswith('.gif'):
                        file_extension = '.gif'
                    
                    filename = f"{book_temp_id}{file_extension}"
                    filepath = covers_dir / filename
                    
                    # Download image
                    response = requests.get(final_cover_url, timeout=10, stream=True)
                    response.raise_for_status()
                    
                    with open(filepath, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    
                    cached_cover_url = f"/static/covers/{filename}"
                    print(f"🖼️ [MANUAL] Cover cached: {final_cover_url} -> {cached_cover_url}")
                    
                except Exception as e:
                    print(f"❌ [MANUAL] Failed to cache cover: {e}")
                    cached_cover_url = final_cover_url  # Fallback to original URL
        else:
            print(f"❌ [MANUAL] No API data found for ISBN: {normalized_isbn}")
    
    # Enhanced field mapping with API fallbacks
    final_title = title or (api_data.get('title') if api_data else '')
    final_subtitle = subtitle if subtitle else (api_data.get('subtitle') if api_data else None)
    final_description = description or (api_data.get('description') if api_data else '')
    final_publisher = publisher_name or (api_data.get('publisher') if api_data else '')
    final_language = language or (api_data.get('language') if api_data else 'en')
    final_page_count = page_count or (api_data.get('page_count') if api_data else None)
    final_cover_url = cached_cover_url or final_cover_url or cover_url
    
    # Enhanced published date handling
    final_published_date = published_date
    if not final_published_date and api_data and api_data.get('published_date'):
        try:
            api_date_str = api_data['published_date']
            final_published_date = datetime.strptime(api_date_str, '%Y-%m-%d').date()
        except ValueError:
            try:
                final_published_date = datetime.strptime(api_date_str, '%Y').date()
            except ValueError:
                pass

    # Enhanced category processing with API integration
    final_categories = []
    
    # Add API categories first
    if api_data and api_data.get('categories'):
        api_categories = api_data['categories']
        if isinstance(api_categories, list):
            final_categories.extend(api_categories)
        elif isinstance(api_categories, str):
            # Handle comma-separated string categories
            final_categories.extend([cat.strip() for cat in api_categories.split(',') if cat.strip()])
    
    # Add manual categories
    final_categories.extend(manual_categories)
    
    # Remove duplicates while preserving order and normalize
    seen = set()
    unique_categories = []
    for cat in final_categories:
        cat_normalized = cat.lower().strip()
        if cat_normalized and cat_normalized not in seen:
            unique_categories.append(cat.strip())
            seen.add(cat_normalized)
    
    print(f"📚 [MANUAL] Final categories (API + manual): {unique_categories}")
    
    # Use manual publisher if provided, otherwise use API publisher
    final_publisher = publisher_name or publisher

    try:
        # Enhanced contributors creation from API data
        contributors = []
        
        # Prioritize individual authors from enhanced API data
        if api_data and api_data.get('authors_list'):
            for i, author_name in enumerate(api_data['authors_list']):
                author_name = author_name.strip()
                if author_name:
                    person = Person(id=str(uuid.uuid4()), name=author_name)
                    contribution = BookContribution(
                        person=person,
                        contribution_type=ContributionType.AUTHORED,
                        order=i
                    )
                    contributors.append(contribution)
                    print(f"👤 [MANUAL] Added author from API: {author_name}")
        elif author:
            # Fallback to manual form entry
            person = Person(id=str(uuid.uuid4()), name=author)
            contribution = BookContribution(
                person=person,
                contribution_type=ContributionType.AUTHORED,
                order=0
            )
            contributors.append(contribution)
            print(f"👤 [MANUAL] Added author from form: {author}")
            
        # Enhanced domain book object creation with comprehensive field mapping
        domain_book = DomainBook(
            id=str(uuid.uuid4()),
            title=final_title,
            subtitle=final_subtitle,
            description=final_description if final_description else None,
            published_date=final_published_date,
            page_count=final_page_count,
            language=final_language,
            cover_url=final_cover_url,
            isbn13=isbn13,
            isbn10=isbn10,
            asin=api_data.get('asin') if api_data else None,
            google_books_id=api_data.get('google_books_id') if api_data else None,
            openlibrary_id=api_data.get('openlibrary_id') if api_data else None,
            average_rating=api_data.get('average_rating') if api_data else None,
            rating_count=api_data.get('rating_count') if api_data else None,
            contributors=contributors,
            publisher=Publisher(id=str(uuid.uuid4()), name=final_publisher) if final_publisher else None,
            series=Series(id=str(uuid.uuid4()), name=series) if series else None,
            series_volume=series_volume if series_volume else None,
            series_order=series_order,
            categories=unique_categories or [],
            raw_categories=unique_categories,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        # Use find_or_create_book to avoid duplicates (global)
        existing_book = book_service.find_or_create_book_sync(domain_book)
        
        # Categories are already processed by find_or_create_book_sync, no need to process again
        if unique_categories:
            print(f"📚 [MANUAL] Categories already processed during book creation: {unique_categories}")
        else:
            print(f"📚 [MANUAL] No categories found for book {title}")
        
        # Add to user's library with custom metadata and location
        # Determine location to use: form-selected location takes priority, then default location
        final_locations = []
        try:
            from .location_service import LocationService
            from .infrastructure.kuzu_graph import get_kuzu_connection
            from config import Config
            
            kuzu_connection = get_kuzu_connection()
            location_service = LocationService(kuzu_connection.connect())
            
            # Check if user selected a location in the form
            if location_id:
                print(f"📍 [MANUAL] User selected location from form: {location_id}")
                final_locations = [location_id]
            else:
                print(f"📍 [MANUAL] No location selected in form, attempting to get default location for user {current_user.id}")
                
                default_location = location_service.get_default_location(str(current_user.id))
                
                if default_location:
                    final_locations = [default_location.id]
                    print(f"📍 [MANUAL] ✅ Found default location: {default_location.name} (ID: {default_location.id})")
                else:
                    print(f"📍 [MANUAL] ❌ No default location found for user {current_user.id}")
                    # Check if user has any locations at all
                    all_locations = location_service.get_all_locations()
                    if not all_locations:
                        print(f"📍 [MANUAL] 🏗️ User has no locations, creating default location...")
                        default_locations_created = location_service.setup_default_locations()
                        if default_locations_created:
                            final_locations = [default_locations_created[0].id]
                            print(f"📍 [MANUAL] ✅ Created and assigned default location: {default_locations_created[0].name} (ID: {default_locations_created[0].id})")
                        else:
                            print(f"📍 [MANUAL] ❌ Failed to create default locations")
                    else:
                        print(f"📍 [MANUAL] User has {len(all_locations)} locations but none are default")
                
        except Exception as e:
            print(f"❌ [MANUAL] Error handling location: {e}")
            import traceback
            traceback.print_exc()
        
        print(f"📍 [MANUAL] Final locations list: {final_locations}")
        
        # Convert reading status string to enum
        reading_status_enum = ReadingStatus.PLAN_TO_READ  # Default
        if reading_status:
            try:
                reading_status_enum = ReadingStatus(reading_status)
            except ValueError:
                print(f"⚠️ [MANUAL] Invalid reading status: {reading_status}, using default")
        
        # Convert ownership status string to enum
        ownership_status_enum = None
        if ownership_status:
            try:
                from .domain.models import OwnershipStatus
                ownership_status_enum = OwnershipStatus(ownership_status)
            except ValueError:
                print(f"⚠️ [MANUAL] Invalid ownership status: {ownership_status}")
        
        # Convert media type string to enum
        media_type_enum = None
        if media_type:
            try:
                from .domain.models import MediaType
                media_type_enum = MediaType(media_type)
            except ValueError:
                print(f"⚠️ [MANUAL] Invalid media type: {media_type}")
        
        print(f"📚 [MANUAL] Adding book to user library with reading status: {reading_status_enum}")
        
        # Extract the first location ID for the simplified book service
        location_id = final_locations[0] if final_locations else None
        print(f"📍 [MANUAL] Using location_id: {location_id}")
        
        result = book_service.add_book_to_user_library_sync(
            user_id=current_user.id,
            book_id=existing_book.id,
            reading_status=reading_status_enum,
            location_id=location_id,
            custom_metadata=custom_metadata if custom_metadata else None
        )
        print(f"📚 [MANUAL] Add to library result: {result}")
        
        # Update additional fields if specified
        update_data = {}
        if ownership_status_enum:
            update_data['ownership_status'] = ownership_status_enum
        if media_type_enum:
            update_data['media_type'] = media_type_enum
        if user_rating:
            update_data['user_rating'] = user_rating
        if personal_notes:
            update_data['personal_notes'] = personal_notes
        if user_tags:
            update_data['user_tags'] = user_tags
        if start_date:
            update_data['start_date'] = start_date
        if finish_date:
            update_data['finish_date'] = finish_date
        # Note: location is already handled via the locations parameter above
            
        if update_data:
            print(f"📚 [MANUAL] Updating book with additional data: {update_data}")
            book_service.update_book_sync(existing_book.uid, str(current_user.id), **update_data)
        
        if existing_book.id == domain_book.id:
            # New book was created
            if custom_metadata:
                flash(f'Book "{title}" added successfully with {len(custom_metadata)} custom fields.', 'success')
            else:
                flash(f'Book "{title}" added successfully to your library.', 'success')
        else:
            # Existing book was found
            if custom_metadata:
                flash(f'Book "{title}" already exists. Added to your collection with {len(custom_metadata)} custom fields.', 'info')
            else:
                flash(f'Book "{title}" already exists in the library. Added to your collection.', 'info')
            
    except Exception as e:
        current_app.logger.error(f"Error adding book manually: {e}")
        flash('An error occurred while adding the book. Please try again.', 'danger')

    return redirect(url_for('main.library'))