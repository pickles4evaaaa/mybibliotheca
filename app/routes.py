from flask import Blueprint, current_app, render_template, request, redirect, url_for, jsonify, flash, send_file, abort, make_response, session
from flask_login import login_required, current_user
from .domain.models import Book as DomainBook, Author, Person, BookContribution, ContributionType, Publisher, User, ReadingStatus, OwnershipStatus, CustomFieldDefinition, ImportMappingTemplate
from .services import book_service, user_service, reading_log_service, custom_field_service, import_mapping_service, direct_import_service
from .utils import fetch_book_data, get_reading_streak, get_google_books_cover, generate_month_review_image
from datetime import datetime, date, timedelta
import secrets
import requests
from io import BytesIO
import pytz
import tempfile
import csv
import threading
import uuid
import os
from werkzeug.utils import secure_filename
import csv # Ensure csv is imported
import calendar
import uuid
import tempfile
import json
import threading  # Add threading import
from werkzeug.utils import secure_filename

bp = Blueprint('main', __name__)

# Import Progress Tracking
import_jobs = {}  # In-memory storage for demo - use Redis/database in production

def _convert_published_date_to_date(published_date_str):
    """Convert published_date string to date object using enhanced date parser."""
    if not published_date_str or not isinstance(published_date_str, str):
        return None
    
    try:
        # Use the enhanced date parser from the direct import service
        # This handles all the comprehensive date formats
        return direct_import_service._detect_and_parse_date(published_date_str, "published_date from form")
    except Exception as e:
        current_app.logger.warning(f"Failed to parse published date '{published_date_str}': {e}")
        return None
    
    return None

@bp.route('/log_book', methods=['POST'])
@login_required
def log_book():
    data = request.json
    title = data.get('title')
    author = data.get('author')
    isbn = data.get('isbn')
    
    try:
        # Create a basic book using the service
        contributors = []
        if author:
            person = Person(id=str(uuid.uuid4()), name=author)
            contribution = BookContribution(
                person=person,
                contribution_type=ContributionType.AUTHORED,
                order=0
            )
            contributors.append(contribution)
            
        domain_book = DomainBook(
            id=str(uuid.uuid4()),
            title=title,
            contributors=contributors,
            isbn13=isbn if isbn and len(isbn.replace('-', '')) == 13 else None,
            isbn10=isbn if isbn and len(isbn.replace('-', '')) == 10 else None,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        # Use find_or_create_book to avoid duplicates (global)
        saved_book = book_service.find_or_create_book_sync(domain_book)
        
        # Add book to user's library
        book_service.add_book_to_user_library_sync(
            user_id=current_user.id,
            book_id=saved_book.id,
            reading_status=ReadingStatus.PLAN_TO_READ
        )
        
        return jsonify({
            'message': 'Book logged successfully', 
            'book': {
                'id': saved_book.id,
                'title': saved_book.title,
                'authors': [author.name for author in saved_book.authors] if saved_book.authors else []
            }
        }), 201
    except Exception as e:
        current_app.logger.error(f"Error logging book: {e}")
        return jsonify({'error': 'Failed to log book'}), 500

@bp.route('/health')
def health_check():
    """Health check endpoint for monitoring and testing."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'service': 'bibliotheca'
    }), 200

@bp.route('/reading_history', methods=['GET'])
@login_required
def reading_history():
    # Use service layer instead of direct SQLite query
    domain_books = book_service.get_user_books_sync(str(current_user.id))
    
    # Convert domain books to dict format for API response
    books_data = []
    for user_book in domain_books:
        book_dict = {
            'id': user_book.id,
            'uid': user_book.uid,
            'title': user_book.title,
            'author': user_book.author,
            'isbn': user_book.isbn,
            'description': user_book.description,
            'start_date': getattr(user_book, 'start_date', None).isoformat() if getattr(user_book, 'start_date', None) else None,
            'finish_date': getattr(user_book, 'finish_date', None).isoformat() if getattr(user_book, 'finish_date', None) else None,
            'want_to_read': user_book.want_to_read,
            'library_only': user_book.library_only,
            'cover_url': user_book.cover_url,
            'user_rating': user_book.user_rating,
            'personal_notes': user_book.personal_notes,
            'status': user_book.status,
            'date_added': user_book.date_added.isoformat() if user_book.date_added else None
        }
        books_data.append(book_dict)
    
    return jsonify(books_data), 200

@bp.route('/fetch_book/<isbn>', methods=['GET'])
def fetch_book(isbn):
    book_data = fetch_book_data(isbn) or {}
    google_cover = get_google_books_cover(isbn)
    if google_cover:
        book_data['cover'] = google_cover
    # If neither source provides a cover, set a default
    if not book_data.get('cover'):
        book_data['cover'] = url_for('static', filename='bookshelf.png')
    return jsonify(book_data), 200 if book_data else 404

@bp.route('/')
@login_required
def index():
    """Redirect to library page as the main landing page"""
    return redirect(url_for('main.library'))

@bp.route('/add', methods=['GET', 'POST'])
@login_required
def add_book():
    """Add a new book to the library"""
    if request.method == 'GET':
        return render_template('add_book_new.html')
    
    # Handle POST request for adding book
    # This would handle form submission for manual book entry
    flash('Manual book addition is not implemented yet. Please use the Google Books search from the Library page.', 'info')
    return redirect(url_for('main.library'))

@bp.route('/search', methods=['GET', 'POST'])
@login_required
def search_books():
    """Redirect to library page where search functionality is now integrated"""
    flash('Search functionality has been moved to the Library page for a better experience.', 'info')
    return redirect(url_for('main.library'))

@bp.route('/month_wrapup')
@login_required
def month_wrapup():
    """Redirect to stats page where month wrap-up is now integrated"""
    flash('Month Wrap-up has been moved to the Stats page along with other reading statistics.', 'info')
    return redirect(url_for('main.stats'))

@bp.route('/community_activity')
@login_required
def community_activity():
    """Redirect to stats page where community activity is now integrated"""
    flash('Community activity has been moved to the Stats page along with your personal reading statistics.', 'info')
    return redirect(url_for('main.stats'))

@bp.route('/bulk_import', methods=['GET', 'POST'])
@login_required
def bulk_import():
    """Redirect to new import interface."""
    flash('Book import has been upgraded! You can now map CSV fields and track progress in real-time.', 'info')
    return redirect(url_for('main.import_books'))

@bp.route('/book/<uid>', methods=['GET', 'POST'])
@login_required
def view_book(uid):
    """Redirect to enhanced book view."""
    return redirect(url_for('main.view_book_enhanced', uid=uid))

@bp.route('/book/<uid>/log', methods=['POST'])
@login_required
def log_reading(uid):
    # Get book through service layer
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
    
    log_date_str = request.form.get('log_date')
    log_date = datetime.strptime(log_date_str, '%Y-%m-%d').date() if log_date_str else date.today()
    
    # Check for existing log using service layer
    existing_log = reading_log_service.get_existing_log_sync(user_book.id, current_user.id, log_date)
    if existing_log:
        flash('You have already logged reading for this day.')
    else:
        # Create reading log using service layer
        reading_log_service.create_reading_log_sync(user_book.id, current_user.id, log_date)
        flash('Reading day logged.')
    return redirect(url_for('main.view_book', uid=uid))

@bp.route('/book/<uid>/delete', methods=['POST'])
@login_required
def delete_book(uid):
    # Delete through service layer
    success = book_service.delete_book_sync(uid, str(current_user.id))
    
    if success:
        flash('Book deleted successfully.')
    else:
        flash('Failed to delete book.', 'error')
        
    return redirect(url_for('main.library'))

@bp.route('/book/<uid>/toggle_finished', methods=['POST'])
@login_required
def toggle_finished(uid):
    # Get book through service layer
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
    
    if getattr(user_book, 'finish_date', None):
        # Mark as currently reading
        book_service.update_book_sync(uid, str(current_user.id), finish_date=None)
        flash('Book marked as currently reading.')
    else:
        # Mark as finished
        book_service.update_book_sync(uid, str(current_user.id), finish_date=date.today())
        flash('Book marked as finished.')
    
    return redirect(url_for('main.view_book', uid=uid))

@bp.route('/book/<uid>/start_reading', methods=['POST'])
@login_required
def start_reading(uid):
    # Get book through service layer
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
    
    update_data = {'want_to_read': False}
    if not getattr(user_book, 'start_date', None):
        update_data['start_date'] = datetime.today().date()
    
    book_service.update_book_sync(uid, str(current_user.id), **update_data)
    flash(f'Started reading "{user_book.title}".')
    return redirect(url_for('main.library'))

@bp.route('/book/<uid>/update_status', methods=['POST'])
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
        update_data.update({
            'finish_date': datetime.now().date(),
            'want_to_read': False,
            'library_only': False
        })
    elif currently_reading:
        update_data.update({
            'finish_date': None,
            'want_to_read': False,
            'library_only': False
        })
        if not getattr(user_book, 'start_date', None):
            update_data['start_date'] = datetime.now().date()
    elif want_to_read:
        update_data.update({
            'finish_date': None,
            'library_only': False
        })
    elif library_only:
        update_data.update({
            'finish_date': None,
            'want_to_read': False
        })

    book_service.update_book_sync(uid, str(current_user.id), **update_data)
    flash('Book status updated.')
    return redirect(url_for('main.view_book', uid=uid))

@bp.route('/library')
@login_required
def library():
    # Get filter parameters from URL - default to "all" to show all books
    status_filter = request.args.get('status_filter', 'all')  # Changed from 'reading' to 'all'
    category_filter = request.args.get('category', '')
    publisher_filter = request.args.get('publisher', '')
    language_filter = request.args.get('language', '')
    search_query = request.args.get('search', '')

    # Use Redis service layer to get all user books (with relationship data)
    user_books = book_service.get_user_books_sync(str(current_user.id))
    
    # Calculate statistics for filter buttons
    stats = {
        'total_books': len(user_books),
        'books_read': len([b for b in user_books if getattr(b, 'reading_status', None) == 'read']),
        'currently_reading': len([b for b in user_books if getattr(b, 'reading_status', None) == 'reading']),
        'want_to_read': len([b for b in user_books if getattr(b, 'reading_status', None) == 'plan_to_read']),
        'on_hold': len([b for b in user_books if getattr(b, 'reading_status', None) == 'on_hold']),
        'wishlist': len([b for b in user_books if getattr(b, 'ownership_status', None) == 'wishlist'])
    }
    
    # Apply status filter first
    filtered_books = user_books
    if status_filter and status_filter != 'all':
        if status_filter == 'wishlist':
            filtered_books = [book for book in filtered_books if getattr(book, 'ownership_status', None) == 'wishlist']
        else:
            filtered_books = [book for book in filtered_books if getattr(book, 'reading_status', None) == status_filter]
    
    # Apply other filters
    if search_query:
        search_lower = search_query.lower()
        filtered_books = [
            book for book in filtered_books 
            if (search_lower in book.title.lower() if book.title else False) or
               (search_lower in book.author.lower() if book.author else False) or
               (search_lower in book.description.lower() if book.description else False)
        ]
    
    if publisher_filter:
        filtered_books = [
            book for book in filtered_books 
            if book.publisher and publisher_filter.lower() in book.publisher.lower()
        ]
    
    if language_filter:
        filtered_books = [
            book for book in filtered_books 
            if book.language == language_filter
        ]
    
    if category_filter:
        filtered_books = [
            book for book in filtered_books 
            if book.categories and category_filter.lower() in book.categories.lower()
        ]

    # Books are already in the right format for the template
    books = filtered_books

    # Get distinct values for filter dropdowns (from all books, not filtered)
    all_books = user_books
    
    categories = set()
    publishers = set()
    languages = set()

    for book in all_books:
        if book.categories:
            categories.update([cat.strip() for cat in book.categories.split(',')])
        if book.publisher:
            # Handle Publisher domain object or string
            publisher_name = book.publisher.name if hasattr(book.publisher, 'name') else str(book.publisher)
            publishers.add(publisher_name)
        if book.language:
            languages.add(book.language)

    # Get users through Redis service layer
    domain_users = user_service.get_all_users_sync()
    
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
        current_status_filter=status_filter,
        current_category=category_filter,
        current_publisher=publisher_filter,
        current_language=language_filter,
        current_search=search_query,
        users=users
    )

@bp.route('/public-library')
def public_library():
    filter_status = request.args.get('filter', 'all')
    
    # Use Redis service to get all books from all users
    # TODO: Implement public library functionality in Redis service
    # For now, return empty list
    books = []
    
    return render_template('public_library.html', books=books, filter_status=filter_status)

@bp.route('/book/<uid>/edit', methods=['GET', 'POST'])
@login_required
def edit_book(uid):
    # Get book through service layer
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
        
    if request.method == 'POST':
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
        
        # Create BookContribution objects
        for contrib in contributor_data.values():
            if contrib.get('name'):
                from .domain.models import Person, BookContribution, ContributionType
                
                # Create or get person
                person_id = contrib.get('id')
                if person_id:
                    person = book_service.get_person_by_id_sync(person_id)
                else:
                    # Create new person
                    person = Person(
                        id=str(uuid.uuid4()),
                        name=contrib['name'],
                        created_at=datetime.now(),
                        updated_at=datetime.now()
                    )
                
                # Map contribution type
                contrib_type_map = {
                    'authored': ContributionType.AUTHORED,
                    'narrated': ContributionType.NARRATED,
                    'edited': ContributionType.EDITED,
                    'contributed': ContributionType.CONTRIBUTED
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
            'page_count': int(request.form.get('page_count')) if request.form.get('page_count', '').strip() else None,
            'language': request.form.get('language', '').strip() or 'en',
            'cover_url': request.form.get('cover_url', '').strip() or None,
            'isbn13': new_isbn13,
            'isbn10': new_isbn10,
            'contributors': contributors
        }
        
        # Remove None values except for specific fields
        filtered_data = {}
        for k, v in update_data.items():
            if k in ['contributors'] or v is not None:
                filtered_data[k] = v
        
        success = book_service.update_book_sync(uid, str(current_user.id), **filtered_data)
        if success:
            flash('Book updated successfully.', 'success')
        else:
            flash('Failed to update book.', 'error')
        return redirect(url_for('main.view_book', uid=uid))
        
    return render_template('edit_book_enhanced.html', book=user_book)


@bp.route('/book/<uid>/enhanced')
@login_required
def view_book_enhanced(uid):
    """Enhanced book view with new status system."""
    print(f"🔍 [VIEW] Loading enhanced view for book {uid}, user {current_user.id}")
    
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        print(f"❌ [VIEW] Book {uid} not found for user {current_user.id}")
        abort(404)
    
    print(f"📖 [VIEW] Found book: {user_book.title}")
    print(f"🔍 [VIEW] User book has custom_metadata attribute: {hasattr(user_book, 'custom_metadata')}")
    if hasattr(user_book, 'custom_metadata'):
        print(f"📋 [VIEW] User book custom_metadata content: {user_book.custom_metadata}")
        print(f"📊 [VIEW] User book custom_metadata type: {type(user_book.custom_metadata)}")

    # Get custom metadata for display
    global_metadata_display = []
    personal_metadata_display = []
    
    try:
        print(f"🔍 [VIEW] Checking for global metadata...")
        # Global metadata is stored on the book itself, but we don't have a separate method to get just the book
        # For now, assume no global metadata since we're storing everything on relationships
        # TODO: Implement proper global vs personal metadata separation
        
        print(f"🔍 [VIEW] Checking for personal metadata...")
        if hasattr(user_book, 'custom_metadata') and user_book.custom_metadata:
            print(f"📋 [VIEW] Personal metadata found: {user_book.custom_metadata}")
            personal_metadata_display = custom_field_service.get_custom_metadata_for_display(
                user_book.custom_metadata
            )
        else:
            print(f"⚪ [VIEW] No personal metadata found")
    except Exception as e:
        print(f"❌ [VIEW] Error loading custom metadata for display: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"📊 [VIEW] Returning with {len(global_metadata_display)} global and {len(personal_metadata_display)} personal metadata items")
    
    return render_template(
        'view_book_enhanced.html', 
        book=user_book,
        global_metadata_display=global_metadata_display,
        personal_metadata_display=personal_metadata_display
    )


@bp.route('/book/<uid>/update_details', methods=['POST'])
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
    
    # Location
    primary_location_id = request.form.get('primary_location_id')
    if primary_location_id:
        update_data['primary_location_id'] = primary_location_id
    
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
    if reading_status == 'reading' and not getattr(user_book, 'start_date', None):
        update_data['start_date'] = date.today()
    elif reading_status == 'read' and not getattr(user_book, 'finish_date', None):
        update_data['finish_date'] = date.today()
        if not getattr(user_book, 'start_date', None):
            update_data['start_date'] = date.today()
    
    # Use service layer to update
    try:
        updated_book = book_service.update_book_sync(uid, str(current_user.id), **update_data)
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
    
    return redirect(url_for('main.view_book_enhanced', uid=uid))


@bp.route('/book/<uid>/update_reading_dates', methods=['POST'])
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
    
    return redirect(url_for('main.view_book_enhanced', uid=uid))


@bp.route('/book/<uid>/update_notes', methods=['POST'])
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
            return redirect(url_for('main.view_book_enhanced', uid=uid))
    
    try:
        success = book_service.update_book_sync(uid, str(current_user.id), **update_data)
        if success:
            flash('Notes and rating updated successfully.', 'success')
        else:
            flash('Failed to update notes and rating.', 'error')
    except Exception as e:
        flash(f'Error updating notes: {str(e)}', 'error')
    
    return redirect(url_for('main.view_book_enhanced', uid=uid))


@bp.route('/book/<uid>/custom_metadata', methods=['GET', 'POST'])
@login_required
def edit_book_custom_metadata(uid):
    """Edit custom metadata for a book."""
    try:
        print(f"🔍 [EDIT_META] Starting custom metadata edit for book {uid}, user {current_user.id}")
        
        # Get user book with relationship data (includes custom metadata)
        print(f"🔍 [EDIT_META] Getting user book...")
        user_book = book_service.get_user_book_sync(str(current_user.id), uid)
        if not user_book:
            print(f"❌ [EDIT_META] User book {uid} not found for user {current_user.id}")
            flash('Book not found in your library.', 'error')
            return redirect(url_for('main.library'))
        
        print(f"✅ [EDIT_META] Found user book: {user_book.title}")
        print(f"📊 [EDIT_META] User book custom metadata: {getattr(user_book, 'custom_metadata', 'NO ATTR')}")
        
        if request.method == 'POST':
            print(f"🔍 [EDIT_META] Processing POST request")
            print(f"🔍 [EDIT_META] Form data keys: {list(request.form.keys())}")
            print(f"🔍 [EDIT_META] Full form data: {dict(request.form)}")
            
            # Process form data for custom metadata
            # Note: In current architecture, we're storing everything as personal metadata
            personal_metadata = {}
            
            # Get available fields (treating all as personal for now)
            personal_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=False)
            # Also get global fields but treat them as personal
            global_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=True)
            all_fields = personal_fields + global_fields
            
            print(f"🔍 [EDIT_META] Found {len(personal_fields)} personal fields, {len(global_fields)} global fields")
            
            # Process all fields as personal metadata
            for field in all_fields:
                # Check both global_ and personal_ prefixes for backward compatibility
                personal_key = f'personal_{field.name}'
                global_key = f'global_{field.name}'
                
                personal_value = request.form.get(personal_key, '').strip()
                global_value = request.form.get(global_key, '').strip()
                
                print(f"🔍 [EDIT_META] Field {field.name}: personal_key='{personal_key}' value='{personal_value}', global_key='{global_key}' value='{global_value}'")
                
                value = personal_value or global_value
                if value:
                    personal_metadata[field.name] = value
                    print(f"✅ [EDIT_META] Added to metadata: {field.name} = {value}")
            
            print(f"📝 [EDIT_META] Final processed metadata: {personal_metadata}")
            
            # Validate metadata - TODO: Implement validation
            valid, errors = True, []  # custom_field_service.validate_and_save_metadata(personal_metadata, current_user.id, is_global=False)
            
            if valid:
                print(f"📝 [EDIT_META] Updating personal metadata: {personal_metadata}")
                
                # Update user book relationship with personal metadata
                success = book_service.update_user_book_sync(str(current_user.id), user_book.id, custom_metadata=personal_metadata)
                if success:
                    print(f"✅ [EDIT_META] Updated user book personal metadata")
                    flash('Custom metadata updated successfully!', 'success')
                    return redirect(url_for('main.view_book_enhanced', uid=uid))
                else:
                    print(f"❌ [EDIT_META] Failed to update user book personal metadata")
                    flash('Failed to update custom metadata.', 'error')
            else:
                # Show validation errors
                for error in errors:
                    flash(f'Validation error: {error}', 'error')
        
        # Get display data for template
        # For now, treat all fields as personal since that's how they're stored
        personal_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=False)
        global_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=True)
        
        print(f"🔍 [EDIT_META] Template data preparation:")
        print(f"   📋 [EDIT_META] Personal fields count: {len(personal_fields)}")
        print(f"   📋 [EDIT_META] Global fields count: {len(global_fields)}")
        
        # Get existing custom metadata
        existing_metadata = getattr(user_book, 'custom_metadata', {}) or {}
        print(f"   📊 [EDIT_META] Existing metadata: {existing_metadata}")
        
        # Prepare template data
        global_metadata = {}  # Empty since we're storing everything as personal
        personal_metadata = existing_metadata
        
        print(f"   📤 [EDIT_META] Passing to template:")
        print(f"      🌐 global_fields: {[f.name for f in global_fields]}")
        print(f"      👤 personal_fields: {[f.name for f in personal_fields]}")
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
        return redirect(url_for('main.view_book_enhanced', uid=uid))


@bp.route('/month_review/<int:year>/<int:month>.jpg')
@login_required  
def month_review(year, month):
    # Query books finished in the given month/year by current user using Redis service
    user_books = book_service.get_user_books_sync(str(current_user.id))
    
    # Filter books finished in the specified month/year
    start_date = datetime(year, month, 1).date()
    if month == 12:
        end_date = datetime(year + 1, 1, 1).date()
    else:
        end_date = datetime(year, month + 1, 1).date()
    
    books = [
        book for book in user_books 
        if getattr(book, 'finish_date', None) and start_date <= getattr(book, 'finish_date', None) < end_date
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
            'title': book.title,
            'author': book.author,
            'cover_url': book.cover_url,
            'finish_date': getattr(book, 'finish_date', None)
        })()
        book_objects.append(book_obj)

    img = generate_month_review_image(book_objects, month, year)
    buf = BytesIO()
    img.save(buf, format='JPEG')
    buf.seek(0)
    return send_file(buf, mimetype='image/jpeg', as_attachment=True, download_name=f"month_review_{year}_{month}.jpg")

@bp.route('/add_book_from_search', methods=['POST'])
@login_required
def add_book_from_search():
    title = request.form.get('title')
    author = request.form.get('author')
    isbn = request.form.get('isbn')
    cover_url = request.form.get('cover_url')

    # Normalize ISBN by extracting digits only
    normalized_isbn = None
    if isbn:
        from .services import normalize_isbn_upc
        normalized_isbn = normalize_isbn_upc(isbn)
        if normalized_isbn:
            print(f"📚 [SEARCH] Normalized ISBN: {isbn} -> {normalized_isbn}")
        else:
            print(f"⚠️ [SEARCH] Could not normalize ISBN: {isbn}")

    # Prevent duplicate ISBNs
    if normalized_isbn:
        existing_book = book_service.get_books_by_isbn_sync(normalized_isbn)
        if existing_book:
            flash('A book with this ISBN already exists.', 'danger')
            return redirect(url_for('main.search_books'))

    # Get additional metadata if available
    if normalized_isbn:
        google_data = get_google_books_cover(normalized_isbn, fetch_title_author=True)
        if google_data:
            description = google_data.get('description')
            published_date = google_data.get('published_date')
            page_count = google_data.get('page_count')
            categories = google_data.get('categories')
            publisher = google_data.get('publisher')
            language = google_data.get('language')
            average_rating = google_data.get('average_rating')
            rating_count = google_data.get('rating_count')
        else:
            description = published_date = page_count = categories = publisher = language = average_rating = rating_count = None
    else:
        description = published_date = page_count = categories = publisher = language = average_rating = rating_count = None

    # Create contributors from author data
    contributors = []
    if author:
        person = Person(name=author)
        contribution = BookContribution(
            person=person,
            contribution_type=ContributionType.AUTHORED,
            order=0
        )
        contributors.append(contribution)

    # Create a domain book object
    domain_book = DomainBook(
        title=title,
        isbn13=normalized_isbn if normalized_isbn and len(normalized_isbn) == 13 else None,
        isbn10=normalized_isbn if normalized_isbn and len(normalized_isbn) == 10 else None,
        contributors=contributors,
        cover_url=cover_url,
        description=description,
        published_date=_convert_published_date_to_date(published_date),
        page_count=page_count,
        categories=categories.split(',') if categories else [],
        publisher=Publisher(name=publisher) if publisher else None,
        language=language,
        average_rating=average_rating,
        rating_count=rating_count
    )
    
    # Use the service to create the book
    try:
        created_book = book_service.create_book_sync(domain_book)
        # Add to user's library
        book_service.add_book_to_user_library_sync(
            user_id=current_user.id,
            book_id=created_book.id,
            reading_status=ReadingStatus.PLAN_TO_READ
        )
        flash(f'Added "{title}" to your library.', 'success')
    except Exception as e:
        current_app.logger.error(f"Error creating book: {e}")
        flash('Error adding book to library.', 'danger')
    
    return redirect(url_for('main.library'))


@bp.route('/download_db', methods=['GET'])
@login_required
def download_db():
    """Export user data from Redis to CSV format."""
    try:
        # Get all user books from Redis
        user_books = book_service.get_user_books_sync(current_user.id)
        
        # Create CSV export
        import io
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow(['Title', 'Author', 'ISBN', 'Reading Status', 'Start Date', 'Finish Date', 'Rating', 'Notes'])
        
        # Write book data
        for book in user_books:
            author_names = ', '.join([author.name for author in book.authors]) if book.authors else ''
            isbn = book.isbn13 or book.isbn10 or ''
            start_date = book.start_date.isoformat() if hasattr(book, 'start_date') and book.start_date else ''
            finish_date = book.finish_date.isoformat() if hasattr(book, 'finish_date') and book.finish_date else ''
            rating = getattr(book, 'user_rating', '') or ''
            notes = getattr(book, 'personal_notes', '') or ''
            reading_status = getattr(book, 'reading_status', '') or ''
            
            writer.writerow([book.title, author_names, isbn, reading_status, start_date, finish_date, rating, notes])
        
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

@bp.route('/community_activity/active_readers')
@login_required
def community_active_readers():
    """Redirect to stats page where community activity is now integrated"""
    flash('Community activity has been moved to the Stats page.', 'info')
    return redirect(url_for('main.stats'))

@bp.route('/community_activity/books_this_month')
@login_required
def community_books_this_month():
    """Redirect to stats page where community activity is now integrated"""
    flash('Community activity has been moved to the Stats page.', 'info')
    return redirect(url_for('main.stats'))

@bp.route('/community_activity/currently_reading')
@login_required
def community_currently_reading():
    """Redirect to stats page where community activity is now integrated"""
    flash('Community activity has been moved to the Stats page.', 'info')
    return redirect(url_for('main.stats'))

@bp.route('/community_activity/recent_activity')
@login_required
def community_recent_activity():
    """Redirect to stats page where community activity is now integrated"""
    flash('Community activity has been moved to the Stats page.', 'info')
    return redirect(url_for('main.stats'))

@bp.route('/user/<string:user_id>/profile')
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
        
        # Get user's books from Redis
        all_user_books = book_service.get_user_books_sync(user_id)
        
        # Calculate statistics from the book list
        current_year = datetime.now().year
        current_month = datetime.now().date().replace(day=1)
        
        total_books = len([book for book in all_user_books if getattr(book, 'finish_date', None)])
        
        books_this_year = len([book for book in all_user_books 
                              if getattr(book, 'finish_date', None) and 
                              getattr(book, 'finish_date', None) >= date(current_year, 1, 1)])
        
        books_this_month = len([book for book in all_user_books 
                               if getattr(book, 'finish_date', None) and 
                               getattr(book, 'finish_date', None) >= current_month])
        
        currently_reading = [book for book in all_user_books 
                           if getattr(book, 'start_date', None) and not getattr(book, 'finish_date', None)] if user.share_current_reading else []
        
        recent_finished = sorted([book for book in all_user_books if getattr(book, 'finish_date', None)],
                                key=lambda x: getattr(x, 'finish_date', date.min), reverse=True)[:10]
        
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

@bp.route('/book/<uid>/assign', methods=['POST'])
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
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            flash('Invalid user selected.', 'danger')
            return redirect(url_for('main.library'))

        # For Redis, this would involve transferring the book to another user
        # This is a complex operation that would need to be implemented in the service layer
        flash('Book assignment feature needs to be implemented for Redis backend.', 'warning')
        return redirect(url_for('main.library'))
    except Exception as e:
        current_app.logger.error(f"Error assigning book: {e}")
        flash('Error assigning book.', 'danger')
        return redirect(url_for('main.library'))

@bp.route('/books/bulk_delete', methods=['POST'])
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
            selected_uids = [single_value]
    
    print(f"Selected UIDs: {selected_uids}")
    
    if not selected_uids:
        print("No books selected for deletion")
        flash('No books selected for deletion.', 'warning')
        return redirect(url_for('main.library'))
    
    deleted_count = 0
    failed_count = 0
    
    for uid in selected_uids:
        try:
            print(f"Attempting to delete book {uid}")
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
        flash(f'Successfully deleted {deleted_count} book(s).', 'success')
    if failed_count > 0:
        flash(f'Failed to delete {failed_count} book(s).', 'error')
    
    return redirect(url_for('main.library'))

@bp.route('/csrf-guide')
def csrf_guide():
    """Demo page showing CSRF protection implementation."""
    return render_template('csrf_guide.html')

@bp.route('/stats')
@login_required
def stats():
    """Combined user and community statistics page"""
    # Get current date for time-based calculations
    timezone = pytz.timezone(current_app.config.get('TIMEZONE', 'UTC'))
    now = datetime.now(timezone)
    today = now.date()
    
    # Calculate date ranges
    week_start = today - timedelta(days=today.weekday())
    month_start = today.replace(day=1)
    year_start = today.replace(month=1, day=1)
    
    # Get user books for stats calculations
    user_books = book_service.get_user_books_sync(str(current_user.id))
    
    # Calculate user stats - use getattr for safe attribute access
    books_finished_this_week = len([b for b in user_books if getattr(b, 'finish_date', None) and getattr(b, 'finish_date', None) >= week_start])
    books_finished_this_month = len([b for b in user_books if getattr(b, 'finish_date', None) and getattr(b, 'finish_date', None) >= month_start])
    books_finished_this_year = len([b for b in user_books if getattr(b, 'finish_date', None) and getattr(b, 'finish_date', None) >= year_start])
    books_finished_total = len([b for b in user_books if getattr(b, 'finish_date', None)])
    
    currently_reading = [b for b in user_books if not getattr(b, 'finish_date', None) and not getattr(b, 'want_to_read', False) and not getattr(b, 'library_only', False)]
    want_to_read = [b for b in user_books if getattr(b, 'want_to_read', False)]
    
    # Get reading streak for current user
    streak = current_user.get_reading_streak()
    
    # Community stats (only if users share their activity)
    try:
        # Import the redis book service for community features
        from .redis_services import redis_book_service
        
        # Get sharing users count
        sharing_users = user_service.get_sharing_users_sync() if hasattr(user_service, 'get_sharing_users_sync') else []
        
        # Recent community activity (books finished in last 30 days)
        recent_finished_books = redis_book_service.get_books_with_sharing_users_sync(days_back=30, limit=20)
        
        # Community currently reading
        community_currently_reading = redis_book_service.get_currently_reading_shared_sync(limit=20)
        
        # Community stats summary - count finished books this month
        month_finished_books = redis_book_service.get_books_with_sharing_users_sync(days_back=30, limit=1000)
        month_start = datetime.now().date().replace(day=1)
        total_books_this_month = len([book for book in month_finished_books 
                                    if getattr(book, 'finish_date', None) and getattr(book, 'finish_date', None) >= month_start])
        
        total_active_readers = len(sharing_users)
        
        # Recent reading logs (from last 7 days) - use service
        recent_logs = reading_log_service.get_recent_shared_logs_sync(days_back=7, limit=50) if hasattr(reading_log_service, 'get_recent_shared_logs_sync') else []
        
    except Exception as e:
        current_app.logger.error(f"Error loading community stats: {e}")
        # Fallback to empty data
        recent_finished_books = []
        community_currently_reading = []
        total_books_this_month = 0
        total_active_readers = 0
        recent_logs = []
    
    return render_template('stats.html',
                         # User stats
                         books_finished_this_week=books_finished_this_week,
                         books_finished_this_month=books_finished_this_month,
                         books_finished_this_year=books_finished_this_year,
                         books_finished_total=books_finished_total,
                         currently_reading=currently_reading,
                         want_to_read=want_to_read,
                         streak=streak,
                         # Community stats
                         recent_finished_books=recent_finished_books,
                         community_currently_reading=community_currently_reading,
                         total_books_this_month=total_books_this_month,
                         total_active_readers=total_active_readers,
                         recent_logs=recent_logs,
                         sharing_users=sharing_users)

@bp.route('/search_books_in_library', methods=['POST'])
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
    search_query = request.args.get('search', '')

    # Use Redis service layer to get all user books (with relationship data)
    user_books = book_service.get_user_books_sync(str(current_user.id))
    
    # Apply filters in Python (Redis doesn't have complex querying like SQL)
    filtered_books = user_books
    
    if search_query:
        search_lower = search_query.lower()
        filtered_books = [
            book for book in filtered_books 
            if (search_lower in book.title.lower() if book.title else False) or
               (search_lower in book.author.lower() if book.author else False) or
               (search_lower in book.description.lower() if book.description else False)
        ]
    if publisher_filter:
        filtered_books = [
            book for book in filtered_books 
            if book.publisher and publisher_filter.lower() in book.publisher.lower()
        ]
    if language_filter:
        filtered_books = [
            book for book in filtered_books 
            if book.language == language_filter
        ]
    if category_filter:
        filtered_books = [
            book for book in filtered_books 
            if book.categories and category_filter.lower() in book.categories.lower()
        ]

    # Books are already in the right format for the template
    books = filtered_books

    # Get distinct values for filter dropdowns
    all_books = user_books  # Use same data
    
    categories = set()
    publishers = set()
    languages = set()

    for book in all_books:
        if book.categories:
            categories.update([cat.strip() for cat in book.categories.split(',')])
        if book.publisher:
            # Handle Publisher domain object or string
            publisher_name = book.publisher.name if hasattr(book.publisher, 'name') else str(book.publisher)
            publishers.add(publisher_name)
        if book.language:
            languages.add(book.language)

    # Get users through Redis service layer
    domain_users = user_service.get_all_users_sync()
    
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
        'library.html',
        books=books,
        categories=sorted(categories),
        publishers=sorted(publishers),
        languages=sorted(languages),
        current_category=category_filter,
        current_publisher=publisher_filter,
        current_language=language_filter,
        current_search=search_query,
        users=users,
        search_results=results,
        search_query=query
    )

@bp.route('/add_book_manual', methods=['POST'])
@login_required
def add_book_manual():
    """Add a book manually from the library page"""
    # Validate required fields
    title = request.form['title'].strip()
    if not title:
        flash('Error: Title is required to add a book.', 'danger')
        return redirect(url_for('main.library'))

    isbn = request.form.get('isbn', '').strip()
    author = request.form.get('author', '').strip()
    cover_url = request.form.get('cover_url', '').strip()
    
    # Normalize ISBN by extracting digits only
    normalized_isbn = None
    if isbn:
        from .services import normalize_isbn_upc
        normalized_isbn = normalize_isbn_upc(isbn)
        if normalized_isbn:
            print(f"📚 [MANUAL] Normalized ISBN: {isbn} -> {normalized_isbn}")
        else:
            print(f"⚠️ [MANUAL] Could not normalize ISBN: {isbn}")
    
    start_date_str = request.form.get('start_date') or None
    finish_date_str = request.form.get('finish_date') or None
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
    finish_date = datetime.strptime(finish_date_str, '%Y-%m-%d').date() if finish_date_str else None
    want_to_read = 'want_to_read' in request.form
    library_only = 'library_only' in request.form

    # Process custom metadata
    custom_metadata = {}
    field_names = request.form.getlist('custom_field_name[]')
    field_values = request.form.getlist('custom_field_value[]')
    
    print(f"🔍 [MANUAL] Processing custom metadata: {len(field_names)} fields")
    
    for name, value in zip(field_names, field_values):
        if name and name.strip() and value and value.strip():
            # Create a simple field definition for this custom field
            field_name = name.strip()
            field_value = value.strip()
            
            print(f"📋 [MANUAL] Adding custom field: {field_name} = {field_value}")
            
            # For manual entry, we'll create temporary field definitions
            # In a full implementation, you'd want to create proper CustomFieldDefinition objects
            try:
                # Create or find custom field definition
                from .domain.models import CustomFieldDefinition, CustomFieldType
                field_def = CustomFieldDefinition(
                    id=str(uuid.uuid4()),
                    name=field_name.lower().replace(' ', '_'),
                    display_name=field_name,
                    field_type=CustomFieldType.TEXT,
                    description=f"Manual entry field for {field_name}",
                    created_by_user_id=str(current_user.id),
                    is_global=False,
                    is_shareable=False,
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                
                # Create the field definition
                saved_field = custom_field_service.create_field_sync(field_def)
                # Store metadata using the field name, not the ID
                custom_metadata[saved_field.name] = field_value
                
                print(f"✅ [MANUAL] Created custom field {field_name} with ID {saved_field.id}, stored as {saved_field.name} = {field_value}")
                
            except Exception as e:
                print(f"❌ [MANUAL] Error creating custom field {field_name}: {e}")
                # Fallback: store with a simple key (not recommended for production)
                custom_metadata[f"manual_{field_name.lower().replace(' ', '_')}"] = field_value

    print(f"📊 [MANUAL] Final custom metadata: {custom_metadata}")

    # If no cover URL provided, try to fetch one
    if not cover_url and normalized_isbn:
        cover_url = get_google_books_cover(normalized_isbn)

    # Get additional metadata if ISBN is provided
    description = published_date = page_count = categories = publisher = language = average_rating = rating_count = None
    
    if normalized_isbn:
        google_data = get_google_books_cover(normalized_isbn, fetch_title_author=True)
        if google_data:
            description = google_data.get('description')
            published_date = google_data.get('published_date')
            page_count = google_data.get('page_count')
            categories = google_data.get('categories')
            publisher = google_data.get('publisher')
            language = google_data.get('language')
            average_rating = google_data.get('average_rating')
            rating_count = google_data.get('rating_count')
        else:
            # Fallback to OpenLibrary data
            ol_data = fetch_book_data(normalized_isbn)
            if ol_data:
                description = ol_data.get('description')
                published_date = ol_data.get('published_date')
                page_count = ol_data.get('page_count')
                categories = ol_data.get('categories')
                publisher = ol_data.get('publisher')
                language = ol_data.get('language')

    try:
        # Create contributors from author data
        contributors = []
        if author:
            person = Person(id=str(uuid.uuid4()), name=author)
            contribution = BookContribution(
                person=person,
                contribution_type=ContributionType.AUTHORED,
                order=0
            )
            contributors.append(contribution)
            
        # Create domain book object
        domain_book = DomainBook(
            id=str(uuid.uuid4()),
            title=title,
            description=description,
            published_date=_convert_published_date_to_date(published_date),
            page_count=page_count,
            language=language or "en",
            cover_url=cover_url,
            isbn13=normalized_isbn if normalized_isbn and len(normalized_isbn) == 13 else None,
            isbn10=normalized_isbn if normalized_isbn and len(normalized_isbn) == 10 else None,
            average_rating=average_rating,
            rating_count=rating_count,
            contributors=contributors,
            publisher=Publisher(id=str(uuid.uuid4()), name=publisher) if publisher else None,
            categories=[],  # TODO: Parse categories
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        # Use find_or_create_book to avoid duplicates (global)
        existing_book = book_service.find_or_create_book_sync(domain_book)
        
        # Add to user's library with custom metadata
        book_service.add_book_to_user_library_sync(
            user_id=current_user.id,
            book_id=existing_book.id,
            reading_status=ReadingStatus.PLAN_TO_READ,
            custom_metadata=custom_metadata if custom_metadata else None
        )
        
        # Update status if specified
        if want_to_read or library_only or start_date or finish_date:
            update_data = {
                'want_to_read': want_to_read,
                'library_only': library_only
            }
            if start_date:
                update_data['start_date'] = start_date
            if finish_date:
                update_data['finish_date'] = finish_date
                
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

@bp.route('/import-books', methods=['GET', 'POST'])
@login_required
def import_books():
    """New unified import interface."""
    if request.method == 'POST':
        # Check if user wants to force custom mapping (bypass template detection)
        force_custom = request.form.get('force_custom', 'false').lower() == 'true'
        
        # Check if we're coming from confirmation screen with existing file
        existing_csv_path = request.form.get('csv_file_path')
        
        if existing_csv_path and force_custom:
            # Reuse existing CSV file for custom mapping
            temp_path = existing_csv_path
        else:
            # Handle new file upload and show field mapping
            file = request.files.get('csv_file')
            if not file or not file.filename.endswith('.csv'):
                flash('Please upload a valid CSV file.', 'danger')
                return redirect(url_for('main.import_books'))
            
            # Save file temporarily
            filename = secure_filename(file.filename)
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv', prefix=f'import_{current_user.id}_')
            file.save(temp_file.name)
            temp_path = temp_file.name
        try:
            # Read CSV headers and first few rows for preview
            with open(temp_path, 'r', encoding='utf-8') as csvfile:
                # Try to detect delimiter with improved logic
                sample = csvfile.read(1024)
                csvfile.seek(0)
                
                delimiter = ','  # Default to comma
                try:
                    sniffer = csv.Sniffer()
                    delimiter = sniffer.sniff(sample).delimiter
                except Exception:
                    # If sniffer fails, try common delimiters
                    for test_delimiter in [',', ';', '\t', '|']:
                        if test_delimiter in sample:
                            delimiter = test_delimiter
                            break
                
                reader = csv.DictReader(csvfile, delimiter=delimiter)
                headers = reader.fieldnames or []
                
                # Get first 3 rows for preview
                preview_rows = []
                total_rows = 0
                for i, row in enumerate(reader):
                    if i < 3:
                        preview_rows.append([row.get(header, '') for header in headers])
                    total_rows += 1
                
                # If no headers detected, treat first column as ISBN list
                if not headers or len(headers) == 1:
                    csvfile.seek(0)
                    first_line = csvfile.readline().strip()
                    if first_line.isdigit() or len(first_line) in [10, 13]:  # Looks like ISBN
                        headers = ['ISBN']
                        csvfile.seek(0)
                        all_lines = csvfile.readlines()
                        preview_rows = [[line.strip()] for line in all_lines[:3]]
                        total_rows = len(all_lines)
            
            # Auto-detect field mappings
            suggested_mappings = auto_detect_fields(headers, current_user.id)
            
            # Check if this is a Goodreads or StoryGraph file for direct import
            if not force_custom:
                goodreads_signatures = ['Book Id', 'Author l-f', 'Bookshelves', 'Exclusive Shelf']
                storygraph_signatures = ['Read Status', 'Moods', 'Pace', 'Character- or Plot-Driven?']
                
                is_goodreads = any(header in headers for header in goodreads_signatures)
                is_storygraph = any(header in headers for header in storygraph_signatures)
                
                if is_goodreads or is_storygraph:
                    # Store the temp file path in the session for the direct import
                    session['direct_import_file'] = temp_path
                    if existing_csv_path:
                        session['direct_import_filename'] = 'existing_import_file.csv'
                    else:
                        session['direct_import_filename'] = file.filename
                    import_type = 'goodreads' if is_goodreads else 'storygraph'
                    
                    # Flash a message suggesting direct import
                    flash(f'This looks like a {import_type.title()} export file! For faster import, use our streamlined direct import process.', 'info')
                    
                    # Redirect to direct import with suggestion
                    return redirect(url_for('main.direct_import', suggested=True, import_type=import_type))
            
            # Get custom fields for the user
            try:
                global_custom_fields = custom_field_service.get_user_fields_sync(current_user.id)
                personal_custom_fields = []  # For Redis version, all are user fields
            except Exception as e:
                current_app.logger.error(f"Error loading custom fields: {e}")
                global_custom_fields = []
                personal_custom_fields = []
            
            # Get import templates for the user
            try:
                import_templates = import_mapping_service.get_user_templates_sync(current_user.id)
                
                print(f"DEBUG: CSV headers: {headers}")
                print(f"DEBUG: Force custom mapping: {force_custom}")
                print(f"DEBUG: Available templates: {[t.name for t in import_templates]}")
                
                # Skip template detection if user wants custom mapping
                if force_custom:
                    detected_template = None
                    detected_template_name = None
                    print(f"DEBUG: Skipping template detection - user requested custom mapping")
                else:
                    # Detect best matching template based on headers
                    detected_template = import_mapping_service.detect_template_sync(headers, current_user.id)
                    detected_template_name = detected_template.name if detected_template else None
                    
                    print(f"DEBUG: Template detection - detected_template: {detected_template}")
                    print(f"DEBUG: Template detection - detected_template_name: {detected_template_name}")
                    if detected_template:
                        print(f"DEBUG: Template detection - template.field_mappings: {detected_template.field_mappings}")
                        print(f"DEBUG: Template detection - field_mappings type: {type(detected_template.field_mappings)}")
                        print(f"DEBUG: Template detection - field_mappings bool: {bool(detected_template.field_mappings)}")
                
                # If a default system template was detected, skip mapping UI and go straight to confirmation
                if not force_custom and detected_template and detected_template.user_id == "__system__" and detected_template.field_mappings:
                    print(f"DEBUG: System template detected - {detected_template.name}")
                    print(f"DEBUG: Using template mappings directly: {detected_template.field_mappings}")
                    
                    # Auto-create any custom fields referenced in the template
                    auto_create_custom_fields(detected_template.field_mappings, current_user.id)
                    
                    # Render confirmation screen instead of mapping UI
                    return render_template('import_books_confirmation.html',
                                         csv_file_path=temp_path,
                                         csv_headers=headers,
                                         csv_preview=preview_rows,
                                         total_rows=total_rows,
                                         detected_template=detected_template,
                                         template_mappings=detected_template.field_mappings)
                
                # If a custom template was detected, use its mappings
                elif detected_template and detected_template.field_mappings:
                    print(f"DEBUG: Custom template detected - Using template mappings")
                    suggested_mappings = detected_template.field_mappings.copy()
                    print(f"DEBUG: Using template mappings: {suggested_mappings}")
                    # Auto-create any custom fields referenced in the template
                    auto_create_custom_fields(suggested_mappings, current_user.id)
                    # Reload custom fields after creating new ones
                    global_custom_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=True)
                    personal_custom_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=False)
                else:
                    print(f"DEBUG: No template detected, using auto-detected mappings: {suggested_mappings}")
                
            except Exception as e:
                current_app.logger.error(f"Error loading import templates: {e}")
                import_templates = []
                detected_template = None
                detected_template_name = None
            
            return render_template('import_books_mapping.html',
                                 csv_file_path=temp_path,
                                 csv_headers=headers,
                                 csv_preview=preview_rows,
                                 total_rows=total_rows,
                                 suggested_mappings=suggested_mappings,
                                 global_custom_fields=global_custom_fields,
                                 personal_custom_fields=personal_custom_fields,
                                 import_templates=import_templates,
                                 detected_template=detected_template,
                                 detected_template_name=detected_template_name)
                                 
        except Exception as e:
            current_app.logger.error(f"Error processing CSV file: {e}")
            flash('Error processing CSV file. Please check the format and try again.', 'danger')
            return redirect(url_for('main.import_books'))
    
    return render_template('import_books.html')

@bp.route('/import-books/execute', methods=['POST'])
@login_required
def import_books_execute():
    """Execute the import with user-defined field mappings or template mappings."""
    csv_file_path = request.form.get('csv_file_path')
    use_template = request.form.get('use_template')
    skip_mapping = request.form.get('skip_mapping', 'false').lower() == 'true'
    
    # Initialize template saving variables
    save_as_template = False
    template_name = ''
    
    # If using a template directly (from confirmation screen)
    if use_template and skip_mapping:
        print(f"DEBUG: Executing import with template: {use_template}")
        
        # Get the template to use its mappings
        try:
            template = import_mapping_service.get_template_by_id_sync(use_template)
            if not template:
                flash('Template not found', 'error')
                return redirect(url_for('main.import_books'))
            
            print(f"DEBUG: Template mappings: {template.field_mappings}")
            
            # Convert template mappings to the format expected by import
            mappings = {}
            for csv_field, mapping_info in template.field_mappings.items():
                if isinstance(mapping_info, dict):
                    target_field = mapping_info.get('target_field')
                else:
                    target_field = mapping_info  # Fallback for string format
                
                if target_field:
                    mappings[csv_field] = target_field
            
            print(f"DEBUG: Converted mappings for import: {mappings}")
            
        except Exception as e:
            current_app.logger.error(f"Error loading template for import: {e}")
            flash('Error loading template configuration', 'error')
            return redirect(url_for('main.import_books'))
    
    else:
        # Handle manual mapping (from mapping UI)
        field_mapping = request.form.to_dict()
        
        # Extract mapping from form data
        mappings = {}
        new_fields_to_create = {}
        
        for key, value in field_mapping.items():
            if key.startswith('field_mapping[') and value:
                csv_field = key[14:-1]  # Remove 'field_mapping[' and ']'
                
                # Handle new field creation
                if value in ['create_global_field', 'create_personal_field']:
                    label_key = f'new_field_label[{csv_field}]'
                    type_key = f'new_field_type[{csv_field}]'
                    
                    field_label = request.form.get(label_key, '').strip()
                    field_type = request.form.get(type_key, 'text')
                    
                    if not field_label:
                        flash(f'Field label is required for CSV column "{csv_field}"', 'error')
                        return redirect(url_for('main.import_books'))
                    
                    # Create the custom field
                    try:
                        is_global = (value == 'create_global_field')
                        field_name = field_label.lower().replace(' ', '_').replace('-', '_')
                        
                        # Check if field already exists
                        existing_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global)
                        if any(f.name == field_name for f in existing_fields):
                            flash(f'A custom field with name "{field_name}" already exists', 'error')
                            return redirect(url_for('main.import_books'))
                        
                        field_definition = CustomFieldDefinition(
                            name=field_name,
                            display_name=field_label,
                            field_type=field_type,
                            is_global=is_global,
                            created_by_user_id=current_user.id,
                            created_at=datetime.utcnow(),
                            description=f'Created during CSV import for column "{csv_field}"'
                        )
                        
                        custom_field_service.create_field(field_definition)
                        
                        # Update mapping to use the new field
                        mappings[csv_field] = f'custom_{"global" if is_global else "personal"}_{field_name}'
                        
                        flash(f'Created new {"global" if is_global else "personal"} field: {field_label}', 'success')
                        
                    except Exception as e:
                        flash(f'Error creating custom field: {str(e)}', 'error')
                        return redirect(url_for('main.import_books'))
                else:
                    mappings[csv_field] = value
        
        # Handle template saving (only for manual mapping)
        save_as_template = request.form.get('save_as_template') == 'on'
        template_name = request.form.get('template_name', '').strip()
    
    if save_as_template and template_name:
        try:
            template = ImportMappingTemplate(
                name=template_name,
                user_id=current_user.id,
                field_mappings=mappings,
                description=f'Template created from import on {datetime.now().strftime("%Y-%m-%d")}',
                created_at=datetime.utcnow()
            )
            
            import_mapping_service.create_template(template)
            flash(f'Import template "{template_name}" saved successfully!', 'success')
            
        except Exception as e:
            flash(f'Error saving template: {str(e)}', 'warning')
            # Continue with import even if template saving fails
    
    default_reading_status = request.form.get('default_reading_status', 'library_only')
    duplicate_handling = request.form.get('duplicate_handling', 'skip')
    
    # Create import job with enhanced data
    task_id = str(uuid.uuid4())
    job_data = {
        'task_id': task_id,
        'user_id': current_user.id,
        'csv_file_path': csv_file_path,
        'field_mappings': mappings,
        'default_reading_status': default_reading_status,
        'duplicate_handling': duplicate_handling,
        'custom_fields_enabled': True,  # Flag to enable custom metadata processing
        'status': 'pending',
        'processed': 0,
        'success': 0,
        'errors': 0,
        'total': 0,
        'start_time': datetime.utcnow().isoformat(),
        'current_book': None,
        'error_messages': [],
        'recent_activity': []
    }
    
    # Count total rows
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            job_data['total'] = sum(1 for _ in reader)
    except:
        job_data['total'] = 0
    
    # Store job data in Redis instead of memory
    print(f"🏗️ [EXECUTE] Creating job {task_id} for user {current_user.id}")
    redis_success = store_job_in_redis(task_id, job_data)
    
    # Also keep in memory for backward compatibility
    import_jobs[task_id] = job_data
    
    print(f"📊 [EXECUTE] Redis storage: {'✅' if redis_success else '❌'}")
    print(f"💾 [EXECUTE] Memory storage: ✅")
    print(f"🔧 [EXECUTE] Job status: {job_data['status']}")
    print(f"📈 [EXECUTE] Total rows to process: {job_data['total']}")
    
    # Start the import in background thread
    app = current_app._get_current_object()  # Get the actual app instance
    def run_import():
        with app.app_context():
            try:
                start_import_job(task_id)
            except Exception as e:
                # Update job with error
                if task_id in import_jobs:
                    import_jobs[task_id]['status'] = 'failed'
                    import_jobs[task_id]['error_message'] = str(e)
                    app.logger.error(f"Import job {task_id} failed: {e}")
    
    # Start the import process in background
    thread = threading.Thread(target=run_import)
    thread.daemon = True
    thread.start()
    
    return redirect(url_for('main.import_books_progress', task_id=task_id))

@bp.route('/import-books/progress/<task_id>')
@login_required
def import_books_progress(task_id):
    """Show import progress page."""
    print(f"🔍 [PROGRESS] Looking for job {task_id}")
    
    # Try Redis first, then fall back to memory
    redis_job = get_job_from_redis(task_id)
    memory_job = import_jobs.get(task_id)
    
    print(f"📊 [PROGRESS] Redis job found: {bool(redis_job)}")
    print(f"💾 [PROGRESS] Memory job found: {bool(memory_job)}")
    
    job = redis_job or memory_job
    
    if not job:
        print(f"❌ [PROGRESS] No job found for {task_id}")
        print(f"📝 [PROGRESS] Available jobs in memory: {list(import_jobs.keys())}")
        flash('Import job not found. This may be due to a server restart. Please start a new import.', 'warning')
        return redirect(url_for('main.import_books'))
    
    if job['user_id'] != current_user.id:
        print(f"🚫 [PROGRESS] Job belongs to user {job['user_id']}, current user is {current_user.id}")
        flash('Access denied to this import job.', 'danger')
        return redirect(url_for('main.import_books'))
    
    print(f"✅ [PROGRESS] Job found with status: {job.get('status', 'unknown')}")
    
    return render_template('import_books_progress.html',
                         task_id=task_id,
                         total_books=job['total'],
                         start_time=job['start_time'])

@bp.route('/api/import/progress/<task_id>')
@login_required
def api_import_progress(task_id):
    """API endpoint for import progress."""
    print(f"Progress API called for task_id: {task_id} by user: {current_user.id}")
    
    # Try Redis first, then fall back to memory
    job = get_job_from_redis(task_id) or import_jobs.get(task_id)
    print(f"Redis job found: {bool(get_job_from_redis(task_id))}")
    print(f"Memory job found: {bool(import_jobs.get(task_id))}")
    print(f"Job data: {job}")
    
    if not job:
        print(f"No job found for task_id: {task_id}")
        return jsonify({'error': 'Job not found'}), 404
        
    if job['user_id'] != current_user.id:
        print(f"Job user_id {job['user_id']} doesn't match current user {current_user.id}")
        return jsonify({'error': 'Job not found'}), 404
    
    return jsonify({
        'status': job['status'],
        'processed': job['processed'],
        'success': job['success'],
        'errors': job['errors'],
        'total': job['total'],
        'current_book': job['current_book'],
        'recent_activity': job['recent_activity'][-10:],  # Last 10 activities
        'error': job.get('error_message')
    })

@bp.route('/api/import/errors/<task_id>')
@login_required
def api_import_errors(task_id):
    """Download error report for import job."""
    job = import_jobs.get(task_id)
    if not job or job['user_id'] != current_user.id:
        return jsonify({'error': 'Job not found'}), 404
    
    if not job['error_messages']:
        return jsonify({'error': 'No errors to download'}), 404
    
    # Create CSV of errors
    output = []
    output.append('Row,Error,Details')
    for error in job['error_messages']:
        output.append(f"{error['row']},{error['error']},{error['details']}")
    
    response = make_response('\n'.join(output))
    response.headers['Content-Type'] = 'text/csv'
    response.headers['Content-Disposition'] = f'attachment; filename=import_errors_{task_id}.csv'
    return response

@bp.route('/debug/import-jobs')
@login_required
def debug_import_jobs():
    """Debug endpoint to check import jobs (remove in production)."""
    if not current_user.is_admin:
        abort(403)
    
    return jsonify({
        'total_jobs': len(import_jobs),
        'jobs': {
            job_id: {
                'status': job['status'],
                'user_id': job['user_id'], 
                'processed': job['processed'],
                'total': job['total'],
                'start_time': job['start_time']
            }
            for job_id, job in import_jobs.items()
        }
    })

def auto_detect_fields(headers, user_id=None):
    """Auto-detect field mappings based on header names."""
    mappings = {}
    
    # Get custom fields for auto-detection if user_id is provided
    custom_fields = []
    if user_id:
        try:
            # Use sync wrapper methods to avoid coroutine issues
            personal_fields = custom_field_service.get_available_fields_sync(user_id, is_global=False)
            global_fields = custom_field_service.get_available_fields_sync(user_id, is_global=True)
            
            # Combine and format for use in detection logic
            custom_fields.extend([(f, False) for f in personal_fields])
            custom_fields.extend([(f, True) for f in global_fields])
            
        except Exception as e:
            current_app.logger.error(f"Error loading custom fields for auto-detection: {e}")
    
    for header in headers:
        header_lower = header.lower().strip()
        
        # Check for custom field matches first (exact and partial matches)
        custom_match_found = False
        for field, is_global in custom_fields:
            field_name_lower = field.name.lower()
            field_display_name_lower = field.display_name.lower()
            
            # Exact match on name or display_name
            if header_lower == field_name_lower or header_lower == field_display_name_lower:
                mappings[header] = f'custom_{"global" if is_global else "personal"}_{field.name}'
                custom_match_found = True
                break
            
            # Partial match on display_name
            elif any(word in header_lower for word in field_display_name_lower.split()) and len(field_display_name_lower.split()) > 1:
                mappings[header] = f'custom_{"global" if is_global else "personal"}_{field.name}'
                custom_match_found = True
                break
        
        if custom_match_found:
            continue
        
        # Standard field detection if no custom field match
        # Title mappings
        if any(word in header_lower for word in ['title', 'book', 'name']):
            mappings[header] = 'title'
        
        # Author mappings - prefer primary author field
        elif header_lower in ['author', 'authors']:
            mappings[header] = 'author'
        elif header_lower in ['additional authors']:
            mappings[header] = 'additional_authors'
        elif any(word in header_lower for word in ['author', 'creator', 'writer']) and 'additional' not in header_lower:
            mappings[header] = 'author'
        
        # ISBN mappings - prefer ISBN13, handle both regular and Goodreads format
        elif header_lower in ['isbn13', 'isbn/uid']:
            mappings[header] = 'isbn'
        elif header_lower in ['isbn', 'isbn10'] and 'ISBN13' not in mappings.values():
            mappings[header] = 'isbn'
        elif any(word in header_lower for word in ['upc', 'barcode']):
            mappings[header] = 'isbn'
        
        # Description mappings
        elif any(word in header_lower for word in ['description', 'summary', 'plot', 'synopsis', 'review']) and 'my review' not in header_lower:
            mappings[header] = 'description'
        
        # Publisher mappings
        elif any(word in header_lower for word in ['publisher', 'publishing', 'imprint']):
            mappings[header] = 'publisher'
        
        # Page count mappings
        elif any(word in header_lower for word in ['number of pages', 'page', 'length']) and 'pages' in header_lower:
            mappings[header] = 'page_count'
        
        # Rating mappings - prefer "My Rating" over "Average Rating"
        elif header_lower in ['my rating', 'star rating']:
            mappings[header] = 'rating'
        elif 'rating' in header_lower and 'my rating' not in [h.lower() for h in headers]:
            mappings[header] = 'rating'
        
        # Reading status mappings - handle both Goodreads and Storygraph
        elif header_lower in ['exclusive shelf', 'read status']:
            mappings[header] = 'reading_status'
        elif any(word in header_lower for word in ['status', 'shelf']) and 'read' not in header_lower:
            mappings[header] = 'reading_status'
        
        # Date mappings
        elif any(word in header_lower for word in ['date read', 'last date read', 'finished', 'completed']):
            mappings[header] = 'date_read'
        elif any(word in header_lower for word in ['date added', 'added']):
            mappings[header] = 'date_added'
        
        # Categories/genres - handle Goodreads bookshelves
        elif any(word in header_lower for word in ['genre', 'category', 'tag', 'subject', 'bookshelves']) and 'positions' not in header_lower:
            mappings[header] = 'categories'
        
        # Language
        elif any(word in header_lower for word in ['language', 'lang']):
            mappings[header] = 'language'
        
        # Year - prefer original publication year
        elif header_lower in ['original publication year']:
            mappings[header] = 'publication_year'
        elif any(word in header_lower for word in ['year published', 'year', 'published', 'publication']) and 'original' not in header_lower:
            if 'publication_year' not in mappings.values():
                mappings[header] = 'publication_year'
        
        # Notes/Review - prefer private notes over public review
        elif header_lower in ['private notes']:
            mappings[header] = 'notes'
        elif any(word in header_lower for word in ['note', 'comment']) and 'private notes' not in [h.lower() for h in headers]:
            mappings[header] = 'notes'
    
    return mappings


def auto_create_custom_fields(mappings, user_id):
    """Auto-create custom fields that are referenced in mappings but don't exist."""
    from app.domain.models import CustomFieldDefinition, CustomFieldType
    from app.services import custom_field_service
    from datetime import datetime
    
    print(f"DEBUG: auto_create_custom_fields called with mappings: {mappings}")
    print(f"DEBUG: mappings type: {type(mappings)}")
    
    # Get existing fields
    existing_global_fields = custom_field_service.get_available_fields_sync(user_id, is_global=True)
    existing_personal_fields = custom_field_service.get_available_fields_sync(user_id, is_global=False)
    
    existing_global_names = {f.name for f in existing_global_fields}
    existing_personal_names = {f.name for f in existing_personal_fields}
    
    # Predefined field configurations for common platform-specific metadata
    FIELD_CONFIGS = {
        # Goodreads fields
        'goodreads_book_id': {'display_name': 'Goodreads Book ID', 'type': CustomFieldType.TEXT, 'global': True},
        'average_rating': {'display_name': 'Average Rating', 'type': CustomFieldType.NUMBER, 'global': True},
        'binding': {'display_name': 'Binding/Format', 'type': CustomFieldType.TEXT, 'global': True},
        'original_publication_year': {'display_name': 'Original Publication Year', 'type': CustomFieldType.NUMBER, 'global': True},
        'spoiler_review': {'display_name': 'Contains Spoilers', 'type': CustomFieldType.BOOLEAN, 'global': True},
        'private_notes': {'display_name': 'Private Notes', 'type': CustomFieldType.TEXTAREA, 'global': False},
        'read_count': {'display_name': 'Read Count', 'type': CustomFieldType.NUMBER, 'global': True},
        'owned_copies': {'display_name': 'Owned Copies', 'type': CustomFieldType.NUMBER, 'global': False},
        
        # StoryGraph fields
        'format': {'display_name': 'Format', 'type': CustomFieldType.TEXT, 'global': True},
        'moods': {'display_name': 'Moods', 'type': CustomFieldType.TEXT, 'global': True},
        'pace': {'display_name': 'Pace', 'type': CustomFieldType.TEXT, 'global': True},
        'character_plot_driven': {'display_name': 'Character or Plot Driven', 'type': CustomFieldType.TEXT, 'global': True},
        'strong_character_development': {'display_name': 'Strong Character Development', 'type': CustomFieldType.BOOLEAN, 'global': True},
        'loveable_characters': {'display_name': 'Loveable Characters', 'type': CustomFieldType.BOOLEAN, 'global': True},
        'diverse_characters': {'display_name': 'Diverse Characters', 'type': CustomFieldType.BOOLEAN, 'global': True},
        'flawed_characters': {'display_name': 'Flawed Characters', 'type': CustomFieldType.BOOLEAN, 'global': True},
        'content_warnings': {'display_name': 'Content Warnings', 'type': CustomFieldType.TEXT, 'global': True},
        'content_warning_description': {'display_name': 'Content Warning Description', 'type': CustomFieldType.TEXTAREA, 'global': True},
        'owned': {'display_name': 'Owned', 'type': CustomFieldType.BOOLEAN, 'global': False},
    }
    
    # Check mappings for custom fields that need to be created
    for csv_field, book_field_info in mappings.items():
        # Handle the new structure where mappings are dictionaries with 'target_field'
        if isinstance(book_field_info, dict):
            book_field = book_field_info.get('target_field', '')
        else:
            # Fallback for simple string mappings
            book_field = book_field_info
        
        # Ensure book_field is a string
        if not isinstance(book_field, str):
            continue
            
        if book_field.startswith('custom_global_'):
            field_name = book_field[14:]  # Remove 'custom_global_' prefix
            if field_name not in existing_global_names:
                # Create the global field
                config = FIELD_CONFIGS.get(field_name, {
                    'display_name': field_name.replace('_', ' ').title(),
                    'type': CustomFieldType.TEXT,
                    'global': True
                })
                
                field_definition = CustomFieldDefinition(
                    name=field_name,
                    display_name=config['display_name'],
                    field_type=config['type'],
                    is_global=True,
                    created_by_user_id=user_id,
                    created_at=datetime.utcnow(),
                    description=f'Auto-created for CSV column "{csv_field}"'
                )
                
                try:
                    custom_field_service.create_field(field_definition)
                    print(f"Auto-created global custom field: {config['display_name']} ({field_name})")
                except Exception as e:
                    print(f"Error auto-creating global field {field_name}: {e}")
        
        elif book_field.startswith('custom_personal_'):
            field_name = book_field[16:]  # Remove 'custom_personal_' prefix
            if field_name not in existing_personal_names:
                # Create the personal field
                config = FIELD_CONFIGS.get(field_name, {
                    'display_name': field_name.replace('_', ' ').title(),
                    'type': CustomFieldType.TEXT,
                    'global': False
                })
                
                field_definition = CustomFieldDefinition(
                    name=field_name,
                    display_name=config['display_name'],
                    field_type=config['type'],
                    is_global=False,
                    created_by_user_id=user_id,
                    created_at=datetime.utcnow(),
                    description=f'Auto-created for CSV column "{csv_field}"'
                )
                
                try:
                    custom_field_service.create_field(field_definition)
                    print(f"Auto-created personal custom field: {config['display_name']} ({field_name})")
                except Exception as e:
                    print(f"Error auto-creating personal field {field_name}: {e}")


def start_import_job(task_id):
    """Start the actual import process (simplified version)."""
    from app.domain.models import Book as DomainBook, Author, Publisher
    from app.services import book_service
    
    print(f"🚀 [START] Starting import job {task_id}")
    
    # Try to get job from both sources
    redis_job = get_job_from_redis(task_id)
    memory_job = import_jobs.get(task_id)
    
    print(f"📊 [START] Redis job found: {bool(redis_job)}")
    print(f"💾 [START] Memory job found: {bool(memory_job)}")
    
    job = redis_job or memory_job
    if not job:
        print(f"❌ [START] Import job {task_id} not found in start_import_job")
        return

    print(f"✅ [START] Starting import job {task_id} for user {job['user_id']}")
    job['status'] = 'running'
    update_job_in_redis(task_id, {'status': 'running'})
    if task_id in import_jobs:
        import_jobs[task_id]['status'] = 'running'

    try:
        csv_file_path = job['csv_file_path']
        mappings = job['field_mappings']
        user_id = job['user_id']
        
        print(f"Processing CSV file: {csv_file_path}")
        print(f"Field mappings: {mappings}")
        print(f"User ID: {user_id} (type: {type(user_id)})")
        
        # Read and process CSV
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            # Try to detect if CSV has headers
            first_line = csvfile.readline().strip()
            csvfile.seek(0)  # Reset to beginning
            
            # Check if first line looks like an ISBN or a header
            has_headers = not (first_line.isdigit() or len(first_line) in [10, 13])
            
            print(f"First line: '{first_line}', Has headers: {has_headers}")
            
            if has_headers:
                reader = csv.DictReader(csvfile)
            else:
                # For headerless CSV (like ISBN-only files), create a simple reader
                reader = csv.reader(csvfile)
                # Convert to dict format for consistency
                dict_reader = []
                for row_data in reader:
                    if row_data and row_data[0].strip():  # Skip empty rows
                        dict_reader.append({'isbn': row_data[0].strip()})
                reader = dict_reader
            
            for row_num, row in enumerate(reader, 1):
                if not has_headers:
                    # For headerless CSV, row is already a dict with 'isbn' key
                    pass
                else:
                    # For CSV with headers, row is already a dict from DictReader
                    pass
                try:
                    print(f"Processing row {row_num}: {row}")
                    
                    # Extract book data based on mappings
                    book_data = {}
                    global_custom_metadata = {}
                    personal_custom_metadata = {}
                    
                    if has_headers:
                        # Use field mappings for CSV with headers
                        for csv_field, book_field in mappings.items():
                            raw_value = row.get(csv_field, '')
                            
                            # Apply Goodreads normalization to all values
                            if book_field == 'isbn':
                                value = normalize_goodreads_value(raw_value, 'isbn')
                            else:
                                value = normalize_goodreads_value(raw_value, 'text')
                            
                            if value:  # Only process non-empty values
                                if book_field == 'isbn':
                                    print(f"Cleaned ISBN from '{raw_value}' to '{value}' (length: {len(value)})")
                                    book_data[book_field] = value
                                elif book_field.startswith('custom_global_'):
                                    # Extract custom global field name
                                    field_name = book_field[14:]  # Remove 'custom_global_' prefix
                                    global_custom_metadata[field_name] = value
                                    print(f"Added global custom metadata: {field_name} = {value}")
                                elif book_field.startswith('custom_personal_'):
                                    # Extract custom personal field name  
                                    field_name = book_field[16:]  # Remove 'custom_personal_' prefix
                                    personal_custom_metadata[field_name] = value
                                    print(f"Added personal custom metadata: {field_name} = {value}")
                                else:
                                    book_data[book_field] = value
                    else:
                        # For headerless CSV, assume it's ISBN-only
                        isbn_value = row.get('isbn', '').strip()
                        if isbn_value:
                            book_data['isbn'] = isbn_value
                            print(f"ISBN from headerless CSV: '{isbn_value}' (length: {len(isbn_value)})")
                    
                    print(f"Extracted book data: {book_data}")
                    
                    # Skip if no title or ISBN
                    if not book_data.get('title') and not book_data.get('isbn'):
                        job['recent_activity'].append(f"Row {row_num}: Skipped - no title or ISBN")
                        print(f"Row {row_num}: Skipped - no title or ISBN")
                        continue
                    
                    # For ISBN-only imports, use ISBN as title temporarily
                    title = book_data.get('title', book_data.get('isbn', 'Unknown Title'))
                    if not book_data.get('title') and book_data.get('isbn'):
                        print(f"Using ISBN as title: {title}")
                    
                    # Update current book
                    job['current_book'] = title
                    print(f"Current book: {job['current_book']}")
                    
                    # Convert reading status from CSV to our enum
                    reading_status_str = book_data.get('reading_status', '').lower()
                    reading_status = None
                    if reading_status_str:
                        print(f"Original reading status: '{reading_status_str}'")
                        if reading_status_str in ['read', 'finished']:
                            reading_status = 'read'
                        elif reading_status_str in ['currently-reading', 'reading']:
                            reading_status = 'reading'
                        elif reading_status_str in ['to-read', 'want-to-read', 'plan-to-read']:
                            reading_status = 'plan_to_read'
                        elif reading_status_str in ['on-hold', 'paused']:
                            reading_status = 'on_hold'
                        elif reading_status_str in ['did-not-finish', 'dnf']:
                            reading_status = 'did_not_finish'
                        print(f"Converted reading status: '{reading_status}'")
                    else:
                        print("No reading status found")
                    
                    # Prepare ISBN fields
                    isbn_value = book_data.get('isbn', '')
                    isbn13 = None
                    isbn10 = None
                    
                    if isbn_value:
                        # Clean ISBN (remove spaces, hyphens, etc.)
                        clean_isbn = ''.join(c for c in isbn_value if c.isdigit() or c.upper() == 'X')
                        print(f"ISBN processing: original='{isbn_value}', cleaned='{clean_isbn}', length={len(clean_isbn)}")
                        
                        if len(clean_isbn) == 13:
                            isbn13 = clean_isbn
                            print(f"Assigned to ISBN13: {isbn13}")
                        elif len(clean_isbn) == 10:
                            isbn10 = clean_isbn
                            print(f"Assigned to ISBN10: {isbn10}")
                        else:
                            print(f"WARNING: ISBN '{clean_isbn}' has unexpected length {len(clean_isbn)}")
                            # Still try to use it, in case it's a valid format we don't recognize
                            if len(clean_isbn) > 10:
                                isbn13 = clean_isbn
                            else:
                                isbn10 = clean_isbn
                    
                    print(f"Final ISBN assignment: isbn13='{isbn13}', isbn10='{isbn10}'")
                    
                    # Fetch additional metadata if ISBN is available
                    # Start with CSV data as base
                    csv_title = title
                    csv_author = book_data.get('author', '')
                    description = book_data.get('description')
                    publisher_name = book_data.get('publisher')
                    page_count = book_data.get('page_count')
                    language = book_data.get('language', 'en')
                    cover_url = None
                    average_rating = None
                    rating_count = None
                    published_date = None
                    google_data = None  # Initialize google_data
                    
                    # Override with API data if available (prioritize API data for richer metadata)
                    # Use the cleaned ISBN for API calls
                    api_isbn = isbn13 or isbn10  # Prefer ISBN13, fallback to ISBN10
                    if api_isbn:
                        print(f"Fetching metadata for cleaned ISBN: {api_isbn}")
                        try:
                            # Try Google Books first
                            google_data = get_google_books_cover(api_isbn, fetch_title_author=True)
                            if google_data:
                                print(f"Got Google Books data: {google_data.keys()}")
                                # Prioritize API title over CSV title if available
                                if google_data.get('title'):
                                    title = google_data['title']
                                    print(f"Updated title from API: {title}")
                                # Prioritize API author over CSV author if available  
                                if google_data.get('author'):
                                    csv_author = google_data['author']
                                    print(f"Updated author from API: {csv_author}")
                                # Get additional metadata
                                description = description or google_data.get('description')
                                # Clean publisher name (remove quotes)
                                api_publisher = google_data.get('publisher')
                                if api_publisher:
                                    api_publisher = api_publisher.strip('"\'')  # Remove quotes
                                publisher_name = publisher_name or api_publisher
                                page_count = page_count or google_data.get('page_count')
                                language = language or google_data.get('language', 'en')
                                cover_url = google_data.get('cover')  # Use 'cover' key not 'cover_url'
                                average_rating = google_data.get('average_rating')
                                rating_count = google_data.get('rating_count')
                                published_date = google_data.get('published_date')
                                if cover_url:
                                    print(f"Got cover URL from Google: {cover_url}")
                            else:
                                # Fallback to OpenLibrary
                                print("No Google Books data, trying OpenLibrary...")
                                ol_data = fetch_book_data(api_isbn)
                                if ol_data:
                                    print(f"Got OpenLibrary data: {ol_data.keys()}")
                                    # Prioritize API title over CSV title if available
                                    if ol_data.get('title'):
                                        title = ol_data['title']
                                        print(f"Updated title from OpenLibrary: {title}")
                                    if ol_data.get('author'):
                                        csv_author = ol_data['author']
                                        print(f"Updated author from OpenLibrary: {csv_author}")
                                    description = description or ol_data.get('description')
                                    # Clean publisher name (remove quotes)
                                    api_publisher = ol_data.get('publisher')
                                    if api_publisher:
                                        api_publisher = api_publisher.strip('"\'')  # Remove quotes
                                    publisher_name = publisher_name or api_publisher
                                    page_count = page_count or ol_data.get('page_count')
                                    language = language or ol_data.get('language', 'en')
                                    published_date = published_date or ol_data.get('published_date')
                        except Exception as api_error:
                            print(f"Error fetching metadata for ISBN {api_isbn}: {api_error}")
                    else:
                        print("No valid ISBN available for API lookup")
                    
                    # Rebuild authors list with updated author data
                    # Build contributors from author data
                    contributors = []
                    if csv_author:
                        # Handle primary author(s) from Google Books API
                        if google_data and google_data.get('authors_list'):
                            # Use individual authors from Google Books API
                            for i, author_name in enumerate(google_data['authors_list']):
                                author_name = author_name.strip()
                                if author_name:
                                    person = Person(name=author_name)
                                    contribution = BookContribution(
                                        person=person,
                                        contribution_type=ContributionType.AUTHORED,
                                        order=i  # First author is primary
                                    )
                                    contributors.append(contribution)
                                    print(f"Added author from Google Books: {author_name}")
                        else:
                            # Fallback to CSV author
                            person = Person(name=csv_author)
                            contribution = BookContribution(
                                person=person,
                                contribution_type=ContributionType.AUTHORED,
                                order=0
                            )
                            contributors.append(contribution)
                            print(f"Added primary author: {csv_author}")
                        
                        # Handle additional authors if present in CSV
                        if book_data.get('additional_authors'):
                            additional_names = book_data['additional_authors'].split(',')
                            for i, name in enumerate(additional_names):
                                name = name.strip()
                                if name:
                                    person = Person(name=name)
                                    contribution = BookContribution(
                                        person=person,
                                        contribution_type=ContributionType.AUTHORED,
                                        order=len(contributors)  # Continue ordering
                                    )
                                    contributors.append(contribution)
                                    print(f"Added additional author: {name}")
                    
                    # Handle Contributors column from StoryGraph
                    contributors_str = book_data.get('contributors', '').strip()
                    if contributors_str and contributors_str != '""' and contributors_str != '':
                        print(f"Processing Contributors field: '{contributors_str}'")
                        # Split contributors by comma and process each one
                        contributor_names = [name.strip() for name in contributors_str.split(',')]
                        for i, name_with_role in enumerate(contributor_names):
                            if name_with_role and name_with_role != '""':  # Skip empty or quoted empty strings
                                # Parse name and role - handle format like "David Wyatt (Contributor)"
                                name = name_with_role
                                contribution_type = ContributionType.CONTRIBUTED  # Default
                                
                                # Check if role is specified in parentheses
                                if '(' in name_with_role and ')' in name_with_role:
                                    # Extract name and role
                                    name = name_with_role.split('(')[0].strip()
                                    role = name_with_role.split('(')[1].split(')')[0].strip().lower()
                                    
                                    # Map role to contribution type
                                    if 'translator' in role or 'translation' in role:
                                        contribution_type = ContributionType.TRANSLATED
                                    elif 'editor' in role or 'edited' in role:
                                        contribution_type = ContributionType.EDITED
                                    elif 'illustrator' in role or 'illustrated' in role:
                                        contribution_type = ContributionType.ILLUSTRATED
                                    elif 'narrator' in role or 'narrated' in role:
                                        contribution_type = ContributionType.NARRATED
                                    elif 'foreword' in role:
                                        contribution_type = ContributionType.GAVE_FOREWORD
                                    elif 'introduction' in role:
                                        contribution_type = ContributionType.GAVE_INTRODUCTION
                                    elif 'afterword' in role:
                                        contribution_type = ContributionType.GAVE_AFTERWORD
                                    # else: keep default CONTRIBUTED
                                    
                                    print(f"Parsed contributor: name='{name}', role='{role}', type={contribution_type.value}")
                                
                                person = Person(name=name)
                                contribution = BookContribution(
                                    person=person,
                                    contribution_type=contribution_type,
                                    order=len(contributors)  # Continue ordering after authors
                                )
                                contributors.append(contribution)
                                print(f"Added contributor: {name} ({contribution_type.value})")
                    else:
                        print(f"No contributors found or empty contributors field: '{contributors_str}'")
                    
                    print(f"Final metadata - Title: {title}, Contributors: {len(contributors)}, Cover: {cover_url}, Publisher: {publisher_name}")
                    
                    # Clean publisher name one more time to be safe
                    if publisher_name:
                        publisher_name = publisher_name.strip('"\'')
                    
                    domain_book = DomainBook(
                        title=title,
                        contributors=contributors,
                        isbn13=isbn13,
                        isbn10=isbn10,
                        description=description,
                        publisher=Publisher(name=publisher_name) if publisher_name else None,
                        page_count=int(page_count) if page_count and str(page_count).isdigit() else None,
                        language=language,
                        cover_url=cover_url,
                        average_rating=average_rating,
                        rating_count=rating_count,
                        published_date=_convert_published_date_to_date(published_date),
                        custom_metadata=global_custom_metadata,  # Add global custom metadata
                        created_at=datetime.now(),
                        updated_at=datetime.now()
                    )
                    
                    print(f"Created domain book: title='{domain_book.title}', contributors={len(domain_book.contributors)}, isbn13='{domain_book.isbn13}', isbn10='{domain_book.isbn10}'")
                    
                    # Create book using the service (globally)
                    print(f"Creating book: {domain_book.title}")
                    try:
                        created_book = book_service.find_or_create_book_sync(domain_book)
                        print(f"Created book result: {created_book}")
                        print(f"Created book type: {type(created_book)}")
                        if created_book:
                            # Convert reading status string to enum
                            reading_status_enum = ReadingStatus.PLAN_TO_READ  # default
                            if reading_status:
                                if reading_status == 'read':
                                    reading_status_enum = ReadingStatus.READ
                                elif reading_status == 'reading':
                                    reading_status_enum = ReadingStatus.READING
                                elif reading_status == 'plan_to_read':
                                    reading_status_enum = ReadingStatus.PLAN_TO_READ
                                elif reading_status == 'on_hold':
                                    reading_status_enum = ReadingStatus.ON_HOLD
                                elif reading_status == 'did_not_finish':
                                    reading_status_enum = ReadingStatus.DNF

                            # Add to user's library
                            success = book_service.add_book_to_user_library_sync(
                                user_id=user_id,
                                book_id=created_book.id,
                                reading_status=reading_status_enum,
                                ownership_status=OwnershipStatus.OWNED
                            )
                            
                            # Add personal custom metadata if available
                            if success and personal_custom_metadata:
                                try:
                                    print(f"Adding personal custom metadata: {personal_custom_metadata}")
                                    # Validate and save personal metadata - TODO: Implement validation
                                    validated, errors = True, []  # custom_field_service.validate_and_save_metadata(personal_custom_metadata, user_id, is_global=False)
                                    if validated:
                                        # Track field usage
                                        # Update the user-book relationship with custom metadata
                                        user_book = book_service.get_user_book_sync(user_id, created_book.id)
                                        if user_book:
                                            # Use the new update_user_book_sync method with correct parameters
                                            success = book_service.update_user_book_sync(user_id, created_book.id, custom_metadata=personal_custom_metadata)
                                            if success:
                                                print(f"Successfully added personal custom metadata")
                                            else:
                                                print(f"Failed to update user-book relationship with custom metadata")
                                        else:
                                            print(f"Could not find user-book relationship to add metadata")
                                    else:
                                        print(f"Personal metadata validation errors: {errors}")
                                        job['recent_activity'].append(f"Row {row_num}: Custom metadata validation errors")
                                except Exception as metadata_error:
                                    print(f"Error processing personal custom metadata: {metadata_error}")
                                    job['recent_activity'].append(f"Row {row_num}: Custom metadata error - {str(metadata_error)}")
                            
                            if success:
                                print(f"Successfully added book to user library")
                            else:
                                print(f"Failed to add book to user library")
                            print(f"Created book ID: {created_book.id}")
                    except Exception as create_error:
                        print(f"Error creating book: {create_error}")
                        created_book = None
                    
                    # Update reading status if provided
                    if created_book and reading_status:
                        try:
                            print(f"Updating reading status to: {reading_status}")
                            update_result = book_service.update_book_sync(created_book.id, user_id, reading_status=reading_status)
                            print(f"Update result: {update_result}")
                        except Exception as update_error:
                            print(f"Failed to update reading status: {update_error}")
                    
                    if created_book:
                        job['success'] += 1
                        status_msg = f" (status: {reading_status})" if reading_status else ""
                        job['recent_activity'].append(f"Row {row_num}: Successfully imported '{created_book.title}'{status_msg}")
                        print(f"SUCCESS: Row {row_num} imported successfully")
                    else:
                        job['errors'] += 1
                        job['error_messages'].append({
                            'row': row_num,
                            'error': 'Failed to create book',
                            'details': 'Unknown error'
                        })
                        job['recent_activity'].append(f"Row {row_num}: Failed to import")
                        print(f"ERROR: Row {row_num} failed to import")
                
                except Exception as e:
                    print(f"Exception in row {row_num}: {e}")
                    print(f"Row data: {row}")
                    job['errors'] += 1
                    job['error_messages'].append({
                        'row': row_num,
                        'error': str(e),
                        'details': f"Data: {row}"
                    })
                    job['recent_activity'].append(f"Row {row_num}: Error - {str(e)}")
                
                job['processed'] += 1
                # Update progress in Redis every 10 books to avoid too many updates
                if job['processed'] % 10 == 0:
                    update_job_in_redis(task_id, {'processed': job['processed']})
                print(f"Row {row_num} processed. Total processed: {job['processed']}")
        
        print(f"CSV processing completed. Success: {job['success']}, Errors: {job['errors']}")
        
        # Mark as completed
        job['status'] = 'completed'
        update_job_in_redis(task_id, {
            'status': 'completed',
            'processed': job['processed'],
            'success': job['success'],
            'errors': job['errors']
        })
        if task_id in import_jobs:
            import_jobs[task_id].update(job)
        job['current_book'] = None
        job['recent_activity'].append(f"Import completed! {job['success']} books imported, {job['errors']} errors")
        
        # Clean up temp file
        try:
            import os
            os.unlink(csv_file_path)
        except:
            pass
            
    except Exception as e:
        job['status'] = 'failed'
        job['error_message'] = str(e)
        update_job_in_redis(task_id, {'status': 'failed', 'error_message': str(e)})
        if task_id in import_jobs:
            import_jobs[task_id]['status'] = 'failed'
            import_jobs[task_id]['error_message'] = str(e)
        print(f"Import job {task_id} failed: {e}")

def normalize_goodreads_value(value, field_type='text'):
    """
    Normalize values from Goodreads CSV exports that use Excel text formatting.
    Goodreads exports often have values like ="123456789" or ="" to force text formatting.
    """
    if not value or not isinstance(value, str):
        return value.strip() if value else ''
    
    # Remove Excel text formatting: ="value" -> value
    if value.startswith('="') and value.endswith('"'):
        value = value[2:-1]  # Remove =" prefix and " suffix
    elif value.startswith('=') and value.endswith('"'):
        value = value[1:-1]  # Remove = prefix and " suffix  
    elif value == '=""':
        value = ''  # Empty quoted value
    
    # Additional cleaning for ISBN fields
    if field_type == 'isbn':
        # Remove any remaining quotes, equals, or whitespace
        value = value.replace('"', '').replace('=', '').strip()
        # Validate that it looks like an ISBN (digits, X, hyphens only)
        if value and not all(c.isdigit() or c in 'X-' for c in value):
            # If it doesn't look like an ISBN, it might be corrupted
            print(f"WARNING: Potentially corrupted ISBN value: '{value}'")
    
    return value.strip()

# Redis functions for job storage
def store_job_in_redis(task_id, job_data):
    """Store import job data in Redis."""
    try:
        from .infrastructure.redis_graph import get_graph_storage
        redis_client = get_graph_storage().redis
        job_key = f'import_job:{task_id}'
        
        # Convert job data to JSON and store with expiration (24 hours)
        import json
        redis_client.setex(job_key, 86400, json.dumps(job_data, default=str))
        print(f"✅ Stored job {task_id} in Redis")
        return True
    except Exception as e:
        print(f"❌ Error storing job {task_id} in Redis: {e}")
        return False

def get_job_from_redis(task_id):
    """Retrieve import job data from Redis."""
    try:
        from .infrastructure.redis_graph import get_graph_storage
        redis_client = get_graph_storage().redis
        job_key = f'import_job:{task_id}'
        
        job_data_str = redis_client.get(job_key)
        if job_data_str:
            import json
            job_data = json.loads(job_data_str)
            print(f"✅ Retrieved job {task_id} from Redis")
            return job_data
        else:
            print(f"❌ Job {task_id} not found in Redis")
            return None
    except Exception as e:
        print(f"❌ Error retrieving job {task_id} from Redis: {e}")
        return None

def update_job_in_redis(task_id, updates):
    """Update specific fields in an import job stored in Redis."""
    try:
        from .infrastructure.redis_graph import get_graph_storage
        redis_client = get_graph_storage().redis
        job_key = f'import_job:{task_id}'
        
        # Get existing job data
        job_data_str = redis_client.get(job_key)
        if job_data_str:
            import json
            job_data = json.loads(job_data_str)
            
            # Update with new values
            job_data.update(updates)
            
            # Store back with same expiration
            redis_client.setex(job_key, 86400, json.dumps(job_data, default=str))
            print(f"✅ Updated job {task_id} in Redis with: {list(updates.keys())}")
            return True
        else:
            print(f"❌ Job {task_id} not found in Redis for update")
            return False
    except Exception as e:
        print(f"❌ Error updating job {task_id} in Redis: {e}")
        return False

# ========================================
# PEOPLE MANAGEMENT ROUTES
# ========================================

@bp.route('/people')
@login_required
def people():
    """Display all people with management options."""
    try:
        # Get all persons with error handling for async issues
        
        # Helper function to handle potential coroutine returns
        def safe_call_sync_method(method, *args, **kwargs):
            """Safely call a sync method that might return a coroutine."""
            import inspect
            result = method(*args, **kwargs)
            if inspect.iscoroutine(result):
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # Can't use loop.run_until_complete if loop is already running
                        print(f"⚠️ [PEOPLE] Loop is running, method {method.__name__} returned coroutine")
                        return []  # Return empty list as fallback
                    else:
                        return loop.run_until_complete(result)
                except Exception as e:
                    print(f"⚠️ [PEOPLE] Error running coroutine for {method.__name__}: {e}")
                    return []
            return result
        
        all_persons = safe_call_sync_method(book_service.list_all_persons_sync)
        
        # Ensure we have a list
        if not isinstance(all_persons, list):
            print(f"⚠️ [PEOPLE] Expected list, got {type(all_persons)}")
            all_persons = []
        
        # Add book counts for each person
        for i, person in enumerate(all_persons):
            # Initialize safe defaults
            person.book_count = 0
            person.contributions = {}
            
            try:
                # Try to get books for this person with safe call
                try:
                    books_by_type = safe_call_sync_method(book_service.get_books_by_person_sync, person.id, str(current_user.id))
                    
                    # Safely handle the result
                    if books_by_type and isinstance(books_by_type, dict):
                        person.contributions = books_by_type
                        # Calculate total books safely
                        try:
                            total_books = 0
                            for book_list in books_by_type.values():
                                if book_list and hasattr(book_list, '__len__'):
                                    total_books += len(book_list)
                            person.book_count = total_books
                        except Exception as count_error:
                            print(f"⚠️ [PEOPLE] Error counting books for {getattr(person, 'name', 'unknown')}: {count_error}")
                            person.book_count = 0
                    else:
                        pass  # No books found
                
                except Exception as book_error:
                    print(f"⚠️ [PEOPLE] Error getting books for person {getattr(person, 'name', 'unknown')}: {book_error}")
                    # Keep defaults: book_count = 0, contributions = {}
                
            except Exception as person_error:
                print(f"❌ [PEOPLE] Error processing person {getattr(person, 'name', 'unknown')} ({getattr(person, 'id', 'unknown')}): {person_error}")
                import traceback
                traceback.print_exc()
                current_app.logger.error(f"Error processing person: {person_error}")
                # Keep defaults: book_count = 0, contributions = {}
        
        # Sort by name safely
        try:
            all_persons.sort(key=lambda p: getattr(p, 'name', '').lower())
        except Exception as sort_error:
            print(f"⚠️ [PEOPLE] Error sorting persons: {sort_error}")
        
        return render_template('people.html', persons=all_persons)
    
    except Exception as e:
        print(f"❌ [PEOPLE] Error loading people page: {e}")
        import traceback
        traceback.print_exc()
        current_app.logger.error(f"Error loading people page: {e}")
        flash('Error loading people page.', 'error')
        return redirect(url_for('main.library'))


@bp.route('/person/<person_id>')
@login_required
def person_details(person_id):
    """Display detailed information about a person."""
    try:
        print(f"🔍 [PERSON] Starting person details page for person_id: {person_id}, user: {current_user.id}")
        
        # Get person details
        print(f"🔍 [PERSON] Calling get_person_by_id_sync for person_id: {person_id}")
        person = book_service.get_person_by_id_sync(person_id)
        print(f"📊 [PERSON] Got person: {person}")
        print(f"📊 [PERSON] Person type: {type(person)}")
        
        if not person:
            print(f"❌ [PERSON] Person not found for ID: {person_id}")
            flash('Person not found.', 'error')
            return redirect(url_for('main.people'))
        
        print(f"✅ [PERSON] Found person: {person.name} (ID: {person.id})")
        
        # Get books by this person for current user
        print(f"🔍 [PERSON] Getting books by person for user {current_user.id}")
        books_by_type = book_service.get_books_by_person_sync(person_id, str(current_user.id))
        print(f"📊 [PERSON] Got books_by_type: {type(books_by_type)}")
        print(f"📊 [PERSON] Books by type keys: {list(books_by_type.keys()) if books_by_type else 'None'}")
        
        if books_by_type:
            for contribution_type, books in books_by_type.items():
                print(f"📋 [PERSON] {contribution_type}: {len(books)} books")
        
        print(f"✅ [PERSON] Rendering template")
        return render_template('person_details.html', 
                             person=person, 
                             contributions_by_type=books_by_type)
    
    except Exception as e:
        print(f"❌ [PERSON] Error loading person details for {person_id}: {e}")
        import traceback
        traceback.print_exc()
        current_app.logger.error(f"Error loading person details: {e}")
        flash('Error loading person details.', 'error')
        return redirect(url_for('main.people'))


@bp.route('/person/add', methods=['GET', 'POST'])
@login_required
def add_person():
    """Add a new person to the library"""
    if request.method == 'POST':
        try:
            # Get form data
            name = request.form.get('name', '').strip()
            bio = request.form.get('bio', '').strip()
            birth_year = request.form.get('birth_year')
            death_year = request.form.get('death_year')
            birth_place = request.form.get('birth_place', '').strip()
            website = request.form.get('website', '').strip()
            
            if not name:
                flash('Name is required.', 'error')
                return render_template('add_person.html', current_year=datetime.now().year)
            
            # Convert years to integers if provided
            birth_year_int = None
            death_year_int = None
            
            if birth_year:
                try:
                    birth_year_int = int(birth_year)
                except ValueError:
                    flash('Birth year must be a valid number.', 'error')
                    return render_template('add_person.html', current_year=datetime.now().year)
            
            if death_year:
                try:
                    death_year_int = int(death_year)
                except ValueError:
                    flash('Death year must be a valid number.', 'error')
                    return render_template('add_person.html', current_year=datetime.now().year)
            
            # Validate year range
            if birth_year_int and (birth_year_int < 0 or birth_year_int > datetime.now().year):
                flash('Birth year must be valid.', 'error')
                return render_template('add_person.html', current_year=datetime.now().year)
            
            if death_year_int and (death_year_int < 0 or death_year_int > datetime.now().year):
                flash('Death year must be valid.', 'error')
                return render_template('add_person.html', current_year=datetime.now().year)
            
            if birth_year_int and death_year_int and death_year_int < birth_year_int:
                flash('Death year cannot be before birth year.', 'error')
                return render_template('add_person.html', current_year=datetime.now().year)
            
            # Create person object
            person = Person(
                id=str(uuid.uuid4()),
                name=name,
                bio=bio if bio else None,
                birth_year=birth_year_int,
                death_year=death_year_int,
                birth_place=birth_place if birth_place else None,
                website=website if website else None,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            
            # Store person in Redis
            from .infrastructure.redis_graph import get_graph_storage
            storage = get_graph_storage()
            
            person_data = {
                '_id': person.id,
                'name': person.name,
                'normalized_name': person.normalized_name,
                'bio': person.bio,
                'birth_year': person.birth_year,
                'death_year': person.death_year,
                'birth_place': person.birth_place,
                'website': person.website,
                'created_at': person.created_at.isoformat(),
                'updated_at': person.updated_at.isoformat()
            }
            
            storage.store_node('person', person.id, person_data)
            
            flash(f'Person "{name}" added successfully!', 'success')
            return redirect(url_for('main.person_details', person_id=person.id))
            
        except Exception as e:
            current_app.logger.error(f"Error adding person: {e}")
            flash('Error adding person. Please try again.', 'error')
    
    return render_template('add_person.html', current_year=datetime.now().year)


@bp.route('/person/<person_id>/edit', methods=['GET', 'POST'])
@login_required
def edit_person(person_id):
    """Edit an existing person."""
    try:
        person = book_service.get_person_by_id_sync(person_id)
        if not person:
            flash('Person not found.', 'error')
            return redirect(url_for('main.people'))
        
        if request.method == 'POST':
            # Get form data
            name = request.form.get('name', '').strip()
            bio = request.form.get('bio', '').strip()
            birth_year = request.form.get('birth_year')
            death_year = request.form.get('death_year')
            birth_place = request.form.get('birth_place', '').strip()
            website = request.form.get('website', '').strip()
            
            if not name:
                flash('Name is required.', 'error')
                return render_template('edit_person.html', person=person, current_year=datetime.now().year)
            
            # Convert years to integers if provided
            birth_year_int = None
            death_year_int = None
            
            if birth_year:
                try:
                    birth_year_int = int(birth_year)
                except ValueError:
                    flash('Birth year must be a valid number.', 'error')
                    return render_template('edit_person.html', person=person, current_year=datetime.now().year)
            
            if death_year:
                try:
                    death_year_int = int(death_year)
                except ValueError:
                    flash('Death year must be a valid number.', 'error')
                    return render_template('edit_person.html', person=person, current_year=datetime.now().year)
            
            # Validate year range
            if birth_year_int and (birth_year_int < 0 or birth_year_int > datetime.now().year):
                flash('Birth year must be valid.', 'error')
                return render_template('edit_person.html', person=person, current_year=datetime.now().year)
            
            if death_year_int and (death_year_int < 0 or death_year_int > datetime.now().year):
                flash('Death year must be valid.', 'error')
                return render_template('edit_person.html', person=person, current_year=datetime.now().year)
            
            if birth_year_int and death_year_int and death_year_int < birth_year_int:
                flash('Death year cannot be before birth year.', 'error')
                return render_template('edit_person.html', person=person, current_year=datetime.now().year)
            
            # Update person data
            person.name = name
            person.bio = bio if bio else None
            person.birth_year = birth_year_int
            person.death_year = death_year_int
            person.birth_place = birth_place if birth_place else None
            person.website = website if website else None
            person.updated_at = datetime.now()
            
            # Update normalized name
            person.normalized_name = Person._normalize_name(person.name)
            
            # Update in Redis
            from .infrastructure.redis_graph import get_graph_storage
            storage = get_graph_storage()
            
            person_data = {
                '_id': person.id,
                'name': person.name,
                'normalized_name': person.normalized_name,
                'bio': person.bio,
                'birth_year': person.birth_year,
                'death_year': person.death_year,
                'birth_place': person.birth_place,
                'website': person.website,
                'created_at': person.created_at.isoformat() if person.created_at else datetime.now().isoformat(),
                'updated_at': person.updated_at.isoformat()
            }
            
            storage.store_node('person', person.id, person_data)
            
            flash(f'Person "{name}" updated successfully!', 'success')
            return redirect(url_for('main.person_details', person_id=person.id))
        
        return render_template('edit_person.html', person=person, current_year=datetime.now().year)
    
    except Exception as e:
        current_app.logger.error(f"Error editing person {person_id}: {e}")
        flash('Error editing person. Please try again.', 'error')
        return redirect(url_for('main.people'))


@bp.route('/person/<person_id>/delete', methods=['POST'])
@login_required
def delete_person(person_id):
    """Delete a person (with confirmation)."""
    try:
        # Helper function to handle potential coroutine returns
        def safe_call_sync_method(method, *args, **kwargs):
            """Safely call a sync method that might return a coroutine."""
            import inspect
            result = method(*args, **kwargs)
            if inspect.iscoroutine(result):
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # Can't use loop.run_until_complete if loop is already running
                        print(f"⚠️ [DELETE_PERSON] Loop is running, method {method.__name__} returned coroutine")
                        return None  # Return None as fallback
                    else:
                        return loop.run_until_complete(result)
                except Exception as e:
                    print(f"⚠️ [DELETE_PERSON] Error running coroutine for {method.__name__}: {e}")
                    return None
            return result
        
        person = safe_call_sync_method(book_service.get_person_by_id_sync, person_id)
        if not person:
            flash('Person not found.', 'error')
            return redirect(url_for('main.people'))
        
        person_name = getattr(person, 'name', 'Unknown Person')
        
        # Check if person has associated books by directly querying the storage layer
        # This bypasses any user filtering and checks for ANY books associated with this person
        from .infrastructure.redis_graph import get_graph_storage
        storage = get_graph_storage()
        
        # FIRST: Clean up orphaned relationships - relationships pointing to books that no longer exist
        print(f"🧹 [DELETE_PERSON] Starting orphaned relationship cleanup for person {person_id}")
        
        # Get all user's books first to check which ones actually exist
        user_books = safe_call_sync_method(book_service.get_user_books_sync, str(current_user.id))
        if user_books is None:
            user_books = []
        
        # Create a set of valid book IDs for quick lookup
        valid_book_ids = set()
        for book in user_books:
            book_id = getattr(book, 'id', None) or getattr(book, '_id', None)
            if book_id:
                valid_book_ids.add(str(book_id))
        
        print(f"🧹 [DELETE_PERSON] User has {len(valid_book_ids)} valid books in their library")
        
        # Find all relationships that point TO this person/author (from any book)
        orphaned_relationships_found = 0
        orphaned_relationships_cleaned = 0
        
        # Get ALL books in the system (not just user's books) to check for orphaned relationships
        all_book_nodes = storage.find_nodes_by_type('book')
        print(f"🧹 [DELETE_PERSON] Checking {len(all_book_nodes)} total books for orphaned relationships")
        
        for book_data in all_book_nodes:
            if not book_data or not book_data.get('_id'):
                continue
                
            book_id = book_data.get('_id')
            if not book_id or not isinstance(book_id, str):
                continue
            
            # Check if this book actually exists in the user's library
            book_exists_in_user_library = book_id in valid_book_ids
            
            # Get ALL relationships from this book
            all_relationships = storage.get_relationships('book', book_id)
            
            # Check for relationships pointing to our person/author
            for rel in all_relationships:
                rel_type = rel.get('relationship', 'unknown')
                to_type = rel.get('to_type')
                to_id = rel.get('to_id')
                
                if ((to_type == 'author' and to_id == person_id) or 
                    (to_type == 'person' and to_id == person_id)):
                    
                    orphaned_relationships_found += 1
                    book_title = book_data.get('title', 'Unknown Title')
                    
                    # If the book doesn't exist in user's library, this is an orphaned relationship
                    if not book_exists_in_user_library:
                        print(f"🗑️ [DELETE_PERSON] Removing orphaned relationship: book '{book_title}' ({book_id}) -> {rel_type} -> {to_type}:{to_id}")
                        try:
                            # Ensure we have valid string values before calling delete_relationship
                            if to_type and to_id and isinstance(to_type, str) and isinstance(to_id, str):
                                storage.delete_relationship('book', book_id, rel_type, to_type, to_id)
                                orphaned_relationships_cleaned += 1
                            else:
                                print(f"⚠️ [DELETE_PERSON] Invalid relationship data: to_type={to_type}, to_id={to_id}")
                        except Exception as cleanup_error:
                            print(f"⚠️ [DELETE_PERSON] Failed to clean orphaned relationship: {cleanup_error}")
        
        print(f"🧹 [DELETE_PERSON] Found {orphaned_relationships_found} total relationships to person")
        print(f"🧹 [DELETE_PERSON] Cleaned {orphaned_relationships_cleaned} orphaned relationships")
        
        # NOW: Count remaining valid books that have relationships to this person/author
        total_associated_books = 0
        associated_book_details = []
        
        print(f"🔍 [DELETE_PERSON] Checking remaining relationships after cleanup")
        
        for book in user_books:
            book_id = getattr(book, 'id', None) or getattr(book, '_id', None)
            if not book_id:
                continue
            
            book_id = str(book_id)
            
            # Check ALL relationships from this book (not just WRITTEN_BY)
            all_relationships = storage.get_relationships('book', book_id)
            
            # Check if any of these relationships point to our person/author
            for rel in all_relationships:
                rel_type = rel.get('relationship', 'unknown')
                to_type = rel.get('to_type')
                to_id = rel.get('to_id')
                
                if ((to_type == 'author' and to_id == person_id) or 
                    (to_type == 'person' and to_id == person_id)):
                    
                    book_title = getattr(book, 'title', 'Unknown Title')
                    total_associated_books += 1
                    associated_book_details.append(f"{book_title} (via {rel_type} -> {to_type})")
                    print(f"🔍 [DELETE_PERSON] Found valid book '{book_title}' linked via {rel_type} -> {to_type}:{to_id}")
                    break  # Found a relationship, count this book and move to next
        
        print(f"📊 [DELETE_PERSON] Total valid associated books found: {total_associated_books}")
        if associated_book_details:
            print(f"📋 [DELETE_PERSON] Valid associated books: {associated_book_details[:5]}")  # Show first 5
        
        if total_associated_books > 0:
            flash(f'Cannot delete "{person_name}" because they are associated with {total_associated_books} books. Please consider merging with another person instead.', 'error')
            return redirect(url_for('main.person_details', person_id=person_id))
        
        # Delete from Redis
        storage.delete_node('person', person_id)
        
        flash(f'Person "{person_name}" deleted successfully.', 'success')
        return redirect(url_for('main.people'))
    
    except Exception as e:
        current_app.logger.error(f"Error deleting person {person_id}: {e}")
        flash('Error deleting person. Please try again.', 'error')
        return redirect(url_for('main.people'))


@bp.route('/person/merge', methods=['GET', 'POST'])
@login_required
def merge_persons():
    """Merge two or more persons into one."""
    if request.method == 'POST':
        try:
            # Get form data
            primary_person_id = request.form.get('primary_person_id')
            merge_person_ids = request.form.getlist('merge_person_ids')
            
            if not primary_person_id:
                flash('Please select a primary person to merge into.', 'error')
                return redirect(url_for('main.merge_persons'))
            
            if not merge_person_ids:
                flash('Please select at least one person to merge.', 'error')
                return redirect(url_for('main.merge_persons'))
            
            if primary_person_id in merge_person_ids:
                flash('Cannot merge a person with themselves.', 'error')
                return redirect(url_for('main.merge_persons'))
            
            # Helper function to handle potential coroutine returns
            def safe_call_sync_method(method, *args, **kwargs):
                """Safely call a sync method that might return a coroutine."""
                import inspect
                result = method(*args, **kwargs)
                if inspect.iscoroutine(result):
                    import asyncio
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            # Can't use loop.run_until_complete if loop is already running
                            print(f"⚠️ [MERGE_PERSON] Loop is running, method {method.__name__} returned coroutine")
                            return None  # Return None as fallback
                        else:
                            return loop.run_until_complete(result)
                    except Exception as e:
                        print(f"⚠️ [MERGE_PERSON] Error running coroutine for {method.__name__}: {e}")
                        return None
                return result
            
            # Get persons
            primary_person = safe_call_sync_method(book_service.get_person_by_id_sync, primary_person_id)
            if not primary_person:
                flash('Primary person not found.', 'error')
                return redirect(url_for('main.merge_persons'))
            
            merge_persons = []
            for person_id in merge_person_ids:
                person = safe_call_sync_method(book_service.get_person_by_id_sync, person_id)
                if person:
                    merge_persons.append(person)
            
            if not merge_persons:
                flash('No valid persons found to merge.', 'error')
                return redirect(url_for('main.merge_persons'))
            
            # Perform merge operation
            from .infrastructure.redis_graph import get_graph_storage
            storage = get_graph_storage()
            
            merged_count = 0
            for merge_person in merge_persons:
                try:
                    # Find all relationships pointing to the merge person
                    # Update WRITTEN_BY relationships to point to primary person
                    all_book_nodes = storage.find_nodes_by_type('book')
                    
                    for book_data in all_book_nodes:
                        if not book_data or not book_data.get('_id'):
                            continue
                        
                        book_id = book_data.get('_id')
                        if not book_id or not isinstance(book_id, str):
                            continue
                            
                        relationships = storage.get_relationships('book', book_id, 'WRITTEN_BY')
                        
                        # Check if any relationships point to the person we're merging
                        for rel in relationships:
                            if ((rel.get('to_type') == 'author' and rel.get('to_id') == merge_person.id) or
                                (rel.get('to_type') == 'person' and rel.get('to_id') == merge_person.id)):
                                
                                # Delete old relationship - with type safety
                                to_type = rel.get('to_type')
                                to_id = rel.get('to_id')
                                if to_type and to_id and isinstance(to_type, str) and isinstance(to_id, str):
                                    storage.delete_relationship('book', book_id, 'WRITTEN_BY', to_type, to_id)
                                    
                                    # Create new relationship to primary person
                                    storage.create_relationship('book', book_id, 'WRITTEN_BY', 'person', primary_person_id)
                                    
                                    current_app.logger.info(f"Merged relationship: Book {book_id} now points to person {primary_person_id} instead of {merge_person.id}")
                    
                    # Delete the merged person
                    storage.delete_node('person', merge_person.id)
                    storage.delete_node('author', merge_person.id)  # Also delete any author node
                    merged_count += 1
                    
                except Exception as e:
                    current_app.logger.error(f"Error merging person {merge_person.id}: {e}")
                    continue
            
            if merged_count > 0:
                person_names = [p.name for p in merge_persons[:merged_count]]
                primary_person_name = getattr(primary_person, 'name', 'Unknown Person')
                flash(f'Successfully merged {merged_count} person(s) ({", ".join(person_names)}) into "{primary_person_name}".', 'success')
            else:
                flash('No persons were merged due to errors.', 'error')
            
            return redirect(url_for('main.person_details', person_id=primary_person_id))
        
        except Exception as e:
            current_app.logger.error(f"Error during person merge: {e}")
            flash('Error merging persons. Please try again.', 'error')
    
    # GET request - show merge form
    try:
        # Helper function to handle potential coroutine returns
        def safe_call_sync_method(method, *args, **kwargs):
            """Safely call a sync method that might return a coroutine."""
            import inspect
            result = method(*args, **kwargs)
            if inspect.iscoroutine(result):
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # Can't use loop.run_until_complete if loop is already running
                        print(f"⚠️ [MERGE_GET] Loop is running, method {method.__name__} returned coroutine")
                        return []  # Return empty list as fallback
                    else:
                        return loop.run_until_complete(result)
                except Exception as e:
                    print(f"⚠️ [MERGE_GET] Error running coroutine for {method.__name__}: {e}")
                    return []
            return result
        
        all_persons = safe_call_sync_method(book_service.list_all_persons_sync)
        if all_persons is None:
            all_persons = []
        all_persons.sort(key=lambda p: p.name.lower())
        return render_template('merge_persons.html', persons=all_persons)
    
    except Exception as e:
        current_app.logger.error(f"Error loading merge persons page: {e}")
        flash('Error loading merge page.', 'error')
        return redirect(url_for('main.people'))


@bp.route('/api/person/search')
@login_required
def api_search_persons():
    """API endpoint for searching persons (used in autocomplete)."""
    query = request.args.get('q', '').strip()
    if len(query) < 2:
        return jsonify([])
    
    try:
        # Helper function to handle potential coroutine returns
        def safe_call_sync_method(method, *args, **kwargs):
            """Safely call a sync method that might return a coroutine."""
            import inspect
            result = method(*args, **kwargs)
            if inspect.iscoroutine(result):
                import asyncio
                try:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # Can't use loop.run_until_complete if loop is already running
                        print(f"⚠️ [SEARCH_API] Loop is running, method {method.__name__} returned coroutine")
                        return []  # Return empty list as fallback
                    else:
                        return loop.run_until_complete(result)
                except Exception as e:
                    print(f"⚠️ [SEARCH_API] Error running coroutine for {method.__name__}: {e}")
                    return []
            return result
        
        persons = safe_call_sync_method(book_service.search_persons_sync, query, 20)
        if persons is None:
            persons = []
        
        results = []
        for person in persons:
            # Get book count for each person
            try:
                books_by_type = safe_call_sync_method(book_service.get_books_by_person_sync, person.id, None)
                if books_by_type is None:
                    books_by_type = {}
                total_books = sum(len(books) for books in books_by_type.values())
            except:
                total_books = 0
            
            results.append({
                'id': person.id,
                'name': person.name,
                'bio': person.bio[:100] + '...' if person.bio and len(person.bio) > 100 else person.bio,
                'book_count': total_books,
                'birth_year': person.birth_year,
                'death_year': person.death_year
            })
        
        return jsonify(results)
    
    except Exception as e:
        current_app.logger.error(f"Error searching persons: {e}")
        return jsonify([])

@bp.route('/toggle_theme', methods=['POST'])
@login_required
def toggle_theme():
    """Toggle user's theme preference between light and dark."""
    try:
        data = request.get_json()
        current_theme = data.get('current_theme', 'light')
        
        # Toggle theme
        new_theme = 'dark' if current_theme == 'light' else 'light'
        
        # Store theme preference in Redis for authenticated users
        if current_user.is_authenticated:
            from .infrastructure.redis_graph import get_graph_storage
            redis_client = get_graph_storage().redis
            theme_key = f'user_theme:{current_user.id}'
            redis_client.set(theme_key, new_theme)
        
        # Also store in session as fallback
        session['theme'] = new_theme
        
        return jsonify({
            'success': True,
            'new_theme': new_theme
        })
        
    except Exception as e:
        current_app.logger.error(f"Error toggling theme: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to toggle theme'
        }), 500

@bp.route('/direct_import', methods=['GET', 'POST'])
@login_required
def direct_import():
    """Handle direct import for Goodreads/StoryGraph."""
    import tempfile
    import os
    
    # Check if we have a suggested file from the session
    suggested = request.args.get('suggested', 'false').lower() == 'true'
    import_type = request.args.get('import_type', 'goodreads')
    suggested_filename = session.get('direct_import_filename')
    
    if request.method == 'GET':
        return render_template('direct_import.html', 
                             suggested=suggested,
                             import_type=import_type,
                             suggested_filename=suggested_filename)
    
    # POST request - handle the import
    try:
        use_suggested_file = request.form.get('use_suggested_file') == 'true'
        
        if use_suggested_file and session.get('direct_import_file'):
            # Use the file from session (already uploaded)
            temp_path = session['direct_import_file']
            original_filename = session.get('direct_import_filename', 'import.csv')
        else:
            # Handle new file upload
            file = request.files.get('csv_file')
            if not file or not file.filename.endswith('.csv'):
                flash('Please select a valid CSV file.', 'error')
                return redirect(url_for('main.direct_import'))
            
            # Save file temporarily
            filename = secure_filename(file.filename)
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv', prefix=f'direct_import_{current_user.id}_')
            file.save(temp_file.name)
            temp_path = temp_file.name
            original_filename = filename
        
        # Detect the file type by checking headers
        with open(temp_path, 'r', encoding='utf-8') as csvfile:
            first_line = csvfile.readline()
            
        # Determine import type based on headers
        goodreads_signatures = ['Book Id', 'Author l-f', 'Bookshelves', 'Exclusive Shelf']
        storygraph_signatures = ['Read Status', 'Moods', 'Pace', 'Character- or Plot-Driven?']
        
        detected_type = None
        if any(sig in first_line for sig in goodreads_signatures):
            detected_type = 'goodreads'
        elif any(sig in first_line for sig in storygraph_signatures):
            detected_type = 'storygraph'
        else:
            flash('This file does not appear to be a valid Goodreads or StoryGraph export.', 'error')
            return redirect(url_for('main.direct_import'))
        
        # Create an import job using the existing import infrastructure
        task_id = str(uuid.uuid4())
        
        # Get the appropriate field mappings for the detected platform
        if detected_type == 'goodreads':
            field_mappings = get_goodreads_field_mappings()
        else:  # storygraph
            field_mappings = get_storygraph_field_mappings()
        
        # Create import job data
        job_data = {
            'task_id': task_id,
            'user_id': current_user.id,
            'csv_file_path': temp_path,
            'field_mappings': field_mappings,
            'default_reading_status': 'library_only',
            'duplicate_handling': 'skip',
            'custom_fields_enabled': True,
            'direct_import_type': detected_type,
            'status': 'pending',
            'processed': 0,
            'success': 0,
            'errors': 0,
            'total': 0,
            'start_time': datetime.utcnow().isoformat(),
            'current_book': None,
            'error_messages': [],
            'recent_activity': []
        }
        
        # Count total rows
        try:
            with open(temp_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                job_data['total'] = sum(1 for _ in reader)
        except:
            job_data['total'] = 0
        
        # Store job data
        print(f"🏗️ [CREATE] Creating job {task_id} for user {current_user.id}")
        redis_success = store_job_in_redis(task_id, job_data)
        import_jobs[task_id] = job_data
        
        print(f"📊 [CREATE] Redis storage: {'✅' if redis_success else '❌'}")
        print(f"💾 [CREATE] Memory storage: ✅")
        print(f"🔧 [CREATE] Job status: {job_data['status']}")
        
        # Auto-create custom fields for the platform
        auto_create_custom_fields(field_mappings, current_user.id)
        
        # Start the import in background
        app = current_app._get_current_object()
        def run_import():
            with app.app_context():
                try:
                    start_import_job(task_id)
                except Exception as e:
                    if task_id in import_jobs:
                        import_jobs[task_id]['status'] = 'failed'
                        import_jobs[task_id]['error_message'] = str(e)
                    app.logger.error(f"Direct import job {task_id} failed: {e}")
        
        thread = threading.Thread(target=run_import)
        thread.daemon = True
        thread.start()
        
        # Clear session data
        session.pop('direct_import_file', None)
        session.pop('direct_import_filename', None)
        
        flash(f'Started {detected_type.title()} import! You can monitor progress on the next page.', 'success')
        return redirect(url_for('main.import_books_progress', task_id=task_id))
        
    except Exception as e:
        current_app.logger.error(f"Error in direct import: {e}")
        flash('An error occurred during import. Please try again.', 'error')
        return redirect(url_for('main.direct_import'))


def get_goodreads_field_mappings():
    """Get predefined field mappings for Goodreads CSV format."""
    return {
        'Title': 'title',
        'Author': 'author',
        'Author l-f': 'author',  # Fallback for "last, first" format
        'Additional Authors': 'additional_authors',
        'ISBN': 'isbn',
        'ISBN13': 'isbn',
        'My Rating': 'rating',
        'Average Rating': 'custom_global_average_rating',
        'Publisher': 'publisher',
        'Binding': 'custom_global_binding',
        'Number of Pages': 'page_count',
        'Year Published': 'publication_year',
        'Original Publication Year': 'custom_global_original_publication_year',
        'Date Read': 'date_read',
        'Date Added': 'date_added',
        'Bookshelves': 'categories',
        'Bookshelves with positions': 'categories',
        'Exclusive Shelf': 'reading_status',
        'My Review': 'notes',
        'Spoiler': 'custom_global_spoiler_review',
        'Private Notes': 'custom_personal_private_notes',
        'Read Count': 'custom_global_read_count',
        'Owned Copies': 'custom_personal_owned_copies',
        'Book Id': 'custom_global_goodreads_book_id'
    }


def get_storygraph_field_mappings():
    """Get predefined field mappings for StoryGraph CSV format."""
    return {
        'Title': 'title',
        'Authors': 'author',
        'Contributors': 'contributors',  # New: Handle Contributors column
        'ISBN/UID': 'isbn',  # Fixed: Use actual StoryGraph column name
        'Star Rating': 'rating',  # Fixed: StoryGraph uses "Star Rating" not "My Rating"
        'Read Status': 'reading_status',
        'Date Started': 'start_date',
        'Last Date Read': 'date_read',  # Fixed: StoryGraph uses "Last Date Read"
        'Tags': 'categories',
        'Review': 'notes',  # Fixed: StoryGraph uses "Review" not "My Review"
        'Format': 'custom_global_format',
        'Moods': 'custom_global_moods',
        'Pace': 'custom_global_pace',
        'Character- or Plot-Driven?': 'custom_global_character_plot_driven',
        'Strong Character Development?': 'custom_global_strong_character_development',
        'Loveable Characters?': 'custom_global_loveable_characters',
        'Diverse Characters?': 'custom_global_diverse_characters',
        'Flawed Characters?': 'custom_global_flawed_characters',
        'Content Warnings': 'custom_global_content_warnings',
        'Content Warning Description': 'custom_global_content_warning_description',
        'Owned?': 'custom_personal_owned'
    }