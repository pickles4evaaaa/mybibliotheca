from flask import Blueprint, current_app, render_template, request, redirect, url_for, jsonify, flash, send_file, abort, make_response
from flask_login import login_required, current_user
from .domain.models import Book as DomainBook, Author, Publisher, User, ReadingStatus, OwnershipStatus
from .services import book_service, user_service, reading_log_service
from .utils import fetch_book_data, get_reading_streak, get_google_books_cover, generate_month_review_image
from datetime import datetime, date, timedelta
import secrets
import requests
from io import BytesIO
import pytz
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

@bp.route('/log_book', methods=['POST'])
@login_required
def log_book():
    data = request.json
    title = data.get('title')
    author = data.get('author')
    isbn = data.get('isbn')
    
    try:
        # Create a basic book using the service
        domain_book = DomainBook(
            id=str(uuid.uuid4()),
            title=title,
            authors=[Author(id=str(uuid.uuid4()), name=author)] if author else [],
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
    """Redirect to library page where Add Book functionality is now integrated"""
    flash('Add Book functionality has been moved to the Library page for a better experience.', 'info')
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
    print(f"Individual delete route called for book {uid} by user {current_user.id}")
    
    # Delete through service layer
    success = book_service.delete_book_sync(uid, str(current_user.id))
    print(f"Individual delete result for {uid}: {success}")
    
    if success:
        flash('Book deleted successfully.')
        print(f"Book {uid} deleted successfully")
    else:
        flash('Failed to delete book.', 'error')
        print(f"Failed to delete book {uid}")
        
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
        new_isbn = request.form['isbn']
        
        # TODO: Check for duplicate ISBN in Redis
        # For now, skip duplicate check
        
        update_data = {
            'title': request.form['title'],
            'description': request.form.get('description', '').strip() or None,
            'page_count': int(request.form.get('page_count')) if request.form.get('page_count', '').strip() else None,
            'language': request.form.get('language', '').strip() or None,
            'cover_url': request.form.get('cover_url', '').strip() or None,
            'isbn13': new_isbn if len(new_isbn.replace('-', '')) == 13 else None,
            'isbn10': new_isbn if len(new_isbn.replace('-', '')) == 10 else None,
        }
        
        # Remove None values
        update_data = {k: v for k, v in update_data.items() if v is not None}
        
        success = book_service.update_book_sync(uid, str(current_user.id), **update_data)
        if success:
            flash('Book updated.', 'success')
        else:
            flash('Failed to update book.', 'error')
        return redirect(url_for('main.view_book', uid=uid))
        
    return render_template('edit_book.html', book=user_book)


@bp.route('/book/<uid>/enhanced')
@login_required
def view_book_enhanced(uid):
    """Enhanced book view with new status system."""
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
    
    # For now, redirect to enhanced template with existing data
    # TODO: Update service layer to provide enhanced status data
    return render_template('view_book_enhanced.html', book=user_book)


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

    # Prevent duplicate ISBNs
    if isbn:
        existing_book = book_service.get_books_by_isbn_sync(isbn, current_user.id)
        if existing_book:
            flash('A book with this ISBN already exists.', 'danger')
            return redirect(url_for('main.search_books'))

    # Get additional metadata if available
    if isbn:
        google_data = get_google_books_cover(isbn, fetch_title_author=True)
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

    # Create a domain book object
    domain_book = DomainBook(
        title=title,
        isbn13=isbn if isbn and len(isbn) == 13 else None,
        isbn10=isbn if isbn and len(isbn) == 10 else None,
        authors=[Author(name=author)] if author else [],
        cover_url=cover_url,
        description=description,
        published_date=published_date,
        page_count=page_count,
        categories=categories.split(',') if categories else [],
        publishers=[Publisher(name=publisher)] if publisher else [],
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
    
    return redirect(url_for('main.add_book_page'))


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
    start_date_str = request.form.get('start_date') or None
    finish_date_str = request.form.get('finish_date') or None
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
    finish_date = datetime.strptime(finish_date_str, '%Y-%m-%d').date() if finish_date_str else None
    want_to_read = 'want_to_read' in request.form
    library_only = 'library_only' in request.form

    # If no cover URL provided, try to fetch one
    if not cover_url and isbn:
        cover_url = get_google_books_cover(isbn)

    # Get additional metadata if ISBN is provided
    description = published_date = page_count = categories = publisher = language = average_rating = rating_count = None
    
    if isbn:
        google_data = get_google_books_cover(isbn, fetch_title_author=True)
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
            ol_data = fetch_book_data(isbn)
            if ol_data:
                description = ol_data.get('description')
                published_date = ol_data.get('published_date')
                page_count = ol_data.get('page_count')
                categories = ol_data.get('categories')
                publisher = ol_data.get('publisher')
                language = ol_data.get('language')

    try:
        # Create domain book object
        domain_book = DomainBook(
            id=str(uuid.uuid4()),
            title=title,
            description=description,
            published_date=published_date,
            page_count=page_count,
            language=language or "en",
            cover_url=cover_url,
            isbn13=isbn if len(isbn.replace('-', '')) == 13 else None,
            isbn10=isbn if len(isbn.replace('-', '')) == 10 else None,
            average_rating=average_rating,
            rating_count=rating_count,
            authors=[Author(id=str(uuid.uuid4()), name=author)] if author else [],
            publisher=Publisher(id=str(uuid.uuid4()), name=publisher) if publisher else None,
            categories=[],  # TODO: Parse categories
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        # Use find_or_create_book to avoid duplicates (global)
        existing_book = book_service.find_or_create_book_sync(domain_book)
        
        # Add to user's library
        book_service.add_book_to_user_library_sync(
            user_id=current_user.id,
            book_id=existing_book.id,
            reading_status=ReadingStatus.PLAN_TO_READ
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
            flash(f'Book "{title}" added successfully to your library.', 'success')
        else:
            # Existing book was found
            flash(f'Book "{title}" already exists in the library. Added to your collection.', 'info')
            
    except Exception as e:
        current_app.logger.error(f"Error adding book manually: {e}")
        flash('An error occurred while adding the book. Please try again.', 'danger')

    return redirect(url_for('main.add_book_page'))

@bp.route('/add_book', methods=['GET'])
@login_required
def add_book_page():
    """New dedicated page for adding books"""
    return render_template('add_book_new.html')

@bp.route('/import-books', methods=['GET', 'POST'])
@login_required
def import_books():
    """New unified import interface."""
    if request.method == 'POST':
        # Handle file upload and show field mapping
        file = request.files.get('csv_file')
        if not file or not file.filename.endswith('.csv'):
            flash('Please upload a valid CSV file.', 'danger')
            return redirect(url_for('main.import_books'))
        
        try:
            # Save file temporarily
            filename = secure_filename(file.filename)
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv', prefix=f'import_{current_user.id}_')
            file.save(temp_file.name)
            temp_path = temp_file.name
            
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
            suggested_mappings = auto_detect_fields(headers)
            
            return render_template('import_books_mapping.html',
                                 csv_file_path=temp_path,
                                 csv_headers=headers,
                                 csv_preview=preview_rows,
                                 total_rows=total_rows,
                                 suggested_mappings=suggested_mappings)
                                 
        except Exception as e:
            current_app.logger.error(f"Error processing CSV file: {e}")
            flash('Error processing CSV file. Please check the format and try again.', 'danger')
            return redirect(url_for('main.import_books'))
    
    return render_template('import_books.html')

@bp.route('/import-books/execute', methods=['POST'])
@login_required
def import_books_execute():
    """Execute the import with user-defined field mappings."""
    csv_file_path = request.form.get('csv_file_path')
    field_mapping = request.form.to_dict()
    
    # Extract mapping from form data
    mappings = {}
    for key, value in field_mapping.items():
        if key.startswith('field_mapping[') and value:
            csv_field = key[14:-1]  # Remove 'field_mapping[' and ']'
            mappings[csv_field] = value
    
    default_reading_status = request.form.get('default_reading_status', 'library_only')
    duplicate_handling = request.form.get('duplicate_handling', 'skip')
    
    # Create import job
    task_id = str(uuid.uuid4())
    job_data = {
        'task_id': task_id,
        'user_id': current_user.id,
        'csv_file_path': csv_file_path,
        'field_mappings': mappings,
        'default_reading_status': default_reading_status,
        'duplicate_handling': duplicate_handling,
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
    store_job_in_redis(task_id, job_data)
    
    # Also keep in memory for backward compatibility
    import_jobs[task_id] = job_data
    
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
    # Try Redis first, then fall back to memory
    job = get_job_from_redis(task_id) or import_jobs.get(task_id)
    if not job:
        flash('Import job not found. This may be due to a server restart. Please start a new import.', 'warning')
        return redirect(url_for('main.import_books'))
    
    if job['user_id'] != current_user.id:
        flash('Access denied to this import job.', 'danger')
        return redirect(url_for('main.import_books'))
    
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

def auto_detect_fields(headers):
    """Auto-detect field mappings based on header names."""
    mappings = {}
    
    for header in headers:
        header_lower = header.lower().strip()
        
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

def start_import_job(task_id):
    """Start the actual import process (simplified version)."""
    from app.domain.models import Book as DomainBook, Author, Publisher
    from app.services import book_service
    
    job = get_job_from_redis(task_id) or import_jobs.get(task_id)
    if not job:
        print(f"Import job {task_id} not found in start_import_job")
        return

    print(f"Starting import job {task_id} for user {job['user_id']}")
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
                    
                    if has_headers:
                        # Use field mappings for CSV with headers
                        for csv_field, book_field in mappings.items():
                            value = row.get(csv_field, '').strip()
                            if value:
                                # Clean ISBN values (remove quotes and equals signs from Goodreads format)
                                if book_field == 'isbn':
                                    value = value.replace('="', '').replace('"', '').replace('=', '').strip()
                                    print(f"Cleaned ISBN: '{value}' (length: {len(value)})")
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
                    isbn13 = isbn_value if len(isbn_value) == 13 else None
                    isbn10 = isbn_value if len(isbn_value) == 10 else None
                    print(f"ISBN processing: original='{isbn_value}', isbn13='{isbn13}', isbn10='{isbn10}'")
                    
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
                    
                    # Override with API data if available (prioritize API data for richer metadata)
                    if isbn_value:
                        print(f"Fetching metadata for ISBN: {isbn_value}")
                        try:
                            # Try Google Books first
                            google_data = get_google_books_cover(isbn_value, fetch_title_author=True)
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
                                ol_data = fetch_book_data(isbn_value)
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
                            print(f"Error fetching metadata for ISBN {isbn_value}: {api_error}")
                    
                    # Rebuild authors list with updated author data
                    authors = []
                    if csv_author:
                        # Handle primary author
                        authors.append(Author(name=csv_author))
                        print(f"Added primary author: {csv_author}")
                        # Handle additional authors if present
                        if book_data.get('additional_authors'):
                            additional_names = book_data['additional_authors'].split(',')
                            for name in additional_names:
                                name = name.strip()
                                if name:
                                    authors.append(Author(name=name))
                                    print(f"Added additional author: {name}")
                    
                    print(f"Final metadata - Title: {title}, Authors: {len(authors)}, Cover: {cover_url}, Publisher: {publisher_name}")
                    
                    # Clean publisher name one more time to be safe
                    if publisher_name:
                        publisher_name = publisher_name.strip('"\'')
                    
                    domain_book = DomainBook(
                        title=title,
                        authors=authors,
                        isbn13=isbn13,
                        isbn10=isbn10,
                        description=description,
                        publisher=Publisher(name=publisher_name) if publisher_name else None,
                        page_count=int(page_count) if page_count and str(page_count).isdigit() else None,
                        language=language,
                        cover_url=cover_url,
                        average_rating=average_rating,
                        rating_count=rating_count,
                        published_date=published_date,
                        created_at=datetime.now(),
                        updated_at=datetime.now()
                    )
                    
                    print(f"Created domain book: title='{domain_book.title}', authors={len(domain_book.authors)}, isbn13='{domain_book.isbn13}', isbn10='{domain_book.isbn10}'")
                    
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
                                    reading_status_enum = ReadingStatus.DID_NOT_FINISH
                            
                            # Add to user's library
                            success = book_service.add_book_to_user_library_sync(
                                user_id=user_id,
                                book_id=created_book.id,
                                reading_status=reading_status_enum,
                                ownership_status=OwnershipStatus.OWNED
                            )
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

# Redis-based job storage
def store_job_in_redis(task_id: str, job_data: dict):
    """Store import job data in Redis."""
    try:
        from .infrastructure.redis_graph import get_graph_storage
        storage = get_graph_storage()
        storage.redis.setex(f"import_job:{task_id}", 3600, json.dumps(job_data))  # 1 hour TTL
        print(f"Stored job {task_id} in Redis")
    except Exception as e:
        print(f"Error storing job in Redis: {e}")

def get_job_from_redis(task_id: str) -> dict:
    """Get import job data from Redis."""
    try:
        from .infrastructure.redis_graph import get_graph_storage
        storage = get_graph_storage()
        job_data = storage.redis.get(f"import_job:{task_id}")
        if job_data:
            return json.loads(job_data)
        return None
    except Exception as e:
        print(f"Error getting job from Redis: {e}")
        return None

def update_job_in_redis(task_id: str, updates: dict):
    """Update import job data in Redis."""
    try:
        job_data = get_job_from_redis(task_id)
        if job_data:
            job_data.update(updates)
            store_job_in_redis(task_id, job_data)
    except Exception as e:
        print(f"Error updating job in Redis: {e}")