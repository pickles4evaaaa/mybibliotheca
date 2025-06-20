from flask import Blueprint, current_app, render_template, request, redirect, url_for, jsonify, flash, send_file, abort, make_response
from flask_login import login_required, current_user
from .domain.models import Book as DomainBook, Author, Publisher, User
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
    start_date = datetime.now().strftime('%Y-%m-%d')
    
    book = Book(title=title, author=author, isbn=isbn, user_id=current_user.id, start_date=start_date)
    book.save()  # Assuming save method is defined in the Book model
    
    return jsonify({'message': 'Book logged successfully', 'book': book.to_dict()}), 201

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
    if isbn and Book.query.filter_by(isbn=isbn, user_id=current_user.id).first():
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

    book = Book(
        title=title,
        author=author,
        isbn=isbn,
        user_id=current_user.id,  # Add user_id for multi-user support
        cover_url=cover_url,
        description=description,
        published_date=published_date,
        page_count=page_count,
        categories=categories,
        publisher=publisher,
        language=language,
        average_rating=average_rating,
        rating_count=rating_count
    )
    book.save()
    flash(f'Added "{title}" to your library.', 'success')
    return redirect(url_for('main.add_book_page'))


@bp.route('/download_db', methods=['GET'])
@login_required
def download_db():
    db_path = current_app.config.get('SQLALCHEMY_DATABASE_URI').replace('sqlite:///', '')
    return send_file(
        db_path,
        as_attachment=True,
        download_name='books.db',
        mimetype='application/octet-stream'
    )

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

@bp.route('/user/<int:user_id>/profile')
@login_required
def user_profile(user_id):
    """Show public profile for a user if they're sharing"""
    user = User.query.get_or_404(user_id)
    
    # Check if user allows profile viewing
    if not user.share_reading_activity:
        flash('This user has not enabled profile sharing.', 'warning')
        return redirect(url_for('main.stats'))
    
    # Get user's reading statistics
    total_books = Book.query.filter(
        Book.user_id == user.id,
        Book.finish_date.isnot(None)
    ).count()
    
    books_this_year = Book.query.filter(
        Book.user_id == user.id,
        Book.finish_date.isnot(None),
        Book.finish_date >= date(datetime.now().year, 1, 1)
    ).count()
    
    books_this_month = Book.query.filter(
        Book.user_id == user.id,
        Book.finish_date.isnot(None),
        Book.finish_date >= datetime.now().date().replace(day=1)
    ).count()
    
    currently_reading = Book.query.filter(
        Book.user_id == user.id,
        Book.start_date.isnot(None),
        Book.finish_date.is_(None)
    ).all() if user.share_current_reading else []
    
    recent_finished = Book.query.filter(
        Book.user_id == user.id,
        Book.finish_date.isnot(None)
    ).order_by(Book.finish_date.desc()).limit(10).all()
    
    # Get reading logs count - use service layer for future enhancement
    reading_logs_count = ReadingLog.query.filter_by(user_id=user.id).count()
    
    return render_template('user_profile.html',
                         profile_user=user,
                         total_books=total_books,
                         books_this_year=books_this_year,
                         books_this_month=books_this_month,
                         currently_reading=currently_reading,
                         recent_finished=recent_finished,
                         reading_logs_count=reading_logs_count)

@bp.route('/book/<uid>/assign', methods=['POST'])
@login_required
def assign_book(uid):
    book = Book.query.filter_by(uid=uid).first_or_404()
    if not current_user.is_admin:
        flash('Only admins can assign books.', 'danger')
        return redirect(url_for('main.library'))

    user_id = request.form.get('user_id')
    user = User.query.get(user_id)
    if not user:
        flash('Invalid user selected.', 'danger')
        return redirect(url_for('main.library'))

    book.user_id = user.id
    db.session.commit()
    flash(f'Book "{book.title}" assigned to {user.username}.', 'success')
    return redirect(url_for('main.library'))

@bp.route('/books/bulk_delete', methods=['POST'])
@login_required
def bulk_delete_books():
    """Delete multiple books selected from the library view."""
    print(f"Bulk delete route called by user {current_user.id}")
    print(f"Form data: {request.form}")
    
    selected_uids = request.form.getlist('selected_books')
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
    sharing_users = User.query.filter_by(share_reading_activity=True, is_active=True).all()
    
    # Recent community activity
    recent_finished_books = Book.query.join(User).filter(
        User.share_reading_activity == True,
        User.is_active == True,
        Book.finish_date.isnot(None),
        Book.finish_date >= (today - timedelta(days=30))
    ).order_by(Book.finish_date.desc()).limit(20).all()
    
    # Community currently reading
    community_currently_reading = Book.query.join(User).filter(
        User.share_current_reading == True,
        User.is_active == True,
        Book.start_date.isnot(None),
        Book.finish_date.is_(None)
    ).order_by(Book.start_date.desc()).limit(20).all()
    
    # Community stats summary
    total_books_this_month = Book.query.join(User).filter(
        User.share_reading_activity == True,
        User.is_active == True,
        Book.finish_date.isnot(None),
        Book.finish_date >= month_start
    ).count()
    
    total_active_readers = len(sharing_users)
    
    # Recent reading logs (from last 7 days)
    recent_logs = ReadingLog.query.join(User).filter(
        User.share_reading_activity == True,
        User.is_active == True,
        ReadingLog.date >= (today - timedelta(days=7))
    ).order_by(ReadingLog.date.desc()).limit(50).all()
    
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
        # Import domain models for Redis
        from .domain.models import Book as DomainBook, Author, Publisher
        import uuid
        
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
        
        # Use find_or_create_book to avoid duplicates
        existing_book = book_service.find_or_create_book_sync(domain_book, current_user.id)
        
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
                # Try to detect delimiter
                sample = csvfile.read(1024)
                csvfile.seek(0)
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter
                
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
    
    import_jobs[task_id] = job_data
    
    # Start the import in background (for now, just mark as running)
    # In production, you'd use Celery, RQ, or similar
    job_data['status'] = 'running'
    
    # Start the actual import process
    start_import_job(task_id)
    
    return redirect(url_for('main.import_books_progress', task_id=task_id))

@bp.route('/import-books/progress/<task_id>')
@login_required
def import_books_progress(task_id):
    """Show import progress page."""
    job = import_jobs.get(task_id)
    if not job or job['user_id'] != current_user.id:
        flash('Import job not found.', 'danger')
        return redirect(url_for('main.import_books'))
    
    return render_template('import_books_progress.html',
                         task_id=task_id,
                         total_books=job['total'],
                         start_time=job['start_time'])

@bp.route('/api/import/progress/<task_id>')
@login_required
def api_import_progress(task_id):
    """API endpoint for import progress."""
    job = import_jobs.get(task_id)
    if not job or job['user_id'] != current_user.id:
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

def auto_detect_fields(headers):
    """Auto-detect field mappings based on header names."""
    mappings = {}
    
    for header in headers:
        header_lower = header.lower().strip()
        
        # Title mappings
        if any(word in header_lower for word in ['title', 'book', 'name']):
            mappings[header] = 'title'
        
        # Author mappings
        elif any(word in header_lower for word in ['author', 'creator', 'writer']):
            mappings[header] = 'author'
        
        # ISBN mappings
        elif any(word in header_lower for word in ['isbn', 'isbn13', 'isbn10', 'upc', 'barcode']):
            mappings[header] = 'isbn'
        
        # Description mappings
        elif any(word in header_lower for word in ['description', 'summary', 'plot', 'synopsis']):
            mappings[header] = 'description'
        
        # Publisher mappings
        elif any(word in header_lower for word in ['publisher', 'publishing', 'imprint']):
            mappings[header] = 'publisher'
        
        # Page count mappings
        elif any(word in header_lower for word in ['page', 'length', 'pages']):
            mappings[header] = 'page_count'
        
        # Rating mappings
        elif any(word in header_lower for word in ['rating', 'score', 'my rating']):
            mappings[header] = 'rating'
        
        # Reading status mappings
        elif any(word in header_lower for word in ['status', 'shelf', 'read status']):
            mappings[header] = 'reading_status'
        
        # Date mappings
        elif any(word in header_lower for word in ['date read', 'finished', 'completed']):
            mappings[header] = 'date_read'
        elif any(word in header_lower for word in ['date added', 'added']):
            mappings[header] = 'date_added'
        
        # Categories/genres
        elif any(word in header_lower for word in ['genre', 'category', 'tag', 'subject']):
            mappings[header] = 'categories'
        
        # Language
        elif any(word in header_lower for word in ['language', 'lang']):
            mappings[header] = 'language'
        
        # Year
        elif any(word in header_lower for word in ['year', 'published', 'publication']):
            mappings[header] = 'publication_year'
        
        # Notes/Review
        elif any(word in header_lower for word in ['note', 'review', 'comment']):
            mappings[header] = 'notes'
    
    return mappings

def start_import_job(task_id):
    """Start the actual import process (simplified version)."""
    job = import_jobs.get(task_id)
    if not job:
        return
    
    try:
        csv_file_path = job['csv_file_path']
        mappings = job['field_mappings']
        user_id = job['user_id']
        
        # Read and process CSV
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row_num, row in enumerate(reader, 1):
                try:
                    # Extract book data based on mappings
                    book_data = {}
                    for csv_field, book_field in mappings.items():
                        value = row.get(csv_field, '').strip()
                        if value:
                            book_data[book_field] = value
                    
                    # Skip if no title or ISBN
                    if not book_data.get('title') and not book_data.get('isbn'):
                        job['recent_activity'].append(f"Row {row_num}: Skipped - no title or ISBN")
                        continue
                    
                    # Update current book
                    job['current_book'] = book_data.get('title', book_data.get('isbn', f'Row {row_num}'))
                    
                    # Create domain book object
                    authors = []
                    if book_data.get('author'):
                        authors = [Author(name=book_data['author'])]
                    
                    domain_book = DomainBook(
                        title=book_data.get('title', 'Unknown Title'),
                        authors=authors,
                        isbn13=book_data.get('isbn') if len(book_data.get('isbn', '')) == 13 else None,
                        isbn10=book_data.get('isbn') if len(book_data.get('isbn', '')) == 10 else None,
                        description=book_data.get('description'),
                        publisher=Publisher(name=book_data['publisher']) if book_data.get('publisher') else None,
                        page_count=int(book_data['page_count']) if book_data.get('page_count', '').isdigit() else None,
                        language=book_data.get('language', 'en')
                    )
                    
                    # Create book using the service
                    created_book = book_service.create_book_sync(domain_book, user_id)
                    
                    if created_book:
                        job['success'] += 1
                        job['recent_activity'].append(f"Row {row_num}: Successfully imported '{created_book.title}'")
                    else:
                        job['errors'] += 1
                        job['error_messages'].append({
                            'row': row_num,
                            'error': 'Failed to create book',
                            'details': 'Unknown error'
                        })
                        job['recent_activity'].append(f"Row {row_num}: Failed to import")
                
                except Exception as e:
                    job['errors'] += 1
                    job['error_messages'].append({
                        'row': row_num,
                        'error': str(e),
                        'details': f"Data: {row}"
                    })
                    job['recent_activity'].append(f"Row {row_num}: Error - {str(e)}")
                
                job['processed'] += 1
        
        # Mark as completed
        job['status'] = 'completed'
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
        current_app.logger.error(f"Import job {task_id} failed: {e}")