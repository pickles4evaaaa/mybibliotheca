from flask import Blueprint, current_app, render_template, request, redirect, url_for, jsonify, flash, send_file
from flask_login import login_required, current_user
from .models import Book, db, ReadingLog, User
from .utils import fetch_book_data, get_reading_streak, get_google_books_cover, generate_month_review_image
from datetime import datetime, date, timedelta
import secrets
import requests
from io import BytesIO
import pytz
import csv # Ensure csv is imported
import calendar
from sqlalchemy import or_

bp = Blueprint('main', __name__)

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

@bp.route('/reading_history', methods=['GET'])
@login_required
def reading_history():
    books = Book.query.filter_by(user_id=current_user.id).all()  # Filter by current user
    for book in books:
        if not book.uid:
            # Generate a uid if missing
            book.uid = secrets.token_urlsafe(6)
            db.session.commit()
    return jsonify([book.to_dict() for book in books]), 200

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
    # This now serves the library functionality as the homepage
    search = request.args.get('search', '').strip()
    category = request.args.get('category', '').strip()
    publisher = request.args.get('publisher', '').strip()
    language = request.args.get('language', '').strip()
    
    # Start with all user's books
    query = Book.query.filter_by(user_id=current_user.id)
    
    # Apply search filter
    if search:
        search_filter = or_(
            Book.title.ilike(f'%{search}%'),
            Book.author.ilike(f'%{search}%'),
            Book.description.ilike(f'%{search}%'),
            Book.categories.ilike(f'%{search}%'),
            Book.publisher.ilike(f'%{search}%')
        )
        query = query.filter(search_filter)
    
    # Apply category filter
    if category:
        query = query.filter(Book.categories.ilike(f'%{category}%'))
    
    # Apply publisher filter
    if publisher:
        query = query.filter(Book.publisher == publisher)
    
    # Apply language filter
    if language:
        query = query.filter(Book.language == language)
    
    # Get filtered books
    all_filtered_books = query.all()
    
    # Sort books by reading status priority
    def get_sort_priority(book):
        # Currently Reading = 1 (highest priority)
        if not book.finish_date and not book.want_to_read and not book.library_only:
            return 1
        # Want to Read = 2
        elif book.want_to_read:
            return 2
        # Finished = 3
        elif book.finish_date:
            return 3
        # Library Only = 4 (lowest priority)
        else:
            return 4
    
    # Sort by priority first, then by title
    books = sorted(all_filtered_books, key=lambda book: (get_sort_priority(book), book.title.lower()))
    
    # Get all books for filter options (unfiltered)
    all_books = Book.query.filter_by(user_id=current_user.id).all()
    
    # Extract unique values for filters
    categories = sorted(set([
        cat.strip() for book in all_books 
        if book.categories 
        for cat in book.categories.split(',')
    ]))
    
    publishers = sorted(set([
        book.publisher for book in all_books 
        if book.publisher and book.publisher.strip()
    ]))
    
    languages = sorted(set([
        book.language for book in all_books 
        if book.language and book.language.strip()
    ]))
    
    return render_template('library.html',
                         books=books,
                         categories=categories,
                         publishers=publishers,
                         languages=languages,
                         current_search=search,
                         current_category=category,
                         current_publisher=publisher,
                         current_language=language)

@bp.route('/add', methods=['GET', 'POST'])
@login_required
def add_book():
    book_data = None
    if request.method == 'POST':
        if 'fetch' in request.form:
            # Fetch book data using the provided ISBN
            isbn = request.form['isbn'].strip()
            if not isbn:
                flash('Error: ISBN is required to fetch book data.', 'danger')
                return render_template('add_book.html', book_data=None)

            book_data = fetch_book_data(isbn)
            if not book_data:
                flash('No book data found for the provided ISBN.', 'warning')
            else:
                # Overwrite cover with Google Books if available
                google_cover = get_google_books_cover(isbn)
                if google_cover:
                    book_data['cover'] = google_cover

            # Re-render the form with fetched data
            return render_template('add_book.html', book_data=book_data)

        elif 'add' in request.form:
            # Validate required fields
            title = request.form['title'].strip()
            if not title:
                flash('Error: Title is required to add a book.', 'danger')
                return render_template('add_book.html', book_data=None)

            isbn = request.form['isbn']
            # Check for duplicate ISBN
            if Book.query.filter_by(isbn=isbn, user_id=current_user.id).first():
                flash('A book with this ISBN already exists.', 'danger')
                return render_template('add_book.html', book_data=None)

            author = request.form['author']
            start_date_str = request.form.get('start_date') or None
            finish_date_str = request.form.get('finish_date') or None
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
            finish_date = datetime.strptime(finish_date_str, '%Y-%m-%d').date() if finish_date_str else None
            want_to_read = 'want_to_read' in request.form
            library_only = 'library_only' in request.form

            cover_url = get_google_books_cover(isbn)

            # Get additional metadata from Google Books API first, then fallback to OpenLibrary
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
                    average_rating = None
                    rating_count = None
                else:
                    description = published_date = page_count = categories = publisher = language = average_rating = rating_count = None

            if not cover_url:
                flash('Warning: No cover image found on Google Books for this ISBN. A default image will be used. A manual cover URL can be added in the "edit book" section.', 'warning')

            book = Book(
                title=title,
                author=author,
                isbn=isbn,
                user_id=current_user.id,  # Add user_id for multi-user support
                start_date=start_date,
                finish_date=finish_date,
                cover_url=cover_url,
                want_to_read=want_to_read,
                library_only=library_only,
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
            flash(f'Book "{title}" added successfully.', 'success')
            return redirect(url_for('main.index'))

    return render_template('add_book.html', book_data=book_data)

@bp.route('/book/<uid>', methods=['GET', 'POST'])
@login_required
def view_book(uid):
    book = Book.query.filter_by(uid=uid, user_id=current_user.id).first_or_404()
    cover_url = book.cover_url  # Use the saved cover_url
    if request.method == 'POST':
        # Update start/finish dates
        start_date_str = request.form.get('start_date')
        finish_date_str = request.form.get('finish_date')
        book.start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
        book.finish_date = datetime.strptime(finish_date_str, '%Y-%m-%d').date() if finish_date_str else None
        db.session.commit()
        flash('Book dates updated.')
        return redirect(url_for('main.view_book', uid=book.uid))
    return render_template('view_book.html', book=book, cover_url=cover_url)

@bp.route('/book/<uid>/log', methods=['POST'])
@login_required
def log_reading(uid):
    book = Book.query.filter_by(uid=uid, user_id=current_user.id).first_or_404()
    log_date_str = request.form.get('log_date')
    
    # Use configured timezone instead of naive date.today()
    timezone = pytz.timezone(current_app.config.get('TIMEZONE', 'UTC'))
    
    if log_date_str:
        log_date = datetime.strptime(log_date_str, '%Y-%m-%d').date()
    else:
        # Get today's date in the configured timezone
        now_tz = datetime.now(timezone)
        log_date = now_tz.date()
    
    existing_log = ReadingLog.query.filter_by(book_id=book.id, date=log_date).first()
    if existing_log:
        flash('You have already logged reading for this day.')
    else:
        log = ReadingLog(book_id=book.id, date=log_date, user_id=current_user.id)
        db.session.add(log)
        db.session.commit()
        flash('Reading day logged.')
    return redirect(url_for('main.view_book', uid=book.uid))

@bp.route('/book/<uid>/delete', methods=['POST'])
@login_required
def delete_book(uid):
    book = Book.query.filter_by(uid=uid, user_id=current_user.id).first_or_404()
    ReadingLog.query.filter_by(book_id=book.id).delete()
    db.session.delete(book)
    db.session.commit()
    flash('Book deleted successfully.')
    return redirect(url_for('main.index'))

@bp.route('/book/<uid>/toggle_finished', methods=['POST'])
@login_required
def toggle_finished(uid):
    book = Book.query.filter_by(uid=uid, user_id=current_user.id).first_or_404()
    
    # Use configured timezone for consistent date handling
    timezone = pytz.timezone(current_app.config.get('TIMEZONE', 'UTC'))
    
    if book.finish_date:
        book.finish_date = None
        flash('Book marked as currently reading.')
    else:
        # Get today's date in the configured timezone
        now_tz = datetime.now(timezone)
        book.finish_date = now_tz.date()
        flash('Book marked as finished.')
    db.session.commit()
    return redirect(url_for('main.view_book', uid=book.uid))

@bp.route('/book/<uid>/start_reading', methods=['POST'])
@login_required
def start_reading(uid):
    book = Book.query.filter_by(uid=uid, user_id=current_user.id).first_or_404()
    book.want_to_read = False
    if not book.start_date:
        # Use configured timezone
        timezone = pytz.timezone(current_app.config.get('TIMEZONE', 'UTC'))
        now_tz = datetime.now(timezone)
        book.start_date = now_tz.date()
    db.session.commit()
    flash(f'Started reading "{book.title}".')
    return redirect(url_for('main.index'))

@bp.route('/book/<uid>/update_status', methods=['POST'])
@login_required
def update_status(uid):
    book = Book.query.filter_by(uid=uid, user_id=current_user.id).first_or_404()
    # Set status based on checkboxes
    book.want_to_read = 'want_to_read' in request.form
    book.library_only = 'library_only' in request.form
    finished = 'finished' in request.form
    currently_reading = 'currently_reading' in request.form

    # Use configured timezone
    timezone = pytz.timezone(current_app.config.get('TIMEZONE', 'UTC'))
    now_tz = datetime.now(timezone)

    if finished:
        book.finish_date = now_tz.date()
        book.want_to_read = False
        book.library_only = False
    elif currently_reading:
        book.finish_date = None
        book.want_to_read = False
        book.library_only = False
        if not book.start_date:
            book.start_date = now_tz.date()
    elif book.want_to_read:
        book.finish_date = None
        book.library_only = False
    elif book.library_only:
        book.finish_date = None
        book.want_to_read = False

    db.session.commit()
    flash('Book status updated.')
    return redirect(url_for('main.view_book', uid=book.uid))

@bp.route('/search', methods=['GET', 'POST'])
@login_required
def search_books():
    results = []
    query = ""
    if request.method == 'POST':
        query = request.form.get('query', '')
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
    return render_template('search_books.html', results=results, query=query)

@bp.route('/library')
@login_required
def library():
    # Get filter parameters from URL
    category_filter = request.args.get('category', '')
    publisher_filter = request.args.get('publisher', '')
    language_filter = request.args.get('language', '')
    search_query = request.args.get('search', '')

    # Start with books belonging to current user
    books_query = Book.query.filter_by(user_id=current_user.id)

    # Apply additional filters
    if category_filter:
        books_query = books_query.filter(Book.categories.contains(category_filter))
    if publisher_filter:
        books_query = books_query.filter(Book.publisher.ilike(f'%{publisher_filter}%'))
    if language_filter:
        books_query = books_query.filter(Book.language == language_filter)
    if search_query:
        books_query = books_query.filter(
            (Book.title.ilike(f'%{search_query}%')) |
            (Book.author.ilike(f'%{search_query}%')) |
            (Book.description.ilike(f'%{search_query}%'))
        )

    books = books_query.all()

    # Get distinct values for filter dropdowns
    all_books = Book.query.filter_by(user_id=current_user.id).all()
    categories = set()
    publishers = set()
    languages = set()

    for book in all_books:
        if book.categories:
            categories.update([cat.strip() for cat in book.categories.split(',')])
        if book.publisher:
            publishers.add(book.publisher)
        if book.language:
            languages.add(book.language)

    # Fetch all users for assignment
    users = User.query.all()

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
        users=users  # Pass users to the template
    )

@bp.route('/public-library')
def public_library():
    filter_status = request.args.get('filter', 'all')
    books_query = Book.query
    
    if filter_status == 'currently_reading':
        books_query = books_query.filter(
            Book.finish_date.is_(None),
            Book.want_to_read.isnot(True),  # Handle NULL and False
            Book.library_only.isnot(True)  # Handle NULL and False
        )
    elif filter_status == 'want_to_read':
        books_query = books_query.filter(Book.want_to_read.is_(True))
    else:  # Default "Show All" case
        books_query = books_query.order_by(Book.finish_date.desc().nullslast(), Book.id.desc())
    
    books = books_query.all()
    return render_template('public_library.html', books=books, filter_status=filter_status)

@bp.route('/book/<uid>/edit', methods=['GET', 'POST'])
@login_required
def edit_book(uid):
    book = Book.query.filter_by(uid=uid, user_id=current_user.id).first_or_404()
    if request.method == 'POST':
        new_isbn = request.form['isbn']
        # Check for duplicate ISBN (excluding the current book)
        if Book.query.filter(Book.isbn == new_isbn, Book.uid != book.uid, Book.user_id == current_user.id).first():
            flash('A book with this ISBN already exists.', 'danger')
            return render_template('edit_book.html', book=book)
        book.title = request.form['title']
        book.author = request.form['author']
        book.isbn = new_isbn
        cover_url = request.form.get('cover_url', '').strip()
        book.cover_url = cover_url if cover_url else None
        
        # Update new metadata fields
        book.description = request.form.get('description', '').strip() or None
        book.published_date = request.form.get('published_date', '').strip() or None
        book.page_count = int(request.form.get('page_count')) if request.form.get('page_count', '').strip() else None
        book.publisher = request.form.get('publisher', '').strip() or None
        book.language = request.form.get('language', '').strip() or None
        book.categories = request.form.get('categories', '').strip() or None
        book.average_rating = float(request.form.get('average_rating')) if request.form.get('average_rating', '').strip() else None
        book.rating_count = int(request.form.get('rating_count')) if request.form.get('rating_count', '').strip() else None
        db.session.commit()
        flash('Book updated.', 'success')
        return redirect(url_for('main.view_book', uid=book.uid))
    return render_template('edit_book.html', book=book)

@bp.route('/month_review/<int:year>/<int:month>.jpg')
@login_required  
def month_review(year, month):
    # Query books finished in the given month/year by current user
    books = Book.query.filter(
        Book.user_id == current_user.id,
        Book.finish_date.isnot(None),
        Book.finish_date >= datetime(year, month, 1),
        Book.finish_date < (
            datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
        )
    ).all()
    
    if not books:
        # This should only be accessed if there are books (from month_wrapup)
        return "No books found", 404

    img = generate_month_review_image(books, month, year)
    buf = BytesIO()
    img.save(buf, format='JPEG')
    buf.seek(0)
    return send_file(buf, mimetype='image/jpeg', as_attachment=True, download_name=f"month_review_{year}_{month}.jpg")

@bp.route('/month_wrapup')
@login_required
def month_wrapup():
    # Get current month and year using Central America time
    tz = pytz.timezone(current_app.config.get('TIMEZONE', 'UTC'))
    now_ca = datetime.now(tz)
    year = now_ca.year
    month = now_ca.month
    
    # Check if there are books finished this month
    books = Book.query.filter(
        Book.user_id == current_user.id,
        Book.finish_date.isnot(None),
        Book.finish_date >= datetime(year, month, 1),
        Book.finish_date < (
            datetime(year + 1, 1, 1) if month == 12 else datetime(year, month + 1, 1)
        )
    ).all()
    
    if not books:
        # Show empty month template instead of redirecting to image route
        month_name = calendar.month_name[month]
        return render_template('month_wrapup_empty.html', 
                             month_name=month_name, 
                             year=year, 
                             month=month)
    else:
        # Redirect to the month review image endpoint
        return redirect(url_for('main.month_review', year=year, month=month))

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
    return redirect(url_for('main.library'))

@bp.route('/import_goodreads', methods=['POST'])
@login_required
def import_goodreads():
    from .utils import start_goodreads_import_task
    
    file = request.files.get('goodreads_csv')
    if not file or not file.filename.endswith('.csv'):
        flash('Please upload a valid Goodreads CSV file.', 'danger')
        return redirect(url_for('main.add_book'))

    csv_content = file.stream.read().decode('utf-8')
    task = start_goodreads_import_task(csv_content, current_user.id)
    
    flash('Goodreads import started in background. You can monitor the progress below.', 'info')
    return redirect(url_for('main.task_status', task_id=task.id))

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

@bp.route('/bulk_import', methods=['GET', 'POST'])
@login_required
def bulk_import():
    if request.method == 'POST':
        if 'csv_file' not in request.files:
            flash('No file part', 'danger')
            return redirect(request.url)
        file = request.files['csv_file']
        if file.filename == '':
            flash('No selected file', 'danger')
            return redirect(request.url)
        if file and file.filename.endswith('.csv'):
            try:
                # Read CSV file
                csv_file = csv.reader(file.stream.read().decode("utf-8").splitlines())
                default_status = request.form.get('default_status', 'library_only')
                imported_count = 0
                failed_count = 0
                failed_isbns = []

                for row in csv_file:
                    if not row:  # Skip empty rows
                        continue
                    isbn = row[0].strip()
                    if not isbn: # Skip rows with empty ISBN
                        continue

                    # Check if book already exists
                    if Book.get_book_by_isbn(isbn):
                        failed_count += 1
                        failed_isbns.append(f"{isbn} (already exists)")
                        continue

                    book_data = fetch_book_data(isbn)
                    if not book_data:
                        google_book_data = get_google_books_cover(isbn, fetch_title_author=True)
                        if google_book_data and google_book_data.get('title') and google_book_data.get('author'):
                            book_data = google_book_data
                        else:
                            failed_count += 1
                            failed_isbns.append(f"{isbn} (data not found)")
                            continue
                    else:
                        # Enhance OpenLibrary data with Google Books data
                        google_data = get_google_books_cover(isbn, fetch_title_author=True)
                        if google_data:
                            for key, value in google_data.items():
                                if value and not book_data.get(key):
                                    book_data[key] = value
                    
                    title = book_data.get('title')
                    author = book_data.get('author')
                    cover_url = book_data.get('cover') or get_google_books_cover(isbn)
                    description = book_data.get('description')
                    published_date = book_data.get('published_date')
                    page_count = book_data.get('page_count')
                    categories = book_data.get('categories')
                    publisher = book_data.get('publisher')
                    language = book_data.get('language')
                    average_rating = book_data.get('average_rating')
                    rating_count = book_data.get('rating_count')


                    if not title or not author:
                        failed_count += 1
                        failed_isbns.append(f"{isbn} (missing title/author)")
                        continue

                    want_to_read = default_status == 'want_to_read'
                    library_only = default_status == 'library_only'
                    start_date = date.today() if default_status == 'reading' else None

                    new_book = Book(
                        title=title,
                        author=author,
                        isbn=isbn,
                        user_id=current_user.id,  # Add user_id for multi-user support
                        cover_url=cover_url,
                        want_to_read=want_to_read,
                        library_only=library_only,
                        start_date=start_date,
                        description=description,
                        published_date=published_date,
                        page_count=page_count,
                        categories=categories,
                        publisher=publisher,
                        language=language,
                        average_rating=average_rating,
                        rating_count=rating_count
                    )
                    new_book.save()
                    imported_count += 1

                if imported_count > 0:
                    flash(f'Successfully imported {imported_count} books.', 'success')
                if failed_count > 0:
                    flash(f'Failed to import {failed_count} books: {", ".join(failed_isbns)}', 'danger')
                return redirect(url_for('main.index'))

            except Exception as e:
                current_app.logger.error(f"Error during bulk import: {e}")
                flash('An error occurred during the bulk import process. Please try again later.', 'danger')
                return redirect(request.url)
        else:
            flash('Invalid file type. Please upload a CSV file.', 'danger')
            return redirect(request.url)

    return render_template('bulk_import.html')

@bp.route('/community_activity')
@login_required
def community_activity():
    """Show activity from users who have enabled activity sharing"""
    
    # Get users who share their reading activity
    sharing_users = User.query.filter_by(share_reading_activity=True, is_active=True).all()
    
    # Recent books (books finished in the last 30 days)
    recent_finished_books = Book.query.join(User).filter(
        User.share_reading_activity == True,
        User.is_active == True,
        Book.finish_date.isnot(None),
        Book.finish_date >= (datetime.now().date() - timedelta(days=30))
    ).order_by(Book.finish_date.desc()).limit(20).all()
    
    # Recent reading logs (from last 7 days)
    recent_logs = ReadingLog.query.join(User).filter(
        User.share_reading_activity == True,
        User.is_active == True,
        ReadingLog.date >= (datetime.now().date() - timedelta(days=7))
    ).order_by(ReadingLog.date.desc()).limit(50).all()
    
    # Currently reading books from sharing users
    currently_reading = Book.query.join(User).filter(
        User.share_current_reading == True,
        User.is_active == True,
        Book.start_date.isnot(None),
        Book.finish_date.is_(None)
    ).order_by(Book.start_date.desc()).limit(20).all()
    
    # Get some statistics
    total_books_this_month = Book.query.join(User).filter(
        User.share_reading_activity == True,
        User.is_active == True,
        Book.finish_date.isnot(None),
        Book.finish_date >= datetime.now().date().replace(day=1)
    ).count()
    
    total_active_readers = len(sharing_users)
    
    return render_template('community_activity.html',
                         recent_finished_books=recent_finished_books,
                         recent_logs=recent_logs,
                         currently_reading=currently_reading,
                         total_books_this_month=total_books_this_month,
                         total_active_readers=total_active_readers,
                         sharing_users=sharing_users)

@bp.route('/community_activity/active_readers')
@login_required
def community_active_readers():
    """Show list of active readers"""
    sharing_users = User.query.filter_by(share_reading_activity=True, is_active=True).all()
    
    # Get stats for each user
    user_stats = []
    for user in sharing_users:
        books_this_month = Book.query.filter(
            Book.user_id == user.id,
            Book.finish_date.isnot(None),
            Book.finish_date >= datetime.now().date().replace(day=1)
        ).count()
        
        total_books = Book.query.filter(
            Book.user_id == user.id,
            Book.finish_date.isnot(None)
        ).count()
        
        currently_reading_count = Book.query.filter(
            Book.user_id == user.id,
            Book.start_date.isnot(None),
            Book.finish_date.is_(None)
        ).count()
        
        user_stats.append({
            'user': user,
            'books_this_month': books_this_month,
            'total_books': total_books,
            'currently_reading': currently_reading_count
        })
    
    # Sort by activity (books this month + currently reading)
    user_stats.sort(key=lambda x: x['books_this_month'] + x['currently_reading'], reverse=True)
    
    return render_template('community_stats/active_readers.html', user_stats=user_stats)

@bp.route('/community_activity/books_this_month')
@login_required
def community_books_this_month():
    """Show books finished this month"""
    books = Book.query.join(User).filter(
        User.share_reading_activity == True,
        User.is_active == True,
        Book.finish_date.isnot(None),
        Book.finish_date >= datetime.now().date().replace(day=1)
    ).order_by(Book.finish_date.desc()).all()
    
    month_name = calendar.month_name[datetime.now().month]
    return render_template('community_stats/books_this_month.html', 
                         books=books, 
                         month_name=month_name,
                         year=datetime.now().year)

@bp.route('/community_activity/currently_reading')
@login_required
def community_currently_reading():
    """Show books currently being read"""
    books = Book.query.join(User).filter(
        User.share_current_reading == True,
        User.is_active == True,
        Book.start_date.isnot(None),
        Book.finish_date.is_(None)
    ).order_by(Book.start_date.desc()).all()
    
    return render_template('community_stats/currently_reading.html', books=books)

@bp.route('/community_activity/recent_activity')
@login_required
def community_recent_activity():
    """Show recent reading activity"""
    recent_logs = ReadingLog.query.join(User).filter(
        User.share_reading_activity == True,
        User.is_active == True,
        ReadingLog.date >= (datetime.now().date() - timedelta(days=7))
    ).order_by(ReadingLog.date.desc()).limit(50).all()
    
    return render_template('community_stats/recent_activity.html', recent_logs=recent_logs)

@bp.route('/user/<int:user_id>/profile')
@login_required
def user_profile(user_id):
    """Show public profile for a user if they're sharing"""
    user = User.query.get_or_404(user_id)
    
    # Check if user allows profile viewing
    if not user.share_reading_activity:
        flash('This user has not enabled profile sharing.', 'warning')
        return redirect(url_for('main.community_activity'))
    
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

@bp.route('/tasks')
@login_required
def list_tasks():
    """List user's background tasks"""
    from .models import Task
    
    tasks = Task.query.filter_by(user_id=current_user.id).order_by(Task.created_at.desc()).limit(20).all()
    return render_template('task_list.html', tasks=tasks)

@bp.route('/task/<task_id>')
@login_required
def task_status(task_id):
    """Show task status page"""
    from .models import Task
    
    task = Task.query.filter_by(id=task_id, user_id=current_user.id).first_or_404()
    return render_template('task_status.html', task=task)

@bp.route('/api/task/<task_id>')
@login_required
def api_task_status(task_id):
    """API endpoint for task status updates"""
    from .models import Task
    
    task = Task.query.filter_by(id=task_id, user_id=current_user.id).first_or_404()
    
    return {
        'id': task.id,
        'name': task.name,
        'status': task.status,
        'progress': task.progress,
        'total_items': task.total_items,
        'processed_items': task.processed_items,
        'success_count': task.success_count,
        'error_count': task.error_count,
        'current_item': task.current_item,
        'error_message': task.error_message,
        'result': task.result
    }