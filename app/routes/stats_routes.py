from flask import Blueprint, jsonify, render_template, current_app
from flask_login import login_required, current_user
from app.services import book_service, reading_log_service
from datetime import datetime, date, timedelta

stats_bp = Blueprint('stats', __name__)

# Utility for safe date conversion
def _safe_date_to_isoformat(date_obj):
    if date_obj and hasattr(date_obj, 'isoformat'):
        return date_obj.isoformat()
    return None

@stats_bp.route('/reading_history', methods=['GET'])
@login_required
def reading_history():
    # Moved from app/routes.py
    domain_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
    books_data = []
    for user_book in domain_books:
        book_dict = {
            'id': user_book.get('id') if isinstance(user_book, dict) else getattr(user_book, 'id', ''),
            'uid': user_book.get('uid') if isinstance(user_book, dict) else getattr(user_book, 'uid', ''),
            'title': user_book.get('title') if isinstance(user_book, dict) else getattr(user_book, 'title', ''),
            'author': user_book.get('author') if isinstance(user_book, dict) else getattr(user_book, 'author', ''),
            'isbn': user_book.get('isbn') if isinstance(user_book, dict) else getattr(user_book, 'isbn', ''),
            'description': user_book.get('description') if isinstance(user_book, dict) else getattr(user_book, 'description', ''),
            'start_date': _safe_date_to_isoformat(user_book.get('start_date') if isinstance(user_book, dict) else getattr(user_book, 'start_date', None)),
            'finish_date': _safe_date_to_isoformat(user_book.get('finish_date') if isinstance(user_book, dict) else getattr(user_book, 'finish_date', None)),
            'want_to_read': user_book.get('want_to_read') if isinstance(user_book, dict) else getattr(user_book, 'want_to_read', False),
            'library_only': user_book.get('library_only') if isinstance(user_book, dict) else getattr(user_book, 'library_only', False),
            'cover_url': user_book.get('cover_url') if isinstance(user_book, dict) else getattr(user_book, 'cover_url', None),
            'user_rating': user_book.get('user_rating') if isinstance(user_book, dict) else getattr(user_book, 'user_rating', None),
            'personal_notes': user_book.get('personal_notes') if isinstance(user_book, dict) else getattr(user_book, 'personal_notes', None),
            'status': user_book.get('status') if isinstance(user_book, dict) else getattr(user_book, 'status', None),
            'date_added': _safe_date_to_isoformat(user_book.get('date_added') if isinstance(user_book, dict) else getattr(user_book, 'date_added', None))
        }
        books_data.append(book_dict)
    return jsonify(books_data), 200

@stats_bp.route('/', methods=['GET'])
@login_required
def index():
    """Display personal and community reading statistics."""
    # Date boundaries
    today = date.today()
    week_start = today - timedelta(days=today.weekday())
    month_start = today.replace(day=1)
    year_start = today.replace(month=1, day=1)

    # Get user books for stats calculations with global visibility
    user_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))

    # Helper to safely get finish date
    def get_finish_date(book):
        finish_date = getattr(book, 'finish_date', None)
        if finish_date is None:
            return None
        if isinstance(finish_date, datetime):
            return finish_date.date()
        return finish_date

    # User stats
    books_finished_this_week = len([b for b in user_books if (fd := get_finish_date(b)) and fd >= week_start])
    books_finished_this_month = len([b for b in user_books if (fd := get_finish_date(b)) and fd >= month_start])
    books_finished_this_year = len([b for b in user_books if (fd := get_finish_date(b)) and fd >= year_start])
    books_finished_total = len([b for b in user_books if get_finish_date(b)])
    currently_reading = [b for b in user_books if not getattr(b, 'finish_date', None) and not getattr(b, 'want_to_read', False) and not getattr(b, 'library_only', False)]
    want_to_read = [b for b in user_books if getattr(b, 'want_to_read', False)]
    streak = current_user.get_reading_streak()

    # Community stats
    sharing_users = []
    recent_finished_books = []
    community_currently_reading = []
    total_books_this_month = 0
    total_active_readers = 0
    recent_logs = []  # Initialize as empty list

    try:
        # TODO: Implement community features via Kuzu
        month_finished_books = []
        # Helper for community finish date
        def get_finish_date_community(book):
            fd = getattr(book, 'finish_date', None)
            if fd is None:
                return None
            if isinstance(fd, datetime):
                return fd.date()
            return fd
        month_start_comm = datetime.now().date().replace(day=1)
        total_books_this_month = len([book for book in month_finished_books if (fd := get_finish_date_community(book)) and fd >= month_start_comm])
        total_active_readers = len(sharing_users)
        if hasattr(reading_log_service, 'get_recent_shared_logs_sync'):
            recent_logs_result = reading_log_service.get_recent_shared_logs_sync(days_back=7, limit=50)
            # Ensure recent_logs is always a list
            recent_logs = recent_logs_result if recent_logs_result is not None else []
    except Exception as e:
        current_app.logger.error(f"Error loading community stats: {e}")
        # Ensure recent_logs is always a list even on error
        recent_logs = []

    return render_template('stats.html',
        books_finished_this_week=books_finished_this_week,
        books_finished_this_month=books_finished_this_month,
        books_finished_this_year=books_finished_this_year,
        books_finished_total=books_finished_total,
        currently_reading=currently_reading,
        want_to_read=want_to_read,
        streak=streak,
        recent_finished_books=recent_finished_books,
        community_currently_reading=community_currently_reading,
        total_books_this_month=total_books_this_month,
        total_active_readers=total_active_readers,
        recent_logs=recent_logs,
        sharing_users=sharing_users
    )