from flask import Blueprint, jsonify, render_template, current_app
from flask_login import login_required, current_user
from app.services import book_service, reading_log_service, user_service
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
        # Get all users and filter those with sharing enabled
        all_users = user_service.get_all_users_sync()
        sharing_users = [user for user in all_users if getattr(user, 'share_reading_activity', False)]
        total_active_readers = len(sharing_users)
        
        # Get community books data
        month_start_comm = datetime.now().date().replace(day=1)
        month_finished_books = []
        
        for user in sharing_users:
            user_books = book_service.get_all_books_with_user_overlay_sync(str(user.id))
            if not user_books:
                continue
                
            for book in user_books:
                finish_date = getattr(book, 'finish_date', None)
                if finish_date:
                    # Handle datetime conversion
                    if isinstance(finish_date, datetime):
                        finish_date = finish_date.date()
                    
                    # Books finished this month
                    if finish_date >= month_start_comm:
                        book_data = {
                            'title': getattr(book, 'title', 'Unknown'),
                            'author': getattr(book, 'author', 'Unknown'),
                            'cover_url': getattr(book, 'cover_url', None),
                            'finish_date': getattr(book, 'finish_date', None),
                            'user': user
                        }
                        month_finished_books.append(book_data)
                    
                    # Recent finished books (last 30 days)
                    if finish_date >= (today - timedelta(days=30)):
                        recent_book_data = {
                            'title': getattr(book, 'title', 'Unknown'),
                            'author': getattr(book, 'author', 'Unknown'),
                            'cover_url': getattr(book, 'cover_url', None),
                            'finish_date': getattr(book, 'finish_date', None),
                            'user': user
                        }
                        recent_finished_books.append(recent_book_data)
                
                # Currently reading books
                elif (getattr(user, 'share_current_reading', False) and
                      getattr(book, 'start_date', None) and 
                      not getattr(book, 'want_to_read', False) and 
                      not getattr(book, 'library_only', False)):
                    book_data = {
                        'title': getattr(book, 'title', 'Unknown'),
                        'author': getattr(book, 'author', 'Unknown'),
                        'cover_url': getattr(book, 'cover_url', None),
                        'start_date': getattr(book, 'start_date', None),
                        'user': user
                    }
                    community_currently_reading.append(book_data)
        
        total_books_this_month = len(month_finished_books)
        
        # Get recent shared reading logs
        if hasattr(reading_log_service, 'get_recent_shared_logs_sync'):
            recent_logs_result = reading_log_service.get_recent_shared_logs_sync(days_back=7, limit=50)
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

@stats_bp.route('/community_stats/active-readers')
@login_required
def community_active_readers():
    """Get active readers data for community stats"""
    try:
        # Get all users and filter those with sharing enabled
        all_users = user_service.get_all_users_sync()
        sharing_users = [user for user in all_users if getattr(user, 'share_reading_activity', False)]
        
        # Calculate stats for each user
        user_stats = []
        for user in sharing_users:
            user_books = book_service.get_all_books_with_user_overlay_sync(str(user.id))
            if not user_books:
                continue
                
            # Count books this month
            month_start = datetime.now().date().replace(day=1)
            books_this_month = 0
            total_books = 0
            currently_reading = 0
            
            for book in user_books:
                finish_date = getattr(book, 'finish_date', None)
                if finish_date:
                    # Handle datetime conversion
                    if isinstance(finish_date, datetime):
                        finish_date = finish_date.date()
                    
                    total_books += 1
                    if finish_date >= month_start:
                        books_this_month += 1
                elif (not getattr(book, 'want_to_read', False) and 
                      not getattr(book, 'library_only', False)):
                    currently_reading += 1
            
            user_stats.append({
                'user': user,
                'books_this_month': books_this_month,
                'total_books': total_books,
                'currently_reading': currently_reading
            })
        
        # Sort by books this month, then total books
        user_stats.sort(key=lambda x: (x['books_this_month'], x['total_books']), reverse=True)
        
        return render_template('community_stats/active_readers.html', user_stats=user_stats)
    except Exception as e:
        current_app.logger.error(f"Error loading active readers: {e}")
        return render_template('community_stats/active_readers.html', user_stats=[])

@stats_bp.route('/community_stats/books-this-month')
@login_required
def community_books_this_month():
    """Get books finished this month by community"""
    try:
        # Get all users and filter those with sharing enabled
        all_users = user_service.get_all_users_sync()
        sharing_users = [user for user in all_users if getattr(user, 'share_reading_activity', False)]
        
        month_start = datetime.now().date().replace(day=1)
        books_this_month = []
        
        for user in sharing_users:
            user_books = book_service.get_all_books_with_user_overlay_sync(str(user.id))
            if not user_books:
                continue
                
            for book in user_books:
                finish_date = getattr(book, 'finish_date', None)
                if finish_date:
                    # Handle both datetime and date objects
                    if isinstance(finish_date, datetime):
                        finish_date = finish_date.date()
                    
                    if finish_date >= month_start:
                        # Create book dict with user info for template
                        book_data = {
                            'title': getattr(book, 'title', 'Unknown'),
                            'author': getattr(book, 'author', 'Unknown'),
                            'cover_url': getattr(book, 'cover_url', None),
                            'finish_date': getattr(book, 'finish_date', None),
                            'user': user
                        }
                        books_this_month.append(book_data)
        
        # Sort by finish date, most recent first
        books_this_month.sort(key=lambda b: b.get('finish_date', date.min), reverse=True)
        
        return render_template('community_stats/books_this_month.html', books=books_this_month)
    except Exception as e:
        current_app.logger.error(f"Error loading books this month: {e}")
        return render_template('community_stats/books_this_month.html', books=[])

@stats_bp.route('/community_stats/currently-reading')
@login_required
def community_currently_reading():
    """Get books currently being read by community"""
    try:
        # Get all users and filter those with sharing enabled
        all_users = user_service.get_all_users_sync()
        sharing_users = [user for user in all_users if 
                        getattr(user, 'share_reading_activity', False) and 
                        getattr(user, 'share_current_reading', False)]
        
        currently_reading = []
        
        for user in sharing_users:
            user_books = book_service.get_all_books_with_user_overlay_sync(str(user.id))
            if not user_books:
                continue
                
            for book in user_books:
                # Currently reading: has start date but no finish date
                if (getattr(book, 'start_date', None) and 
                    not getattr(book, 'finish_date', None) and
                    not getattr(book, 'want_to_read', False) and 
                    not getattr(book, 'library_only', False)):
                    
                    # Create book dict with user info for template
                    book_data = {
                        'title': getattr(book, 'title', 'Unknown'),
                        'author': getattr(book, 'author', 'Unknown'),
                        'cover_url': getattr(book, 'cover_url', None),
                        'start_date': getattr(book, 'start_date', None),
                        'user': user
                    }
                    currently_reading.append(book_data)
        
        # Sort by start date, most recent first
        currently_reading.sort(key=lambda b: b.get('start_date', date.min), reverse=True)
        
        return render_template('community_stats/currently_reading.html', books=currently_reading)
    except Exception as e:
        current_app.logger.error(f"Error loading currently reading: {e}")
        return render_template('community_stats/currently_reading.html', books=[])

@stats_bp.route('/community_stats/recent-activity')
@login_required
def community_recent_activity():
    """Get recent reading activity from community"""
    try:
        # Get recent shared reading logs
        if hasattr(reading_log_service, 'get_recent_shared_logs_sync'):
            recent_logs = reading_log_service.get_recent_shared_logs_sync(days_back=7, limit=50)
            if recent_logs is None:
                recent_logs = []
        else:
            recent_logs = []
        
        return render_template('community_stats/recent_activity.html', logs=recent_logs)
    except Exception as e:
        current_app.logger.error(f"Error loading recent activity: {e}")
        return render_template('community_stats/recent_activity.html', logs=[])