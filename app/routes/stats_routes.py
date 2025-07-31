from flask import Blueprint, jsonify, render_template, current_app, request, flash, redirect, url_for
from flask_login import login_required, current_user
from app.services import book_service, reading_log_service, user_service
from datetime import datetime, date, timedelta
import logging
import calendar

logger = logging.getLogger(__name__)

stats_bp = Blueprint('stats', __name__)

# Utility for safe date conversion
def _safe_date_to_isoformat(date_obj):
    if date_obj and hasattr(date_obj, 'isoformat'):
        return date_obj.isoformat()
    return None

@stats_bp.route('/network-explorer')
@login_required
def network_explorer():
    """Display the Interactive Library Network Explorer visualization."""
    try:
        # Get user's books with all relationships
        user_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
        
        if not user_books:
            return render_template('stats/network_explorer.html', 
                                 network_data={}, 
                                 stats={'total_books': 0, 'total_authors': 0, 'total_categories': 0})
        
        # Process books into network data structure
        network_data = _build_network_data(user_books)
        
        # Calculate summary stats
        stats = {
            'total_books': len(user_books),
            'total_contributors': len(network_data.get('contributors', {})),
            'total_categories': len(network_data.get('categories', {})),
            'total_series': len(network_data.get('series', {})),
            'total_custom_fields': len(network_data.get('custom_fields', {})),
            'relationships': sum([
                len(network_data.get('contributor_relationships', [])),
                len(network_data.get('category_relationships', [])),
                len(network_data.get('series_relationships', [])),
                len(network_data.get('publisher_relationships', [])),
                len(network_data.get('custom_field_relationships', []))
            ])
        }
        
        return render_template('stats/network_explorer.html', 
                             network_data=network_data, 
                             stats=stats)
        
    except Exception as e:
        logger.error(f"Error generating network explorer: {str(e)}")
        return render_template('stats/network_explorer.html', 
                             network_data={}, 
                             stats={'total_books': 0, 'total_authors': 0, 'total_categories': 0},
                             error=str(e))

@stats_bp.route('/library-journey')
@login_required
def library_journey():
    """Display the Library Journey Timeline visualization using HTML/CSS with filtering."""
    # Get filter parameters from query string
    date_type = request.args.get('date_type', 'date_added')
    status_filter = request.args.get('status', '')
    year_from = request.args.get('year_from', '')
    year_to = request.args.get('year_to', '')
    
    try:
        
        # Get user's books using the same API as before
        user_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
        
        if not user_books:
            filters = {'date_type': date_type, 'status': status_filter, 'year_from': year_from, 'year_to': year_to}
            return render_template('stats/library_journey.html', books=[], 
                                 stats={'total_books': 0, 'date_range': 'No data', 'avg_rating': 'N/A', 'total_pages': 0},
                                 filters=filters)
        
        logger.info(f"Processing {len(user_books)} total books with filters: date_type={date_type}, status={status_filter}")
        
        # Process books for timeline positioning
        processed_books = []
        
        for book in user_books:
            try:
                # Extract basic book data with robust fallbacks
                book_data = {
                    'id': _get_value(book, 'id', ''),
                    'uid': _get_value(book, 'uid', ''),
                    'title': _get_value(book, 'title', 'Unknown Title'),
                    'cover_url': _get_value(book, 'cover_url', None),
                    'reading_status': _get_value(book, 'reading_status', None),
                    'user_rating': _get_value(book, 'user_rating', None),
                    'page_count': _get_value(book, 'page_count', None),
                    'personal_notes': _get_value(book, 'personal_notes', None),
                    'description': _get_value(book, 'description', None),
                }
                
                # Extract authors
                book_data['authors'] = _extract_authors(book)
                
                # Extract categories
                book_data['categories'] = _extract_categories(book)
                
                # Extract all date fields for filtering
                for date_field in ['date_added', 'start_date', 'finish_date', 'publication_date']:
                    book_data[date_field] = _extract_date(book, date_field)
                
                # Apply filters
                # 1. Status filter
                if status_filter and book_data['reading_status'] != status_filter:
                    continue
                    
                # 2. Date filter - use the selected date type for positioning, with fallback
                target_date = book_data.get(date_type)
                if not target_date:
                    # Fallback to date_added if the selected date type is not available
                    target_date = book_data.get('date_added')
                    if not target_date:
                        continue  # Skip only if no dates are available at all
                
                # 3. Year range filter
                try:
                    book_year = datetime.fromisoformat(target_date).year
                    if year_from and book_year < int(year_from):
                        continue
                    if year_to and book_year > int(year_to):
                        continue
                except (ValueError, TypeError):
                    continue
                
                # Use the selected date type as the primary date for positioning
                book_data['timeline_date'] = target_date
                processed_books.append(book_data)
                
            except Exception as book_error:
                logger.warning(f"Error processing book {_get_value(book, 'title', 'Unknown')}: {book_error}")
                continue
        
        # Sort by the timeline date (selected date type)
        processed_books.sort(key=lambda x: x['timeline_date'])
        
        # Check if we have any books after filtering
        if not processed_books:
            stats = {'total_books': 0, 'date_range': 'No data', 'avg_rating': 'N/A', 'total_pages': 0}
            filters = {'date_type': date_type, 'status': status_filter, 'year_from': year_from, 'year_to': year_to}
            return render_template('stats/library_journey.html', books=[], stats=stats, filters=filters)
        
        # Calculate positions for CSS positioning using timeline_date
        timeline_books = _calculate_timeline_positions(processed_books, date_type)
        
        # Calculate stats
        stats = _calculate_timeline_stats(timeline_books)
        
        logger.info(f"Successfully processed {len(timeline_books)} books for Library Journey timeline using {date_type}")
        
        # Pass filter values to template for preserving state
        filters = {
            'date_type': date_type,
            'status': status_filter,
            'year_from': year_from,
            'year_to': year_to
        }
        
        return render_template('stats/library_journey.html', books=timeline_books, stats=stats, filters=filters)
        
    except Exception as e:
        logger.error(f"Error generating Library Journey timeline: {str(e)}")
        stats = {'total_books': 0, 'date_range': 'No data', 'avg_rating': 'N/A', 'total_pages': 0}
        filters = {'date_type': date_type, 'status': status_filter, 'year_from': year_from, 'year_to': year_to}
        return render_template('stats/library_journey.html', books=[], stats=stats, filters=filters, error=str(e))

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
    
    # Use reading_status field for currently reading (handle both 'reading' and 'currently_reading')
    currently_reading = [b for b in user_books if getattr(b, 'reading_status', None) in ['reading', 'currently_reading']]
    want_to_read = [b for b in user_books if getattr(b, 'reading_status', None) == 'plan_to_read']
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
                
                # Currently reading books - use reading_status field
                elif (getattr(user, 'share_current_reading', False) and
                      getattr(book, 'reading_status', None) in ['reading', 'currently_reading']):
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

    # Get user reading log stats
    reading_log_stats = {}
    try:
        if hasattr(reading_log_service, 'get_user_all_time_reading_stats_sync'):
            reading_log_stats = reading_log_service.get_user_all_time_reading_stats_sync(str(current_user.id))
    except Exception as e:
        current_app.logger.error(f"Error loading reading log stats: {e}")
        reading_log_stats = {
            'total_log_entries': 0,
            'total_pages': 0,
            'total_minutes': 0,
            'distinct_books': 0,
            'distinct_days': 0,
            'total_time_formatted': '0m'
        }

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
        sharing_users=sharing_users,
        reading_log_stats=reading_log_stats
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
                elif getattr(book, 'reading_status', None) in ['reading', 'currently_reading']:
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
                # Currently reading: use reading_status field
                if getattr(book, 'reading_status', None) in ['reading', 'currently_reading']:
                    
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


@stats_bp.route('/reading-logs')
@login_required
def reading_logs():
    """Get paginated reading logs for the current user"""
    from flask import request
    
    try:
        # Get pagination parameters
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 25))
        
        # Validate per_page options
        valid_per_page = [10, 25, 50, 100]
        if per_page not in valid_per_page:
            per_page = 25
        
        # Get paginated logs
        result = reading_log_service.get_user_reading_logs_paginated_sync(
            str(current_user.id), page=page, per_page=per_page
        )
        
        # Get all-time stats for totals
        all_time_stats = reading_log_service.get_user_all_time_reading_stats_sync(str(current_user.id))
        
        return render_template('reading_logs/user_logs.html', 
                             logs=result['logs'],
                             pagination=result['pagination'],
                             all_time_stats=all_time_stats,
                             valid_per_page=valid_per_page)
    
    except Exception as e:
        current_app.logger.error(f"Error loading reading logs: {e}")
        return render_template('reading_logs/user_logs.html', 
                             logs=[], 
                             pagination={'page': 1, 'per_page': 25, 'total_count': 0, 'total_pages': 1, 'has_prev': False, 'has_next': False},
                             all_time_stats={'total_log_entries': 0, 'total_pages': 0, 'total_minutes': 0, 'distinct_books': 0, 'distinct_days': 0, 'total_time_formatted': '0m'},
                             valid_per_page=[10, 25, 50, 100])

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


# Helper functions for timeline processing

def _get_value(obj, key, default=None):
    """Safely get a value from either a dict or object."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    else:
        return getattr(obj, key, default)


def _extract_authors(book):
    """Extract author names with robust fallback logic."""
    contributors = _get_value(book, 'contributors', [])
    if not contributors:
        author = _get_value(book, 'author', None)
        return [author] if author else ['Unknown Author']
    
    author_names = []
    for contributor in contributors:
        try:
            contrib_type = _get_value(contributor, 'contribution_type', '')
            contrib_type_str = str(contrib_type).lower()
            
            if 'author' in contrib_type_str or contrib_type_str == 'authored':
                person = _get_value(contributor, 'person', None)
                if person:
                    name = _get_value(person, 'name', None)
                    if name:
                        author_names.append(name)
                else:
                    name = _get_value(contributor, 'name', None) or _get_value(contributor, 'author_name', None)
                    if name:
                        author_names.append(name)
        except Exception:
            continue
    
    return author_names if author_names else ['Unknown Author']


def _extract_categories(book):
    """Extract category names."""
    categories = _get_value(book, 'categories', [])
    if not categories:
        return []
    
    category_names = []
    for cat in categories:
        name = _get_value(cat, 'name', None)
        if name:
            category_names.append(name)
        elif isinstance(cat, str):
            category_names.append(cat)
    
    return category_names


def _extract_publisher_name(book):
    """Extract publisher name from book data, handling both string and object types."""
    publisher = _get_value(book, "publisher", None)
    if publisher is None:
        return None
    
    # Handle Publisher object
    if hasattr(publisher, 'name'):
        return str(publisher.name)
    
    # Handle string publisher name
    return str(publisher)

def _extract_date(book, field_name):
    """Extract and normalize date fields to ISO strings."""
    # Handle both 'publication_date' and 'published_date' for compatibility
    if field_name == 'publication_date':
        date_value = _get_value(book, 'publication_date', None) or _get_value(book, 'published_date', None)
    else:
        date_value = _get_value(book, field_name, None)
    
    if not date_value:
        return None
    
    try:
        if isinstance(date_value, datetime):
            return date_value.date().isoformat()
        elif hasattr(date_value, 'isoformat'):
            return date_value.isoformat()
        elif isinstance(date_value, str):
            # Try to parse and reformat
            try:
                parsed = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
                return parsed.date().isoformat()
            except:
                return date_value
        else:
            return str(date_value)
    except Exception:
        return None


def _get_book_date(book, date_type):
    """Get the appropriate date from book based on date_type."""
    date_value = None
    
    if date_type == 'finish_date' and book.get('finish_date'):
        date_value = book['finish_date']
    elif date_type == 'start_date' and book.get('start_date'):
        date_value = book['start_date']
    elif date_type == 'publication_date' and book.get('publication_date'):
        date_value = book['publication_date']
    elif book.get('date_added'):  # fallback to date_added
        date_value = book['date_added']
    
    # Convert string dates to datetime if needed
    if isinstance(date_value, str):
        try:
            date_value = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return None
    
    return date_value


def _calculate_timeline_positions(books, date_type):
    """Calculate positions for books on the timeline with clustering for nearby books."""
    if not books:
        return []
    
    # Convert dates to timestamps for calculation
    book_positions = []
    for book in books:
        date_value = _get_book_date(book, date_type)
        if date_value:
            timestamp = date_value.timestamp()
            book_positions.append({
                'book': book,
                'timestamp': timestamp,
                'year': date_value.year
            })
    
    if not book_positions:
        return []
    
    # Sort by timestamp
    book_positions.sort(key=lambda x: x['timestamp'])
    
    # Calculate time range
    min_timestamp = book_positions[0]['timestamp']
    max_timestamp = book_positions[-1]['timestamp']
    time_range = max_timestamp - min_timestamp
    
    if time_range == 0:
        # All books have the same date - create a single cluster
        cluster = {
            'type': 'cluster',
            'books': [item['book'] for item in book_positions],
            'x_position': 50,
            'y_position': 50,
            'timestamp': min_timestamp,
            'year': book_positions[0]['year']
        }
        return [cluster]
    
    # Group books by proximity for clustering
    clusters = []
    current_cluster_books = [book_positions[0]]
    cluster_threshold = time_range * 0.03  # 3% of total time range for clustering
    
    for i in range(1, len(book_positions)):
        time_diff = book_positions[i]['timestamp'] - current_cluster_books[-1]['timestamp']
        if time_diff <= cluster_threshold:
            current_cluster_books.append(book_positions[i])
        else:
            # Finish current cluster/individual book
            if len(current_cluster_books) >= 3:  # Cluster if 3 or more books
                avg_timestamp = sum(item['timestamp'] for item in current_cluster_books) / len(current_cluster_books)
                avg_year = sum(item['year'] for item in current_cluster_books) / len(current_cluster_books)
                clusters.append({
                    'type': 'cluster',
                    'books': [item['book'] for item in current_cluster_books],
                    'timestamp': avg_timestamp,
                    'year': int(avg_year)
                })
            else:
                # Individual books
                for item in current_cluster_books:
                    clusters.append({
                        'type': 'individual',
                        'book': item['book'],
                        'timestamp': item['timestamp'],
                        'year': item['year']
                    })
            
            current_cluster_books = [book_positions[i]]
    
    # Handle final cluster/books
    if len(current_cluster_books) >= 3:
        avg_timestamp = sum(item['timestamp'] for item in current_cluster_books) / len(current_cluster_books)
        avg_year = sum(item['year'] for item in current_cluster_books) / len(current_cluster_books)
        clusters.append({
            'type': 'cluster',
            'books': [item['book'] for item in current_cluster_books],
            'timestamp': avg_timestamp,
            'year': int(avg_year)
        })
    else:
        for item in current_cluster_books:
            clusters.append({
                'type': 'individual',
                'book': item['book'],
                'timestamp': item['timestamp'],
                'year': item['year']
            })
    
    # Calculate positions for clusters and individual books
    usable_height = 85
    top_margin = 8
    
    for i, cluster in enumerate(clusters):
        # X position based on time (10% to 90% of width)
        x_percent = 10 + ((cluster['timestamp'] - min_timestamp) / time_range) * 80
        
        # Y position with some vertical variation
        if len(clusters) == 1:
            y_percent = top_margin + usable_height * 0.5
        else:
            # Distribute vertically with some randomness based on position
            base_y = top_margin + usable_height * 0.3
            variation = (hash(str(cluster['timestamp'])) % 40) / 100  # ±20%
            y_percent = base_y + variation * usable_height * 0.4
        
        # Ensure positions stay within bounds
        cluster['x_position'] = max(5, min(95, x_percent))
        cluster['y_position'] = max(10, min(80, y_percent))
    
    return clusters


def _clean_book_data_for_json(book):
    """Clean book data to ensure safe JSON encoding in HTML attributes."""
    import html
    
    # Create a cleaned copy of the book data
    cleaned = {}
    for key, value in book.items():
        if isinstance(value, str):
            # Remove or escape problematic characters
            cleaned_value = value.replace('"', '&quot;').replace("'", '&#39;').replace('\n', ' ').replace('\r', ' ')
            # Limit length to prevent overly long attributes
            if len(cleaned_value) > 500:
                cleaned_value = cleaned_value[:500] + '...'
            cleaned[key] = cleaned_value
        elif value is None:
            cleaned[key] = ''
        else:
            cleaned[key] = value
    
    return cleaned


def _get_status_color(status):
    """Get color for reading status (same as D3.js version)."""
    status_colors = {
        'read': '#28a745',
        'reading': '#17a2b8', 
        'plan_to_read': '#ffc107',
        'on_hold': '#fd7e14',
        'did_not_finish': '#dc3545'
    }
    return status_colors.get(status, '#6c757d')


def _calculate_timeline_stats(timeline_items):
    """Calculate statistics for the timeline."""
    if not timeline_items:
        return {
            'total_books': 0,
            'date_range': '-',
            'avg_rating': '-',
            'total_pages': '-'
        }
    
    # Flatten books from clusters and individuals
    all_books = []
    all_years = []
    
    for item in timeline_items:
        if item.get('type') == 'cluster':
            all_books.extend(item['books'])
            all_years.append(item['year'])
        else:
            all_books.append(item['book'])
            all_years.append(item['year'])
    
    # Calculate stats
    total_books = len(all_books)
    
    # Date range
    date_range = f"{min(all_years)} - {max(all_years)}" if all_years else '-'
    
    # Average rating  
    ratings = [book.get('user_rating') for book in all_books if book.get('user_rating') and book.get('user_rating') > 0]
    avg_rating = f"{sum(ratings) / len(ratings):.1f}" if ratings else '-'
    
    # Total pages
    pages = [book.get('page_count') for book in all_books if book.get('page_count') and book.get('page_count') > 0]
    total_pages = f"{sum(pages):,}" if pages else '-'
    
    return {
        'total_books': total_books,
        'date_range': date_range,
        'avg_rating': avg_rating,
        'total_pages': total_pages
    }


@stats_bp.route('/reading-journey')
@login_required
def reading_journey():
    """Display reading journey calendar based on reading logs."""
    try:
        logger.info("Loading Reading Journey calendar view")
        
        # Get filter parameters
        year = request.args.get('year', type=int)
        month = request.args.get('month', type=int)
        status_filter = request.args.get('status', '')
        
        # Default to current year/month if not specified
        if not year:
            year = datetime.now().year
        if not month:
            month = datetime.now().month
            
        # Get reading logs data (use paginated method with large page size)
        result = reading_log_service.get_user_reading_logs_paginated_sync(str(current_user.id), page=1, per_page=1000)
        all_logs = result.get('logs', []) if result else []
        logger.info(f"Retrieved {len(all_logs) if all_logs else 0} reading logs for user {current_user.id}")
        
        # Also get full book data with relationships for author extraction
        user_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
        books_by_id = {book.id if hasattr(book, 'id') else _get_value(book, 'id'): book for book in user_books} if user_books else {}
        logger.info(f"Retrieved {len(books_by_id)} full book objects for author extraction")
        
        if not all_logs:
            calendar_data = _generate_empty_calendar(year, month)
            stats = {'total_logs': 0, 'month_name': calendar.month_name[month], 'year': year}
            filters = {'year': year, 'month': month, 'status': status_filter}
            return render_template('stats/reading_journey.html', 
                                calendar_data=calendar_data, 
                                stats=stats, 
                                filters=filters)
        
        # Process reading logs for calendar display
        processed_logs = []
        logger.info(f"Processing logs for {year}-{month:02d}")
        
        for log in all_logs:
            try:
                # Extract log data
                log_date_str = log.get('date')
                if not log_date_str:
                    continue
                    
                # Parse the log date
                if isinstance(log_date_str, str):
                    log_date = datetime.strptime(log_date_str, '%Y-%m-%d').date()
                elif hasattr(log_date_str, 'date'):
                    log_date = log_date_str.date()
                else:
                    log_date = log_date_str
                
                # Filter by year/month
                if log_date.year != year or log_date.month != month:
                    continue
                
                # Get book data from the log
                book_data = log.get('book', {})
                if not book_data:
                    continue
                
                # Apply status filter based on book reading status
                if status_filter and book_data.get('reading_status') != status_filter:
                    continue
                
                # Get full book object for proper author extraction
                book_id = book_data.get('id')
                full_book = books_by_id.get(book_id) if book_id else None
                
                # Extract authors - use full book object if available, otherwise fallback
                authors_text = 'Unknown Author'
                
                if full_book:
                    # Use the full book object with contributor relationships
                    try:
                        authors_list = _extract_authors(full_book)
                        if authors_list and authors_list != ['Unknown Author']:
                            authors_text = ', '.join(authors_list)
                    except Exception as e:
                        logger.debug(f"Error extracting authors from full book: {e}")
                
                # Fallback to basic book data if full book not available or author extraction failed
                if authors_text == 'Unknown Author':
                    direct_author = book_data.get('author') or book_data.get('authors_text') or book_data.get('authors')
                    if direct_author:
                        if isinstance(direct_author, list):
                            authors_text = ', '.join(direct_author)
                        else:
                            authors_text = str(direct_author)
                
                # Get the reading status from the log itself or book (prefer log status)
                reading_status = log.get('reading_status') or book_data.get('reading_status') or 'read'
                
                # Calculate progress percentage if we have page numbers
                progress_percentage = None
                pages_read = log.get('pages_read', 0)
                page_count = book_data.get('page_count', 0)
                if pages_read and page_count and pages_read > 0 and page_count > 0:
                    progress_percentage = round((pages_read / page_count) * 100, 1)
                
                # Clean and prepare the log data for display
                processed_log = {
                    'id': book_data.get('id', ''),
                    'uid': book_data.get('uid', ''),
                    'title': book_data.get('title', 'Unknown Title'),
                    'authors_text': authors_text,
                    'reading_status': reading_status,
                    'status_color': _get_status_color(reading_status),
                    'cover_url': book_data.get('cover_image_url') or book_data.get('cover_url'),
                    'user_rating': book_data.get('user_rating'),
                    'page_count': book_data.get('page_count'),
                    'categories_text': book_data.get('categories_text', ''),
                    'personal_notes': log.get('notes', ''),
                    'pages_read': pages_read,
                    'time_read': log.get('time_read', 0),
                    'start_page': log.get('start_page', 0),
                    'end_page': log.get('end_page', 0),
                    'progress_percentage': progress_percentage,
                    'log_date': log_date,
                    'day': log_date.day,
                    'display_title': book_data.get('title', 'Unknown')[:10] if book_data.get('title') else 'Unknown'
                }
                
                processed_logs.append(processed_log)
                
            except Exception as e:
                logger.warning(f"Error processing reading log: {e}")
                continue
        
        # Generate calendar with logs
        calendar_data = _generate_calendar_with_logs(year, month, processed_logs)
        
        # Calculate enhanced stats including heatmap metrics
        active_days = [day for day in calendar_data['days'] if day.get('logs') or day.get('clusters')]
        activity_counts = [day.get('activity_count', 0) for day in calendar_data['days'] if day.get('is_current_month')]
        
        # Calculate current streak
        current_streak = _calculate_current_streak(calendar_data['days'])
        
        # Determine intensity level
        max_activity = calendar_data.get('max_activity', 0)
        avg_activity = calendar_data.get('avg_activity', 0)
        
        if avg_activity == 0:
            intensity_level = 'None'
        elif avg_activity < 1:
            intensity_level = 'Low'
        elif avg_activity < 2:
            intensity_level = 'Medium'
        elif avg_activity < 3:
            intensity_level = 'High'
        else:
            intensity_level = 'Very High'
        
        stats = {
            'total_logs': len(processed_logs),
            'month_name': calendar.month_name[month],
            'year': year,
            'active_days': len(active_days),
            'current_streak': current_streak,
            'intensity_level': intensity_level,
            'max_activity': max_activity,
            'avg_activity': avg_activity,
            'weekend_activity': _calculate_weekend_activity(calendar_data['days']),
            'total_activity_count': sum(activity_counts)
        }
        
        filters = {'year': year, 'month': month, 'status': status_filter}
        
        logger.info(f"Successfully generated calendar for {calendar.month_name[month]} {year} with {len(processed_logs)} reading logs")
        
        return render_template('stats/reading_journey.html', 
                             calendar_data=calendar_data, 
                             stats=stats, 
                             filters=filters)
        
    except Exception as e:
        logger.error(f"Error in reading_journey route: {e}")
        flash('Error loading reading journey. Please try again.', 'error')
        return redirect(url_for('stats.index'))


def _generate_empty_calendar(year, month):
    """Generate empty calendar structure."""
    cal = calendar.monthcalendar(year, month)
    days = []
    
    for week in cal:
        for day in week:
            if day == 0:  # Empty day from previous/next month
                days.append({
                    'day': None,
                    'date': None,
                    'books': [],
                    'clusters': [],
                    'is_current_month': False
                })
            else:
                days.append({
                    'day': day,
                    'date': datetime(year, month, day).date(),
                    'books': [],
                    'clusters': [],
                    'is_current_month': True
                })
    
    return {
        'year': year,
        'month': month,
        'month_name': calendar.month_name[month],
        'days': days,
        'weeks': len(cal)
    }


def _generate_calendar_with_logs(year, month, logs):
    """Generate calendar with reading logs placed on appropriate days and enhanced heatmap data."""
    cal = calendar.monthcalendar(year, month)
    days = []
    
    # Group logs by day
    logs_by_day = {}
    for log in logs:
        day = log['day']
        if day not in logs_by_day:
            logs_by_day[day] = []
        logs_by_day[day].append(log)
    
    # Calculate activity intensity metrics
    all_activity_counts = []
    for day_logs in logs_by_day.values():
        all_activity_counts.append(len(day_logs))
    
    max_activity = max(all_activity_counts) if all_activity_counts else 0
    avg_activity = sum(all_activity_counts) / len(all_activity_counts) if all_activity_counts else 0
    
    # Calculate intensity thresholds
    intensity_thresholds = [
        0,
        max(1, int(max_activity * 0.25)),
        max(1, int(max_activity * 0.5)),
        max(2, int(max_activity * 0.75)),
        max_activity
    ] if max_activity > 0 else [0, 0, 0, 0, 0]
    
    for week in cal:
        for day in week:
            if day == 0:  # Empty day from previous/next month
                days.append({
                    'day': None,
                    'date': None,
                    'logs': [],
                    'clusters': [],
                    'is_current_month': False,
                    'activity_intensity': 0,
                    'day_of_week': 0,
                    'is_weekend': False,
                    'day_pattern': 'empty'
                })
            else:
                day_date = datetime(year, month, day).date()
                day_logs = logs_by_day.get(day, [])
                
                # Calculate activity intensity (0-4 scale)
                activity_count = len(day_logs)
                intensity = 0
                for i in range(1, len(intensity_thresholds)):
                    if activity_count >= intensity_thresholds[i]:
                        intensity = i
                
                # Determine day of week (0=Monday, 6=Sunday)
                day_of_week = day_date.weekday()
                is_weekend = day_of_week >= 5  # Saturday=5, Sunday=6
                
                # Determine day pattern based on activity and day type
                day_pattern = 'normal'
                if activity_count > avg_activity:
                    day_pattern = 'high_activity'
                elif is_weekend and activity_count > 0:
                    day_pattern = 'weekend_active'
                elif activity_count == 0:
                    day_pattern = 'inactive'
                
                # Separate individual logs from clusters (3+ logs = cluster)
                individual_logs = day_logs[:2] if len(day_logs) <= 2 else []
                clusters = []
                
                if len(day_logs) >= 3:
                    clusters.append({
                        'count': len(day_logs),
                        'date': day_date.isoformat(),
                        'logs': day_logs
                    })
                
                days.append({
                    'day': day,
                    'date': day_date,
                    'logs': individual_logs,
                    'clusters': clusters,
                    'is_current_month': True,
                    'activity_intensity': intensity,
                    'activity_count': activity_count,
                    'day_of_week': day_of_week,
                    'is_weekend': is_weekend,
                    'day_pattern': day_pattern
                })
    
    return {
        'year': year,
        'month': month,
        'month_name': calendar.month_name[month],
        'days': days,
        'weeks': len(cal),
        'max_activity': max_activity,
        'avg_activity': round(avg_activity, 1),
        'intensity_thresholds': intensity_thresholds
    }
    

def _calculate_current_streak(days):
    """Calculate the current reading streak (consecutive days with activity)."""
    streak = 0
    today = datetime.now().date()
    
    # Reverse the days to start from the most recent
    for day in reversed(days):
        if not day.get('is_current_month') or not day.get('day'):
            continue
            
        day_date = day.get('date')
        if isinstance(day_date, str):
            day_date = datetime.strptime(day_date, '%Y-%m-%d').date()
        elif hasattr(day_date, 'date'):
            day_date = day_date.date()
            
        # Only count days up to today
        if day_date > today:
            continue
            
        activity_count = day.get('activity_count', 0)
        if activity_count > 0:
            streak += 1
        else:
            break  # Streak is broken
            
        # Stop if we've gone too far back
        if (today - day_date).days > 30:
            break
            
    return streak


def _calculate_weekend_activity(days):
    """Calculate weekend vs weekday activity ratio."""
    weekend_activity = 0
    weekday_activity = 0
    
    for day in days:
        if not day.get('is_current_month') or not day.get('day'):
            continue
            
        activity_count = day.get('activity_count', 0)
        is_weekend = day.get('is_weekend', False)
        
        if is_weekend:
            weekend_activity += activity_count
        else:
            weekday_activity += activity_count
    
    total_activity = weekend_activity + weekday_activity
    if total_activity == 0:
        return 0
        
    return round((weekend_activity / total_activity) * 100, 1)


def _generate_calendar_with_books(year, month, books):
    """Generate calendar with books placed on appropriate days."""
    cal = calendar.monthcalendar(year, month)
    days = []
    
    # Group books by day
    books_by_day = {}
    for book in books:
        day = book['day']
        if day not in books_by_day:
            books_by_day[day] = []
        books_by_day[day].append(book)
    
    for week in cal:
        for day in week:
            if day == 0:  # Empty day from previous/next month
                days.append({
                    'day': None,
                    'date': None,
                    'books': [],
                    'clusters': [],
                    'is_current_month': False
                })
            else:
                day_books = books_by_day.get(day, [])
                
                # Apply clustering logic - if 3+ books on same day, create cluster
                if len(day_books) >= 3:
                    days.append({
                        'day': day,
                        'date': datetime(year, month, day).date(),
                        'books': [],
                        'clusters': [{
                            'type': 'cluster',
                            'count': len(day_books),
                            'books': day_books,
                            'date': datetime(year, month, day).date()
                        }],
                        'is_current_month': True
                    })
                else:
                    days.append({
                        'day': day,
                        'date': datetime(year, month, day).date(),
                        'books': day_books,
                        'clusters': [],
                        'is_current_month': True
                    })
    
    return {
        'year': year,
        'month': month,
        'month_name': calendar.month_name[month],
        'days': days,
        'weeks': len(cal)
    }



def _build_network_data(user_books):
    """Build network data structure for the Interactive Library Network Explorer."""
    import json  # For handling custom metadata that might be JSON strings
    
    # Initialize data structures
    network_data = {
        "books": {},
        "contributors": {},  # People with their specific contribution types
        "categories": {},
        "series": {},
        "publishers": {},
        "custom_fields": {},  # Custom field values as nodes
        "contributor_relationships": [],
        "category_relationships": [],
        "series_relationships": [],
        "publisher_relationships": [],
        "custom_field_relationships": []
    }
    
    # Process each book
    for book in user_books:
        book_id = _get_value(book, "uid", "") or _get_value(book, "id", "")
        if not book_id:
            continue
            
        # Extract book data
        book_data = {
            "id": book_id,
            "title": _get_value(book, "title", "Unknown Title"),
            "cover_url": _get_value(book, "cover_url", None),
            "reading_status": _get_value(book, "reading_status", None),
            "user_rating": _get_value(book, "user_rating", None),
            "page_count": _get_value(book, "page_count", None),
            "finish_date": _extract_date(book, "finish_date"),
            "date_added": _extract_date(book, "date_added"),
            "publisher": _extract_publisher_name(book),
            "series_name": _get_value(book, "series_name", None),
            "series_volume": _get_value(book, "series_volume", None)
        }
        
        # Get status color for visual encoding
        book_data["status_color"] = _get_network_status_color(book_data["reading_status"])
        
        # Store book
        network_data["books"][book_id] = book_data
        
        # Extract detailed contributors with contribution types
        contributors = _get_value(book, "contributors", [])
        if contributors:
            for contributor in contributors:
                person = _get_value(contributor, "person", None)
                contribution_type = _get_value(contributor, "contribution_type", None)
                
                if person and contribution_type:
                    person_name = _get_value(person, "name", "Unknown")
                    person_id = _get_value(person, "id", "")
                    
                    # Get contribution type value
                    if hasattr(contribution_type, 'value'):
                        contrib_type = contribution_type.value
                    else:
                        contrib_type = str(contribution_type).lower()
                    
                    # Create unique contributor node ID based on person and contribution type
                    contributor_id = f"contributor_{person_id}_{contrib_type}" if person_id else f"contributor_{person_name.replace(' ', '_').lower()}_{contrib_type}"
                    
                    if contributor_id not in network_data["contributors"]:
                        network_data["contributors"][contributor_id] = {
                            "id": contributor_id,
                            "person_id": person_id,
                            "name": person_name,
                            "contribution_type": contrib_type,
                            "book_count": 0,
                            "books": []
                        }
                    
                    network_data["contributors"][contributor_id]["book_count"] += 1
                    network_data["contributors"][contributor_id]["books"].append(book_id)
                    
                    # Create relationship
                    network_data["contributor_relationships"].append({
                        "book_id": book_id,
                        "contributor_id": contributor_id,
                        "type": contrib_type
                    })
        
        # Fallback: Extract basic authors if no detailed contributors
        if not contributors:
            authors = _extract_authors(book)
            for author in authors:
                contributor_id = f"contributor_{author.replace(' ', '_').lower()}_authored"
                if contributor_id not in network_data["contributors"]:
                    network_data["contributors"][contributor_id] = {
                        "id": contributor_id,
                        "person_id": None,
                        "name": author,
                        "contribution_type": "authored",
                        "book_count": 0,
                        "books": []
                    }
                
                network_data["contributors"][contributor_id]["book_count"] += 1
                network_data["contributors"][contributor_id]["books"].append(book_id)
                
                # Create relationship
                network_data["contributor_relationships"].append({
                    "book_id": book_id,
                    "contributor_id": contributor_id,
                    "type": "authored"
                })
        
        # Extract categories and create relationships
        categories = _extract_categories(book)
        for category in categories:
            category_id = f"category_{category.replace(' ', '_').lower()}"
            if category_id not in network_data["categories"]:
                network_data["categories"][category_id] = {
                    "id": category_id,
                    "name": category,
                    "book_count": 0,
                    "books": [],
                    "color": _get_category_color(category)
                }
            
            network_data["categories"][category_id]["book_count"] += 1
            network_data["categories"][category_id]["books"].append(book_id)
            
            # Create relationship
            network_data["category_relationships"].append({
                "book_id": book_id,
                "category_id": category_id,
                "type": "categorized_as"
            })
        
        # Extract series and create relationships
        if book_data["series_name"]:
            series_id = f"series_{book_data['series_name'].replace(' ', '_').lower()}"
            if series_id not in network_data["series"]:
                network_data["series"][series_id] = {
                    "id": series_id,
                    "name": book_data["series_name"],
                    "book_count": 0,
                    "books": []
                }
            
            network_data["series"][series_id]["book_count"] += 1
            network_data["series"][series_id]["books"].append(book_id)
            
            # Create relationship
            network_data["series_relationships"].append({
                "book_id": book_id,
                "series_id": series_id,
                "type": "part_of_series",
                "volume": book_data["series_volume"]
            })
        
        # Extract publishers and create relationships
        if book_data["publisher"]:
            # Handle case where publisher might be an object instead of string
            publisher_name = book_data["publisher"]
            if hasattr(publisher_name, 'name'):
                publisher_name = publisher_name.name
            elif not isinstance(publisher_name, str):
                publisher_name = str(publisher_name)
            
            publisher_id = f"publisher_{publisher_name.replace(' ', '_').lower()}"
            if publisher_id not in network_data["publishers"]:
                network_data["publishers"][publisher_id] = {
                    "id": publisher_id,
                    "name": publisher_name,
                    "book_count": 0,
                    "books": []
                }
            
            network_data["publishers"][publisher_id]["book_count"] += 1
            network_data["publishers"][publisher_id]["books"].append(book_id)
            
            # Create relationship
            network_data["publisher_relationships"].append({
                "book_id": book_id,
                "publisher_id": publisher_id,
                "type": "published_by"
            })
        
        # Extract custom fields and create relationships
        custom_metadata = _get_value(book, "custom_metadata", {})
        if custom_metadata:
            # Handle case where custom_metadata might be a string (JSON) instead of dict
            if isinstance(custom_metadata, str):
                try:
                    custom_metadata = json.loads(custom_metadata)
                except (json.JSONDecodeError, TypeError, NameError):
                    custom_metadata = {}
            
            # Ensure it's a dictionary before iterating
            if isinstance(custom_metadata, dict):
                for field_name, field_value in custom_metadata.items():
                    if field_value and str(field_value).strip():
                        # Clean field value for ID generation
                        clean_value = str(field_value).strip()
                        if len(clean_value) > 50:  # Truncate very long values
                            clean_value = clean_value[:50] + "..."
                        
                        custom_field_id = f"custom_{field_name}_{clean_value.replace(' ', '_').lower()}"
                        
                        if custom_field_id not in network_data["custom_fields"]:
                            network_data["custom_fields"][custom_field_id] = {
                                "id": custom_field_id,
                                "field_name": field_name,
                                "field_value": clean_value,
                                "display_name": field_name.replace('_', ' ').title(),
                                "book_count": 0,
                                "books": []
                            }
                        
                        network_data["custom_fields"][custom_field_id]["book_count"] += 1
                        network_data["custom_fields"][custom_field_id]["books"].append(book_id)
                        
                        # Create relationship
                        network_data["custom_field_relationships"].append({
                            "book_id": book_id,
                            "custom_field_id": custom_field_id,
                            "type": "has_custom_field",
                            "field_name": field_name,
                            "field_value": field_value
                        })
    
    return network_data


def _get_network_status_color(status):
    """Get color for reading status in network visualization."""
    status_colors = {
        "read": "#28a745",           # Green
        "reading": "#007bff",        # Blue  
        "plan_to_read": "#ffc107",   # Yellow
        "on_hold": "#fd7e14",        # Orange
        "did_not_finish": "#dc3545", # Red
        "library_only": "#6c757d"   # Gray
    }
    return status_colors.get(status, "#6c757d")


def _get_category_color(category_name):
    """Get color for category based on name."""
    # Simple hash-based color assignment for consistency
    import hashlib
    hash_obj = hashlib.md5(category_name.encode())
    hash_hex = hash_obj.hexdigest()
    
    # Convert first 6 characters to color, ensure good contrast
    color = f"#{hash_hex[:6]}"
    return color
