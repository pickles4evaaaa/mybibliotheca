import calendar
import json
import logging
import re
from collections import defaultdict
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple

from flask import Blueprint, jsonify, render_template, current_app, request, flash, redirect, url_for, make_response
from flask_login import login_required, current_user

from app.services import book_service, reading_log_service, user_service
from app.utils.user_utils import calculate_reading_streak

logger = logging.getLogger(__name__)

stats_bp = Blueprint('stats', __name__)

STATUS_ALIASES = {
    'currentlyreading': 'reading',
    'currently_reading': 'reading',
    'current_reading': 'reading',
    'reading': 'reading',
    'plan_to_read': 'plan_to_read',
    'want_to_read': 'plan_to_read',
    'wishlist_reading': 'plan_to_read',
    'tbr': 'plan_to_read',
    'on_hold': 'on_hold',
    'onhold': 'on_hold',
    'paused': 'on_hold',
    'hold': 'on_hold',
    'did_not_finish': 'did_not_finish',
    'dnf': 'did_not_finish',
    'abandoned': 'did_not_finish',
    'unfinished': 'did_not_finish',
    'read': 'read',
    'finished': 'read',
    'complete': 'read',
    'completed': 'read',
    'has_read': 'read',
    'library_only': 'library_only'
}

READ_STATUSES = {'read'}
NON_FINISHED_STATUSES = {
    'plan_to_read', 'want_to_read', 'wishlist_reading', 'tbr',
    'library_only', 'on_hold', 'did_not_finish', 'dnf',
    'abandoned', 'unfinished'
}


def determine_finished_activity(status: Optional[str], finish_date: Optional[date], fallback_log_date: Optional[date]) -> Tuple[bool, Optional[date]]:
    """Determine if a book should count as finished and the date to use for stats."""
    if finish_date:
        if status and status in NON_FINISHED_STATUSES:
            return False, None
        return True, finish_date
    if status in READ_STATUSES:
        return True, fallback_log_date
    return False, None

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
                # Provide authors_text for templates
                try:
                    book_data['authors_text'] = ', '.join([a for a in book_data['authors'] if a]) if isinstance(book_data['authors'], list) else (book_data['authors'] or 'Unknown Author')
                except Exception:
                    book_data['authors_text'] = 'Unknown Author'
                
                # Extract categories
                book_data['categories'] = _extract_categories(book)
                # Provide categories_text for templates
                try:
                    book_data['categories_text'] = ', '.join(book_data['categories']) if isinstance(book_data['categories'], list) else (book_data['categories'] or '')
                except Exception:
                    book_data['categories_text'] = ''
                
                # Extract all date fields for filtering
                for date_field in ['date_added', 'start_date', 'finish_date', 'publication_date']:
                    book_data[date_field] = _extract_date(book, date_field)
                
                # Status color and display title for UI
                book_data['status_color'] = _get_status_color(book_data.get('reading_status'))
                book_data['display_title'] = (book_data['title'][:10] + ('' if len(book_data['title']) <= 10 else '')) if isinstance(book_data.get('title'), str) else 'Unknown'

                # Apply filters
                # 1. Status filter
                if status_filter and book_data['reading_status'] != status_filter:
                    continue
                    
                # 2. Date filter - Use the selected date type primarily, with transparent fallback to retain data density
                fallback_order = [date_type, 'date_added', 'finish_date', 'start_date', 'publication_date']
                # Remove duplicates while keeping order
                seen = set()
                fallback_order = [f for f in fallback_order if not (f in seen or seen.add(f))]
                target_date = None
                source_field = None
                for field in fallback_order:
                    if book_data.get(field):
                        target_date = book_data.get(field)
                        source_field = field
                        break
                if not target_date:
                    continue  # Skip only if no dates are available at all
                
                # 3. Year range filter (support year-only and YYYY-MM dates)
                try:
                    normalized_date = None
                    if isinstance(target_date, str):
                        td = target_date.strip()
                        if len(td) == 4 and td.isdigit():
                            book_year = int(td)
                            normalized_date = f"{td}-01-01"
                        elif len(td) == 7 and td[4] == '-':  # YYYY-MM
                            book_year = int(td[:4])
                            normalized_date = f"{td}-01"
                        else:
                            dt = datetime.fromisoformat(td.replace('Z', '+00:00'))
                            book_year = dt.year
                            # store date in YYYY-MM-DD
                            normalized_date = (dt.date() if hasattr(dt, 'date') else dt).isoformat()
                    else:
                        # date or datetime
                        by = getattr(target_date, 'year', None)
                        if by is None:
                            raise ValueError('Unsupported date type')
                        book_year = int(by)
                        if hasattr(target_date, 'isoformat'):
                            # if datetime, convert to date()
                            try:
                                normalized_date = target_date.date().isoformat()
                            except Exception:
                                normalized_date = target_date.isoformat()
                        else:
                            normalized_date = str(target_date)

                    if year_from and book_year < int(year_from):
                        continue
                    if year_to and book_year > int(year_to):
                        continue
                except (ValueError, TypeError):
                    continue
                
                # Use the normalized/selected date as the primary date for positioning
                book_data['timeline_date'] = normalized_date or target_date
                book_data['timeline_source'] = source_field or date_type
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

    user_id = str(current_user.id)

    # Get user books for stats calculations with global visibility
    user_books = book_service.get_all_books_with_user_overlay_sync(user_id) or []

    def _get_book_value(book, field, default=None):
        if isinstance(book, dict):
            return book.get(field, default)
        return getattr(book, field, default)

    def _parse_date_like(value):
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned:
                return None
            cleaned = cleaned.replace('Z', '+00:00')
            try:
                return datetime.fromisoformat(cleaned).date()
            except ValueError:
                try:
                    return date.fromisoformat(cleaned.split('T')[0])
                except ValueError:
                    return None
        return None

    def _normalize_status(book):
        raw_status = _get_book_value(book, 'reading_status')
        if raw_status is None:
            raw_status = _get_book_value(book, 'status')
        if raw_status in (None, '', 'null'):
            custom_blob = _get_book_value(book, 'personal_custom_fields')
            if custom_blob:
                try:
                    if isinstance(custom_blob, str):
                        parsed = json.loads(custom_blob)
                    elif isinstance(custom_blob, dict):
                        parsed = custom_blob
                    else:
                        parsed = {}
                    if isinstance(parsed, dict):
                        raw_status = parsed.get('reading_status') or parsed.get('status')
                except Exception:
                    raw_status = None
        if raw_status in (None, '', 'null'):
            book_id = _get_book_id(book)
            if book_id:
                try:
                    from app.services.personal_metadata_service import personal_metadata_service
                    metadata = personal_metadata_service.get_personal_metadata(user_id, str(book_id))
                    raw_status = metadata.get('reading_status') or metadata.get('status')
                except Exception as pm_error:
                    current_app.logger.debug(f"Stats status fallback failed for book {book_id}: {pm_error}")
        if not isinstance(raw_status, str) and raw_status is not None and hasattr(raw_status, 'value'):
            raw_status = raw_status.value
        if not isinstance(raw_status, str):
            return str(raw_status).strip().lower() if raw_status is not None else None
        cleaned = raw_status.strip().lower().replace('-', '_').replace(' ', '_')
        cleaned = cleaned.replace('__', '_')
        return STATUS_ALIASES.get(cleaned, cleaned or None)

    def _get_book_id(book):
        candidate = _get_book_value(book, 'id') or _get_book_value(book, 'uid')
        return str(candidate) if candidate is not None else None

    def get_finish_date(book):
        finish_value = _get_book_value(book, 'finish_date')
        if finish_value is None:
            return None
        return _parse_date_like(finish_value)

    # Gather reading logs to supplement completion dates
    try:
        reading_logs_recent = reading_log_service.get_user_reading_logs_sync(user_id, days_back=365, limit=2000) or []
    except Exception as log_error:
        current_app.logger.error(f"Error loading reading logs for stats overview: {log_error}")
        reading_logs_recent = []

    last_log_by_book: Dict[str, date] = {}
    for log in reading_logs_recent:
        log_date = _parse_date_like(log.get('date')) if isinstance(log, dict) else None
        if not log_date:
            continue
        log_book = log.get('book', {}) if isinstance(log, dict) else {}
        book_id = log.get('book_id') if isinstance(log, dict) else None
        if not book_id and isinstance(log_book, dict):
            book_id = log_book.get('id') or log_book.get('uid')
        if book_id:
            book_id_str = str(book_id)
            previous = last_log_by_book.get(book_id_str)
            if previous is None or log_date > previous:
                last_log_by_book[book_id_str] = log_date

    status_buckets = defaultdict(list)
    finished_book_dates: Dict[str, Optional[date]] = {}

    for idx, book in enumerate(user_books):
        book_id = _get_book_id(book)
        book_key = book_id or f"book:{idx}"
        status_normalized = _normalize_status(book)
        if status_normalized:
            status_buckets[status_normalized].append(book)
        else:
            status_buckets['unset'].append(book)

        finish_date = get_finish_date(book)
        fallback_log_date = last_log_by_book.get(book_id) if book_id else None
        finished_flag, activity_date = determine_finished_activity(status_normalized, finish_date, fallback_log_date)
        if finished_flag:
            if activity_date:
                finished_book_dates[book_key] = activity_date
            else:
                finished_book_dates.setdefault(book_key, None)

    def _count_finished_since(threshold: date) -> int:
        return sum(1 for dt in finished_book_dates.values() if dt and dt >= threshold)

    books_finished_total = len(status_buckets.get('read', []))
    books_finished_this_week = _count_finished_since(week_start)
    books_finished_this_month = _count_finished_since(month_start)
    books_finished_this_year = _count_finished_since(year_start)

    currently_reading = status_buckets.get('reading', [])
    want_to_read = status_buckets.get('plan_to_read', [])
    streak_offset = getattr(current_user, 'reading_streak_offset', 0)
    streak = calculate_reading_streak(user_id, streak_offset)

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

    # Add simple ETag and Cache-Control for the full HTML
    try:
        # Use user id and day marker for a coarse ETag; refine as needed
        uid = str(getattr(current_user, 'id', 'unknown'))
        etag = f"W/\"stats:{uid}:{today.isoformat()}\""
        if request.headers.get('If-None-Match') == etag:
            return ('', 304)
    except Exception:
        etag = None

    resp = make_response(render_template('stats.html',
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
    ))
    if etag:
        resp.headers['ETag'] = etag
    resp.headers['Cache-Control'] = 'private, max-age=60, stale-while-revalidate=120'
    return resp

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
            # Normalize common short formats
            s = date_value.strip()
            if len(s) == 4 and s.isdigit():
                # Year-only
                return f"{s}-01-01"
            if len(s) == 7 and s[4] == '-':  # YYYY-MM
                return f"{s}-01"
            # Try to parse and reformat
            try:
                parsed = datetime.fromisoformat(s.replace('Z', '+00:00'))
                return parsed.date().isoformat()
            except Exception:
                return s
        else:
            return str(date_value)
    except Exception:
        return None


def _get_book_date(book, date_type):
    """Get the appropriate date from book based on date_type."""
    date_value = None
    
    # Prefer the pre-computed timeline_date if present to honor earlier fallback logic
    timeline_date = book.get('timeline_date') if isinstance(book, dict) else None
    if timeline_date:
        s = timeline_date.strip() if isinstance(timeline_date, str) else timeline_date
        try:
            if isinstance(s, str):
                return _parse_flexible_date(s)
            # If it's already a date/datetime
            if hasattr(s, 'year'):
                return datetime(s.year, getattr(s, 'month', 1), getattr(s, 'day', 1))
        except Exception:
            # Fallback to legacy logic below if parsing fails
            pass
    
    # Primary selection based on requested type
    if date_type == 'finish_date' and book.get('finish_date'):
        date_value = book['finish_date']
    elif date_type == 'start_date' and book.get('start_date'):
        date_value = book['start_date']
    elif date_type == 'publication_date' and book.get('publication_date'):
        date_value = book['publication_date']
    elif date_type == 'date_added' and book.get('date_added'):
        date_value = book['date_added']

    # Fallback sequence if we still don't have a value:
    # 1. date_added (user context)
    # 2. publication_date (bibliographic)
    # 3. start_date (reading context)
    # 4. finish_date
    if not date_value and book.get('date_added'):
        date_value = book['date_added']
    if not date_value and book.get('publication_date'):
        date_value = book['publication_date']
    if not date_value and book.get('start_date'):
        date_value = book['start_date']
    if not date_value and book.get('finish_date'):
        date_value = book['finish_date']
    
    # Convert string dates to datetime if needed
    if isinstance(date_value, str):
        try:
            return _parse_flexible_date(date_value)
        except Exception:
            return None
    
    return date_value


def _parse_flexible_date(s: str) -> datetime:
    """Parse common date shapes to a datetime: YYYY, YYYY-MM, YYYY-MM-DD, handles 00 for month/day and slash formats."""
    if not s:
        raise ValueError('empty date string')
    s = s.strip()
    # Normalize Zulu suffix
    s = s.replace('Z', '+00:00')
    # Replace slashes with hyphens
    s2 = s.replace('/', '-')
    # If full ISO parses, use it
    try:
        return datetime.fromisoformat(s2)
    except Exception:
        pass
    # Year only
    if len(s2) == 4 and s2.isdigit():
        return datetime(int(s2), 1, 1)
    # YYYY-MM (allow 00 month)
    if re.match(r'^\d{4}-\d{2}$', s2):
        year = int(s2[:4])
        month = int(s2[5:7]) or 1
        return datetime(year, month, 1)
    # YYYY-MM-DD (allow 00 month/day)
    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})', s2)
    if m:
        year = int(m.group(1))
        month = int(m.group(2)) or 1
        day = int(m.group(3)) or 1
        # Clamp day to 28 to avoid invalid dates for Feb/April
        day = min(day, 28)
        return datetime(year, month, day)
    # As a last resort, extract first 4-digit year and use Jan 1
    m = re.search(r'(\d{4})', s2)
    if m:
        return datetime(int(m.group(1)), 1, 1)
    # Could not parse
    raise ValueError(f'unrecognized date format: {s}')


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
        user_id = str(current_user.id)
        streak_offset = getattr(current_user, 'reading_streak_offset', 0)
        try:
            global_streak = calculate_reading_streak(user_id, streak_offset)
        except Exception as streak_error:
            logger.error(f"Error calculating global reading streak: {streak_error}")
            global_streak = streak_offset if isinstance(streak_offset, int) else 0
        
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
        result = reading_log_service.get_user_reading_logs_paginated_sync(user_id, page=1, per_page=1000)
        all_logs = result.get('logs', []) if result else []
        logger.info(f"Retrieved {len(all_logs) if all_logs else 0} reading logs for user {current_user.id}")
        
        # Also get full book data with relationships for author extraction
        user_books = book_service.get_all_books_with_user_overlay_sync(user_id)
        books_by_id = {book.id if hasattr(book, 'id') else _get_value(book, 'id'): book for book in user_books} if user_books else {}
        logger.info(f"Retrieved {len(books_by_id)} full book objects for author extraction")
        
        if not all_logs:
            calendar_data = _generate_empty_calendar(year, month)
            stats = {
                'total_logs': 0,
                'month_name': calendar.month_name[month],
                'year': year,
                'active_days': 0,
                'current_streak': global_streak,
                'calendar_streak': 0,
                'intensity_level': 'None',
                'max_activity': 0,
                'avg_activity': 0,
                'weekend_activity': 0,
                'total_activity_count': 0
            }
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
        
        # Calculate current streak using calendar data while displaying global streak in UI
        calendar_streak = _calculate_current_streak(calendar_data['days'])
        
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
            'current_streak': global_streak,
            'calendar_streak': calendar_streak,
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
