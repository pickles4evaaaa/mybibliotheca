from flask import Blueprint, jsonify, request
from flask_login import login_required, current_user
from app.services import book_service
from datetime import datetime, date
import logging

logger = logging.getLogger(__name__)

api_bp = Blueprint('api', __name__, url_prefix='/api')

@api_bp.route('/timeline/library-journey', methods=['GET'])
@login_required
def library_journey_timeline():
    """
    Get raw book data for Library Journey Timeline.
    All filtering and processing is done client-side for better performance and reliability.
    """
    try:
        # Get user's books
        user_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
        
        if not user_books:
            return jsonify({
                'books': [],
                'total_count': 0
            })
        
        # Transform books into a clean, consistent format
        timeline_books = []
        
        for book in user_books:
            try:
                # Extract basic book data with robust fallbacks
                book_data = {
                    'id': _get_value(book, 'id', ''),
                    'title': _get_value(book, 'title', 'Unknown Title'),
                    'cover_url': _get_value(book, 'cover_url', None),
                    'reading_status': _get_value(book, 'reading_status', None),
                    'user_rating': _get_value(book, 'user_rating', None),
                    'page_count': _get_value(book, 'page_count', None),
                    'personal_notes': _get_value(book, 'personal_notes', None),
                }
                
                # Extract authors with simplified, robust logic
                book_data['authors'] = _extract_authors(book)
                
                # Extract categories
                book_data['categories'] = _extract_categories(book)
                
                # Extract series information
                series = _get_value(book, 'series', None)
                if series:
                    book_data['series'] = _get_value(series, 'name', str(series)) if hasattr(series, 'name') else str(series)
                else:
                    book_data['series'] = None
                
                # Extract and normalize all date fields
                for date_field in ['date_added', 'start_date', 'finish_date', 'published_date']:
                    book_data[date_field] = _extract_date(book, date_field)
                
                timeline_books.append(book_data)
                
            except Exception as book_error:
                logger.warning(f"Error processing book {_get_value(book, 'title', 'Unknown')}: {book_error}")
                continue
        
        logger.info(f"Successfully processed {len(timeline_books)} books for user {current_user.id}")
        
        return jsonify({
            'books': timeline_books,
            'total_count': len(timeline_books)
        })
        
    except Exception as e:
        logger.error(f"Error generating library journey timeline: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


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
        # Fallback to direct author field
        author = _get_value(book, 'author', None)
        return [author] if author else ['Unknown Author']
    
    author_names = []
    for contributor in contributors:
        try:
            # Check if this is an author/authored contribution
            contrib_type = _get_value(contributor, 'contribution_type', '')
            contrib_type_str = str(contrib_type).lower()
            
            if 'author' in contrib_type_str or contrib_type_str == 'authored':
                # Get the person's name
                person = _get_value(contributor, 'person', None)
                if person:
                    name = _get_value(person, 'name', None)
                    if name:
                        author_names.append(name)
                else:
                    # Fallback to direct name fields on contributor
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


def _extract_date(book, date_field):
    """Extract and normalize date fields to ISO strings."""
    date_value = _get_value(book, date_field, None)
    
    if not date_value:
        return None
    
    try:
        if isinstance(date_value, datetime):
            return date_value.date().isoformat()
        elif isinstance(date_value, date):
            return date_value.isoformat()
        elif isinstance(date_value, str):
            # Handle various string formats
            date_value = date_value.strip()
            if len(date_value) == 4 and date_value.isdigit():
                # Year only - use January 1st
                return f"{date_value}-01-01"
            elif len(date_value) >= 10:
                # Full date string - extract first 10 chars (YYYY-MM-DD)
                parsed_date = datetime.strptime(date_value[:10], '%Y-%m-%d')
                return parsed_date.date().isoformat()
        
        return None
    except (ValueError, AttributeError):
        return None


@api_bp.route('/timeline/reading-patterns', methods=['GET'])
@login_required 
def reading_patterns():
    """
    Get reading pattern data for advanced timeline visualizations.
    Returns data about reading habits, genre evolution, discovery paths, etc.
    """
    try:
        user_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
        
        if not user_books:
            return jsonify({
                'genre_evolution': [],
                'reading_pace': [],
                'discovery_paths': [],
                'reading_streaks': []
            })
        
        # Analyze genre evolution over time
        genre_evolution = []
        books_by_finish_date = []
        
        for book in user_books:
            finish_date = getattr(book, 'finish_date', None)
            if finish_date:
                if isinstance(finish_date, datetime):
                    finish_date = finish_date.date()
                elif isinstance(finish_date, str):
                    try:
                        finish_date = datetime.strptime(finish_date[:10], '%Y-%m-%d').date()
                    except ValueError:
                        continue
                
                categories = getattr(book, 'categories', [])
                category_names = []
                for cat in categories:
                    if isinstance(cat, dict):
                        category_names.append(cat.get('name', ''))
                    elif hasattr(cat, 'name'):
                        category_names.append(cat.name)
                
                books_by_finish_date.append({
                    'date': finish_date,
                    'title': getattr(book, 'title', ''),
                    'categories': category_names
                })
        
        # Sort by finish date
        books_by_finish_date.sort(key=lambda x: x['date'])
        
        # Calculate reading pace (books per month)
        reading_pace = []
        if books_by_finish_date:
            current_month = None
            month_count = 0
            
            for book in books_by_finish_date:
                book_month = book['date'].replace(day=1)
                
                if current_month is None:
                    current_month = book_month
                    month_count = 1
                elif book_month == current_month:
                    month_count += 1
                else:
                    reading_pace.append({
                        'month': current_month.isoformat(),
                        'books_read': month_count
                    })
                    current_month = book_month
                    month_count = 1
            
            # Add the last month
            if current_month:
                reading_pace.append({
                    'month': current_month.isoformat(),
                    'books_read': month_count
                })
        
        return jsonify({
            'genre_evolution': books_by_finish_date[:50],  # Limit for performance
            'reading_pace': reading_pace,
            'discovery_paths': [],  # TODO: Implement discovery path analysis
            'reading_streaks': []   # TODO: Implement streak analysis
        })
        
    except Exception as e:
        logger.error(f"Error generating reading patterns: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500