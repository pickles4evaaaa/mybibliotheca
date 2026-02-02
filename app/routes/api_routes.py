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

# ---------------------------------------------------------------------------
# FINAL VERSION: Robust gegen String/Date Fehler
# ---------------------------------------------------------------------------
@api_bp.route('/widget/finished-books', methods=['GET'])
@login_required
def finished_books_widget():
    # Lokale Imports (sicher ist sicher)
    from flask import request
    import datetime as dt_mod 

    # 1. Parameter aus URL lesen
    start_str = request.args.get('start_date')
    end_str = request.args.get('end_date')

    # Filter-Datum parsen
    filter_start = None
    filter_end = None

    try:
        if start_str and start_str.strip():
            filter_start = dt_mod.datetime.strptime(start_str, '%Y-%m-%d').date()
        if end_str and end_str.strip():
            filter_end = dt_mod.datetime.strptime(end_str, '%Y-%m-%d').date()
    except:
        pass # Bei Fehlern im Filter einfach alles anzeigen

    # 2. Buecher laden
    user_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
    finished_books = []
    
    for book in user_books:
        if _get_value(book, 'reading_status', '') != 'read':
            continue

        # Datum holen
        raw_date = _extract_date(book, 'finish_date')
        if not raw_date:
            raw_date = _extract_date(book, 'date_added')
        
        if not raw_date:
            continue

        # --- REPARATUR: Datum normalisieren ---
        # Wir stellen sicher, dass f_date IMMER ein echtes Date-Objekt ist
        f_date = None
        
        # Fall A: Es ist schon ein Datum (datetime.date)
        if isinstance(raw_date, dt_mod.date):
            f_date = raw_date
        # Fall B: Es ist ein Datum mit Zeit (datetime.datetime)
        elif isinstance(raw_date, dt_mod.datetime):
            f_date = raw_date.date()
        # Fall C: Es ist ein Text/String (DER FEHLERVERURSACHER!)
        elif isinstance(raw_date, str):
            try:
                # Wir nehmen die ersten 10 Zeichen (YYYY-MM-DD) und parsen sie
                # Das hilft auch, falls da "2025-01-01 14:00:00" steht
                clean_str = str(raw_date)[:10]
                f_date = dt_mod.datetime.strptime(clean_str, '%Y-%m-%d').date()
            except:
                continue # Wenn der Text Müll ist, überspringen
        
        if not f_date:
            continue

        # --- JETZT IST DER VERGLEICH SICHER ---
        if filter_start and f_date < filter_start:
            continue
        if filter_end and f_date > filter_end:
            continue

        # --- AUDIO ERKENNUNG ---
        is_audio = False
        m_type = str(_get_value(book, 'media_type', '')).lower()
        
        if 'audio' in m_type or 'hörbuch' in m_type:
            is_audio = True
        else:
            cats = _extract_categories(book)
            cat_str = " ".join(cats).lower()
            if 'audio' in cat_str or 'hörbuch' in cat_str:
                is_audio = True

        book_data = {
            'id': _get_value(book, 'id', ''),
            'title': _get_value(book, 'title', 'Unknown'),
            'authors': _extract_authors(book),
            'cover_url': _get_value(book, 'cover_url', ''),
            'date': f_date, # Hier senden wir das saubere Objekt
            # NEU: Das Rating hinzufügen (Default auf 0, falls leer)
            'rating': _get_value(book, 'user_rating', 0),
            'type': 'audio' if is_audio else 'book'
        }
        
        finished_books.append(book_data)

    # 3. Sortierung (Aufsteigend / Älteste zuerst)
    finished_books.sort(key=lambda x: x['date'])
    
    return jsonify(finished_books)