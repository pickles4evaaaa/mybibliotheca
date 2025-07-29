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
    Get timeline data for the Library Journey visualization.
    
    Query Parameters:
    - sort_by: 'date_added', 'start_date', 'finish_date', 'published_date' (default: 'date_added')
    - start_date: Filter books from this date (YYYY-MM-DD)
    - end_date: Filter books to this date (YYYY-MM-DD)
    - reading_status: Filter by reading status ('read', 'reading', 'plan_to_read', etc.)
    - category: Filter by category name
    """
    try:
        # Get query parameters
        sort_by = request.args.get('sort_by', 'date_added')
        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')
        reading_status_filter = request.args.get('reading_status')
        category_filter = request.args.get('category')
        
        # Parse date filters
        start_date = None
        end_date = None
        if start_date_str:
            try:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            except ValueError:
                return jsonify({'error': 'Invalid start_date format. Use YYYY-MM-DD'}), 400
                
        if end_date_str:
            try:
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            except ValueError:
                return jsonify({'error': 'Invalid end_date format. Use YYYY-MM-DD'}), 400
        
        # Get user's books
        user_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
        
        logger.info(f"Retrieved {len(user_books) if user_books else 0} books for user {current_user.id}")
        
        if not user_books:
            return jsonify({
                'timeline_data': [],
                'metadata': {
                    'total_books': 0,
                    'sort_by': sort_by,
                    'date_range': None,
                    'filters_applied': {}
                }
            })
        
        # Debug: Log first book structure
        if user_books:
            first_book = user_books[0]
            logger.info(f"First book structure - Type: {type(first_book)}")
            if hasattr(first_book, '__dict__'):
                logger.info(f"First book attributes: {list(first_book.__dict__.keys())}")
            elif isinstance(first_book, dict):
                logger.info(f"First book keys: {list(first_book.keys())}")
                
            # Debug contributors structure
            if isinstance(first_book, dict) and 'contributors' in first_book:
                contributors = first_book['contributors']
                logger.info(f"Contributors type: {type(contributors)}")
                logger.info(f"Contributors content: {contributors}")
                if contributors and len(contributors) > 0:
                    logger.info(f"First contributor type: {type(contributors[0])}")
                    logger.info(f"First contributor content: {contributors[0]}")
        
        # Process books for timeline
        timeline_books = []
        filters_applied = {}
        
        for book in user_books:
            try:
                # Extract relevant data - handle both dict and object formats
                if isinstance(book, dict):
                    book_data = {
                        'id': book.get('id', ''),
                        'title': book.get('title', 'Unknown Title'),
                        'cover_url': book.get('cover_url', None),
                        'reading_status': book.get('reading_status', None),
                        'user_rating': book.get('user_rating', None),
                        'page_count': book.get('page_count', None),
                        'categories': book.get('categories', []),
                        'series': book.get('series', None),
                        'personal_notes': book.get('personal_notes', None),
                    }
                    
                    # Extract authors from contributors array for dict format
                    contributors = book.get('contributors', [])
                    if contributors:
                        author_names = []
                        for contributor in contributors:
                            try:
                                # Handle BookContribution objects embedded in dict
                                if hasattr(contributor, 'contribution_type') and hasattr(contributor, 'person'):
                                    contrib_type = getattr(contributor, 'contribution_type', None)
                                    if contrib_type:
                                        contrib_type_str = str(contrib_type).lower()
                                        if 'author' in contrib_type_str or contrib_type_str == 'authored':
                                            person = getattr(contributor, 'person', None)
                                            if person and hasattr(person, 'name'):
                                                author_names.append(person.name)
                                # Handle dict format as fallback
                                elif isinstance(contributor, dict):
                                    contrib_type = contributor.get('contribution_type', contributor.get('role', ''))
                                    if str(contrib_type).lower() == 'authored':
                                        # Try to get person name from different possible structures
                                        person = contributor.get('person', {})
                                        if isinstance(person, dict):
                                            name = person.get('name', person.get('full_name', ''))
                                            if name:
                                                author_names.append(name)
                                        elif isinstance(person, str):
                                            author_names.append(person)
                                        else:
                                            # Fallback to direct name field
                                            name = contributor.get('name', contributor.get('author_name', ''))
                                            if name:
                                                author_names.append(name)
                                else:
                                    # Object format
                                    if hasattr(contributor, 'contribution_type'):
                                        contrib_type = getattr(contributor, 'contribution_type', None)
                                        if contrib_type and str(contrib_type).lower() == 'authored':
                                            person = getattr(contributor, 'person', None)
                                            if person and hasattr(person, 'name'):
                                                author_names.append(person.name)
                            except Exception as contrib_error:
                                logger.warning(f"Error processing contributor for book {book.get('title', 'Unknown')}: {contrib_error}")
                                continue
                        
                        book_data['author'] = ', '.join([name for name in author_names if name]) if author_names else 'Unknown Author'
                    else:
                        book_data['author'] = book.get('author', 'Unknown Author')
                        
                else:
                    # Handle object format
                    book_data = {
                        'id': getattr(book, 'id', ''),
                        'title': getattr(book, 'title', 'Unknown Title'),
                        'cover_url': getattr(book, 'cover_url', None),
                        'reading_status': getattr(book, 'reading_status', None),
                        'user_rating': getattr(book, 'user_rating', None),
                        'page_count': getattr(book, 'page_count', None),
                        'categories': getattr(book, 'categories', []),
                        'series': getattr(book, 'series', None),
                        'personal_notes': getattr(book, 'personal_notes', None),
                    }
                    
                    # Extract authors from contributors for object format
                    contributors = getattr(book, 'contributors', [])
                    if contributors:
                        author_names = []
                        for contributor in contributors:
                            try:
                                # Handle different contributor formats
                                if hasattr(contributor, 'contribution_type'):
                                    contrib_type = getattr(contributor, 'contribution_type', None)
                                    if contrib_type:
                                        # Handle enum values - check both the enum and its string value
                                        contrib_type_str = str(contrib_type).lower()
                                        if 'author' in contrib_type_str or contrib_type_str == 'authored':
                                            person = getattr(contributor, 'person', None)
                                            if person and hasattr(person, 'name'):
                                                author_names.append(person.name)
                                elif isinstance(contributor, dict):
                                    # Dict format - check both contribution_type and role
                                    contrib_type = contributor.get('contribution_type', contributor.get('role', ''))
                                    if str(contrib_type).lower() in ['authored', 'author']:
                                        person = contributor.get('person', {})
                                        if isinstance(person, dict):
                                            name = person.get('name', person.get('full_name', ''))
                                            if name:
                                                author_names.append(name)
                                        elif isinstance(person, str):
                                            author_names.append(person)
                                        else:
                                            name = contributor.get('name', contributor.get('author_name', ''))
                                            if name:
                                                author_names.append(name)
                            except Exception as contrib_error:
                                logger.warning(f"Error processing contributor for book {getattr(book, 'title', 'Unknown')}: {contrib_error}")
                                continue
                        
                        book_data['author'] = ', '.join([name for name in author_names if name]) if author_names else 'Unknown Author'
                    else:
                        book_data['author'] = getattr(book, 'author', 'Unknown Author')
            
            except Exception as book_error:
                logger.error(f"Error processing book: {book_error}")
                continue
            
            # Add date fields with better handling
            dates = {}
            for date_field in ['date_added', 'start_date', 'finish_date', 'published_date']:
                if isinstance(book, dict):
                    date_value = book.get(date_field, None)
                else:
                    date_value = getattr(book, date_field, None)
                
                if date_value:
                    if isinstance(date_value, datetime):
                        dates[date_field] = date_value.date()
                    elif isinstance(date_value, date):
                        dates[date_field] = date_value
                    elif isinstance(date_value, str):
                        try:
                            # Try to parse string dates
                            if len(date_value) == 4 and date_value.isdigit():
                                # Year only
                                dates[date_field] = datetime.strptime(f"{date_value}-01-01", '%Y-%m-%d').date()
                            else:
                                dates[date_field] = datetime.strptime(date_value[:10], '%Y-%m-%d').date()
                        except ValueError:
                            logger.warning(f"Could not parse date {date_field}: {date_value}")
                            dates[date_field] = None
                    else:
                        dates[date_field] = None
                else:
                    dates[date_field] = None
            
            book_data.update(dates)
            
            # Apply filters
            include_book = True
            
            # Ensure we have a valid timeline date for sorting
            sort_date = dates.get(sort_by)
            if not sort_date and sort_by == 'date_added':
                # If no date_added, use current date as fallback
                sort_date = date.today()
                dates['date_added'] = sort_date
                book_data['date_added'] = sort_date
            elif not sort_date and sort_by in ['start_date', 'finish_date']:
                # If we're sorting by reading dates but the book doesn't have them, exclude it
                include_book = False
            elif not sort_date and sort_by == 'published_date':
                # For published date, we can still include the book but it will sort to the beginning
                sort_date = date(1900, 1, 1)  # Very old date for books without publication info
                dates['published_date'] = sort_date
                book_data['published_date'] = sort_date
            
            # Date range filter (based on sort_by field)
            if sort_date and include_book:
                if start_date and sort_date < start_date:
                    include_book = False
                if end_date and sort_date > end_date:
                    include_book = False
            
            # Reading status filter
            if reading_status_filter and book_data['reading_status'] != reading_status_filter:
                include_book = False
                
            # Category filter (simple string matching for now)
            if category_filter:
                book_categories = book_data.get('categories', [])
                category_names = []
                if isinstance(book_categories, list):
                    for cat in book_categories:
                        if isinstance(cat, dict):
                            category_names.append(cat.get('name', ''))
                        elif hasattr(cat, 'name'):
                            category_names.append(cat.name)
                        else:
                            category_names.append(str(cat))
                
                if not any(category_filter.lower() in cat_name.lower() for cat_name in category_names):
                    include_book = False
            
            if include_book:
                # Set the primary sort date for timeline positioning
                book_data['timeline_date'] = sort_date
                timeline_books.append(book_data)
        
        # Sort books by the chosen date field
        timeline_books.sort(key=lambda x: x['timeline_date'] or date.min)
        
        # Remove books without the sort date (except for date_added which should always exist)
        if sort_by != 'date_added':
            timeline_books = [book for book in timeline_books if book['timeline_date'] is not None]
        
        # Calculate metadata
        valid_dates = [book['timeline_date'] for book in timeline_books if book['timeline_date']]
        date_range = None
        if valid_dates:
            date_range = {
                'start': min(valid_dates).isoformat(),
                'end': max(valid_dates).isoformat()
            }
        
        # Track applied filters
        if reading_status_filter:
            filters_applied['reading_status'] = reading_status_filter
        if category_filter:
            filters_applied['category'] = category_filter
        if start_date:
            filters_applied['start_date'] = start_date.isoformat()
        if end_date:
            filters_applied['end_date'] = end_date.isoformat()
        
        # Convert dates to strings for JSON serialization
        for book in timeline_books:
            for date_field in ['date_added', 'start_date', 'finish_date', 'published_date', 'timeline_date']:
                if book.get(date_field):
                    book[date_field] = book[date_field].isoformat()
        
        return jsonify({
            'timeline_data': timeline_books,
            'metadata': {
                'total_books': len(timeline_books),
                'sort_by': sort_by,
                'date_range': date_range,
                'filters_applied': filters_applied
            }
        })
        
    except Exception as e:
        logger.error(f"Error generating library journey timeline: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500


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