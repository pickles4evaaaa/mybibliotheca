"""
Routes package initialization.
Registers all blueprint modules for the Bibliotheca application.
"""

import logging
import os
from flask import Blueprint, request

logger = logging.getLogger(__name__)

# Import all blueprint modules
from .book_routes import book_bp
from .people_routes import people_bp
from .import_routes import import_bp
from .stats_routes import stats_bp
from .misc_routes import misc_bp
from .genre_routes import genres_bp
from .reading_log_routes import reading_logs
from .genre_taxonomy_routes import genre_taxonomy_bp
from .api_routes import api_bp
from .series_routes import series_bp

# Create a main blueprint that can be registered with the app
main_bp = Blueprint('main', __name__)

# Add main routes to the main blueprint
@main_bp.route('/')
def index():
    """Main index route - redirect to library for authenticated users, login for others"""
    from flask_login import current_user
    from flask import redirect, url_for
    try:
        print("[ROUTE] Enter main.index")
    except Exception:
        pass
    
    if current_user.is_authenticated:
        try:
            print("[ROUTE] main.index: authenticated -> redirect book.library")
        except Exception:
            pass
        return redirect(url_for('book.library'))
    else:
        try:
            print("[ROUTE] main.index: anonymous -> redirect auth.login")
        except Exception:
            pass
        return redirect(url_for('auth.login'))

@main_bp.route('/api/user/books')
def api_user_books():
    """API endpoint to get user's books for the reading log modal."""
    from flask_login import login_required, current_user
    from flask import jsonify
    from app.services import book_service
    import logging
    
    @login_required
    def _api_user_books():
        logger = logging.getLogger(__name__)
        try:
            # Get user's books
            books = book_service.get_books_for_user(current_user.id, limit=1000)
            
            # Format for dropdown
            book_data = []
            for book in books:
                # Get authors string
                authors_str = ''
                if hasattr(book, 'contributors') and book.contributors:
                    author_names = [contrib.person.name for contrib in book.contributors 
                                  if contrib.contribution_type.value in ['authored', 'co_authored']]
                    authors_str = ', '.join(author_names[:3])  # Limit to 3 authors
                    if len(author_names) > 3:
                        authors_str += ' et al.'
                elif hasattr(book, 'authors') and book.authors:
                    authors_str = ', '.join([author.name for author in book.authors[:3]])
                    if len(book.authors) > 3:
                        authors_str += ' et al.'
                
                book_data.append({
                    'id': book.id,
                    'title': book.title,
                    'authors': authors_str
                })
            
            # Sort by title
            book_data.sort(key=lambda x: x['title'].lower())
            
            return jsonify({
                'status': 'success',
                'books': book_data
            })
            
        except Exception as e:
            logger.error(f"Error getting user books for API: {e}")
            return jsonify({
                'status': 'error',
                'message': 'Failed to load books'
            }), 500
    
    return _api_user_books()


@main_bp.route('/library')
def library():
    """Compatibility route for main.library - redirect to book.library"""
    from flask import redirect, url_for
    target = url_for('book.library')
    try:
        query_bytes = request.query_string or b''
        if query_bytes:
            target = f"{target}?{query_bytes.decode('utf-8', 'ignore')}"
    except Exception:
        pass
    return redirect(target)

@main_bp.route('/stats')
def stats():
    """Compatibility route for main.stats - redirect to stats.index"""
    from flask import redirect, url_for
    return redirect(url_for('stats.index'))

@main_bp.route('/people')
def people():
    """Compatibility route for main.people - redirect to people.people"""
    from flask import redirect, url_for
    return redirect(url_for('people.people'))

@main_bp.route('/import_books')
def import_books():  # legacy endpoint -> library
    from flask import redirect, url_for
    return redirect(url_for('book.library'))

@main_bp.route('/simple-import')
def simple_import_redirect():  # legacy endpoint -> library
    from flask import redirect, url_for
    return redirect(url_for('book.library'))

@main_bp.route('/add_book')
def add_book():
    """Compatibility route for main.add_book - redirect to book.add_book"""
    from flask import redirect, url_for
    return redirect(url_for('book.add_book'))

@main_bp.route('/bulk_delete_books', methods=['POST'])
def bulk_delete_books():
    """Delete multiple books selected from the library view."""
    from flask import request, flash, redirect, url_for
    from flask_login import current_user, login_required
    from app.services.kuzu_service_facade import KuzuServiceFacade
    
    # Check authentication
    if not current_user.is_authenticated:
        return redirect(url_for('auth.login'))
    
    print(f"Bulk delete route called by user {current_user.id}")
    print(f"Request method: {request.method}")
    print(f"Request form keys: {list(request.form.keys())}")
    print(f"Full form data: {dict(request.form)}")
    
    # Initialize service
    book_service = KuzuServiceFacade()
    
    # Try both possible field names
    selected_uids = request.form.getlist('selected_books')
    if not selected_uids:
        selected_uids = request.form.getlist('book_ids')
    
    # If still empty, try getting as single values
    if not selected_uids:
        single_value = request.form.get('selected_books') or request.form.get('book_ids')
        if single_value:
            # Split by comma or newline if multiple values in single field
            selected_uids = [uid.strip() for uid in single_value.replace(',', '\n').split('\n') if uid.strip()]
    
    # Filter out empty strings
    selected_uids = [uid for uid in selected_uids if uid and uid.strip()]
    
    print(f"Selected UIDs after filtering: {selected_uids}")
    
    if not selected_uids:
        print("No books selected for deletion")
        flash('No books selected for deletion.', 'warning')
        return redirect(url_for('main.library'))
    
    deleted_count = 0
    failed_count = 0
    
    for uid in selected_uids:
        try:
            print(f"Attempting to delete book {uid} for user {current_user.id}")
            # Use the regular delete_book_sync method which handles global deletion
            # if no other users have the book
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
        flash(f'Successfully deleted {deleted_count} book(s) from your library.', 'success')
        # Invalidate cached library payloads for this user
        try:
            from app.utils.simple_cache import bump_user_library_version
            bump_user_library_version(str(current_user.id))
        except Exception:
            pass
    if failed_count > 0:
        flash(f'Failed to delete {failed_count} book(s).', 'error')
    
    return redirect(url_for('main.library'))

@main_bp.route('/view_book_enhanced/<uid>')
def view_book_enhanced(uid):
    """Compatibility route for main.view_book_enhanced - redirect to book.view_book_enhanced"""
    from flask import redirect, url_for
    return redirect(url_for('book.view_book_enhanced', uid=uid))

@main_bp.route('/add_book_from_search', methods=['POST'])
def add_book_from_search():
    """Compatibility route for main.add_book_from_search - redirect to book.add_book_from_search"""
    from flask import redirect, url_for, request
    return redirect(url_for('book.add_book_from_search'), code=307)

@main_bp.route('/download_db')
def download_db():
    """Compatibility route for main.download_db - redirect to book.download_db"""
    from flask import redirect, url_for
    return redirect(url_for('book.download_db'))

@main_bp.route('/toggle_theme', methods=['POST'])
def toggle_theme():
    """Compatibility route for main.toggle_theme - redirect to people.toggle_theme"""
    from flask import redirect, url_for, request
    return redirect(url_for('people.toggle_theme'), code=307)

@main_bp.route('/detect_sqlite', methods=['POST'])
def detect_sqlite():
    """Compatibility route for main.detect_sqlite - redirect to misc.detect_sqlite"""
    from flask import redirect, url_for, request
    return redirect(url_for('misc.detect_sqlite'), code=307)

@main_bp.route('/index')
def index_alias():
    """Alias for main.index - redirect to main index"""
    from flask import redirect, url_for
    return redirect(url_for('main.index'))

# People-related compatibility routes
@main_bp.route('/add_person')
def add_person():
    """Compatibility route for main.add_person - redirect to people.add_person"""
    from flask import redirect, url_for
    return redirect(url_for('people.add_person'))

@main_bp.route('/edit_person/<person_id>')
def edit_person(person_id):
    """Compatibility route for main.edit_person - redirect to people.edit_person"""
    from flask import redirect, url_for
    return redirect(url_for('people.edit_person', person_id=person_id))

@main_bp.route('/delete_person/<person_id>', methods=['POST'])
def delete_person(person_id):
    """Compatibility route for main.delete_person - redirect to people.delete_person"""
    from flask import redirect, url_for
    return redirect(url_for('people.delete_person', person_id=person_id), code=307)

@main_bp.route('/person_details/<person_id>')
def person_details(person_id):
    """Compatibility route for main.person_details - redirect to people.person_details"""
    from flask import redirect, url_for
    return redirect(url_for('people.person_details', person_id=person_id))

@main_bp.route('/merge_persons')
def merge_persons():
    """Compatibility route for main.merge_persons - redirect to people.merge_persons"""
    from flask import redirect, url_for
    return redirect(url_for('people.merge_persons'))

@main_bp.route('/bulk_delete_persons', methods=['POST'])
def bulk_delete_persons():
    """Compatibility route for main.bulk_delete_persons - redirect to people.bulk_delete_persons"""
    from flask import redirect, url_for
    return redirect(url_for('people.bulk_delete_persons'), code=307)

# Additional book-related compatibility routes
@main_bp.route('/add_book_manual', methods=['POST'])
def add_book_manual():
    """Compatibility route for main.add_book_manual - redirect to book.add_book_manual"""
    from flask import redirect, url_for
    return redirect(url_for('book.add_book_manual'), code=307)

@main_bp.route('/add_book_from_image', methods=['POST'])
def add_book_from_image():
    """Compatibility route for main.add_book_from_image - redirect to book.add_book_from_image"""
    from flask import redirect, url_for
    return redirect(url_for('book.add_book_from_image'), code=307)

@main_bp.route('/fetch_book/<isbn>')
def fetch_book(isbn):
    """Compatibility route for main.fetch_book - redirect to book.fetch_book"""
    from flask import redirect, url_for
    return redirect(url_for('book.fetch_book', isbn=isbn))

@main_bp.route('/refresh_person_metadata/<person_id>', methods=['POST'])
def refresh_person_metadata(person_id):
    """Compatibility route for main.refresh_person_metadata - redirect to people.refresh_person_metadata"""
    from flask import redirect, url_for
    return redirect(url_for('people.refresh_person_metadata', person_id=person_id), code=307)

@main_bp.route('/direct_import')
def direct_import():
    """Compatibility route for main.direct_import - redirect to import.direct_import"""
    from flask import redirect, url_for
    return redirect(url_for('import.direct_import'))

@main_bp.route('/import_books_execute', methods=['POST'])
def import_books_execute():
    """Compatibility route for main.import_books_execute - redirect to import.import_books_execute"""
    from flask import redirect, url_for
    return redirect(url_for('import.import_books_execute'), code=307)

@main_bp.route('/public_library')
def public_library():
    """Compatibility route for main.public_library - redirect to book.public_library"""
    from flask import redirect, url_for, request
    filter_param = request.args.get('filter', 'all')
    return redirect(url_for('book.public_library', filter=filter_param))

@main_bp.route('/community_activity')
def community_activity():
    """Compatibility route for main.community_activity - redirect to stats.index"""
    from flask import redirect, url_for
    return redirect(url_for('stats.index'))

@main_bp.route('/migrate_sqlite', methods=['GET', 'POST'])
def migrate_sqlite():
    """Compatibility route for main.migrate_sqlite - redirect to import.migrate_sqlite"""
    from flask import redirect, url_for, request
    if request.method == 'POST':
        return redirect(url_for('import.migrate_sqlite'), code=307)
    else:
        return redirect(url_for('import.migrate_sqlite'))

def register_blueprints(app):
    """Register all blueprints with the Flask application."""
    
    # Register the main blueprint
    app.register_blueprint(main_bp)
    # Register miscellaneous (legacy compatibility) routes
    app.register_blueprint(misc_bp)

    # Register stats blueprint at '/stats'
    app.register_blueprint(stats_bp, url_prefix='/stats')
    
    # Register API blueprint at '/api'
    app.register_blueprint(api_bp)
    
    # Register specific feature blueprints
    app.register_blueprint(book_bp, url_prefix='/books')
    app.register_blueprint(series_bp, url_prefix='/series')
    app.register_blueprint(people_bp, url_prefix='/people')
    app.register_blueprint(import_bp, url_prefix='/import')
    # Register genres blueprint under both URL prefixes for terminology flexibility
    app.register_blueprint(genres_bp, url_prefix='/genres')
    app.register_blueprint(genres_bp, url_prefix='/categories', name='categories')
    app.register_blueprint(reading_logs, url_prefix='/reading-logs')
    # Register admin genre taxonomy routes
    app.register_blueprint(genre_taxonomy_bp, url_prefix='/admin/genre-taxonomy')
    
    # Only log route registration in debug mode
    debug_mode = os.getenv('KUZU_DEBUG', 'false').lower() == 'true'
    if debug_mode:
        logger.debug("All blueprints registered successfully")
    
    # Note: The original routes.py had routes at the root level
    # We may need to adjust URL prefixes to maintain compatibility
    # Or add URL rules to the main blueprint for backward compatibility

# For backward compatibility, we can also export the blueprints
__all__ = ['book_bp', 'people_bp', 'import_bp', 'genres_bp', 'main_bp', 'reading_logs', 'register_blueprints']
