# Miscellaneous routes migrated from the original routes.py
from flask import Blueprint, redirect, url_for, jsonify
from flask_login import login_required

misc_bp = Blueprint('misc', __name__)

@misc_bp.route('/health')
def health_check():
    """Health check endpoint for monitoring and testing."""
    return jsonify({'status': 'healthy'}), 200

@misc_bp.route('/api/api-limits')
@login_required
def api_limits_status():
    """Current usage/limits for Google Books and OpenLibrary."""
    from app.utils.api_limits import get_status
    return jsonify(get_status())

@misc_bp.route('/api/google-books-key', methods=['GET', 'POST'])
@login_required
def google_books_key():
    """View/save/test the Google Books API key (admin only)."""
    from flask import request
    from flask_login import current_user
    from app.utils import google_books_settings as gbs
    if not getattr(current_user, 'is_admin', False):
        return jsonify({'ok': False, 'error': 'admin_required'}), 403
    if request.method == 'GET':
        key = gbs.get_api_key()
        return jsonify({'ok': True, 'configured': bool(key), 'masked': gbs.mask(key) if key else '', 'source': gbs.key_source()})
    payload = request.get_json(silent=True) or {}
    key = (payload.get('api_key') or '').strip()
    if payload.get('clear'):
        gbs.save(api_key='')
        return jsonify({'ok': True, 'configured': bool(gbs.get_api_key()), 'source': gbs.key_source()})
    if not key:
        return jsonify({'ok': False, 'error': 'No key provided'}), 400
    result = test_google_key(key)
    if result['status'] == 'invalid':
        return jsonify({'ok': False, 'error': result['message']}), 400
    gbs.save(api_key=key)
    try:
        from app.utils.api_limits import clear_block
        clear_block()
    except Exception:
        pass
    return jsonify({'ok': True, 'configured': True, 'masked': gbs.mask(key), 'source': 'settings', 'test': result})


def test_google_key(key: str) -> dict:
    """Try the key against Google Books. status: valid | invalid | unverified."""
    import requests
    try:
        r = requests.get('https://www.googleapis.com/books/v1/volumes',
                         params={'q': 'isbn:9780441172719', 'key': key, 'maxResults': 1}, timeout=10)
    except Exception as e:
        return {'status': 'unverified', 'message': f'Could not reach Google Books ({e.__class__.__name__}); key saved unverified.'}
    if r.status_code == 200:
        return {'status': 'valid', 'message': 'Key works.'}
    if r.headers.get('X-MyBibliotheca-Blocked'):
        return {'status': 'unverified', 'message': 'Google calls are currently paused locally; key saved unverified.'}
    body = (r.text or '')[:500]
    if r.status_code in (400, 403) and ('API key' in body or 'API_KEY' in body or 'keyInvalid' in body or 'not valid' in body or 'blocked' in body):
        return {'status': 'invalid', 'message': 'Google rejected this API key. Check it and that the Books API is enabled for its project.'}
    if r.status_code == 429:
        return {'status': 'unverified', 'message': 'Key accepted but Google reports the quota is used up; it will resume when the quota resets.'}
    return {'status': 'unverified', 'message': f'Unexpected response from Google ({r.status_code}); key saved unverified.'}


@misc_bp.route('/fetch_book/<isbn>')
def fetch_book(isbn):
    # Temporary compatibility redirect to book blueprint
    from app.routes.book_routes import fetch_book as book_fetch
    return book_fetch(isbn)

@misc_bp.route('/reading_history', methods=['GET'])
@login_required
def reading_history_redirect():
    # Redirect to new stats reading history endpoint
    return redirect(url_for('stats.reading_history'))

@misc_bp.route('/month_wrapup')
@login_required
def month_wrapup():
    # Redirect to stats page
    return redirect(url_for('stats.index'))

@misc_bp.route('/community_activity')
@login_required
def community_activity():
    # Redirect to stats page
    return redirect(url_for('stats.index'))

@misc_bp.route('/bulk_import', methods=['GET', 'POST'])
@login_required
def bulk_import():
    # Redirect to new import interface
    return redirect(url_for('import.import_books'))

@misc_bp.route('/import-books', methods=['GET', 'POST'])
@login_required
def import_books():
    # Legacy import-books route
    return redirect(url_for('import.import_books'))

@misc_bp.route('/import-books/execute', methods=['POST'])
@login_required
def import_books_execute():
    # Legacy import execute
    from app.routes.import_routes import import_books_execute as execute
    return execute()

@misc_bp.route('/import-books/progress/<task_id>')
@login_required
def import_books_progress(task_id):
    # Legacy import progress
    return redirect(url_for('import.import_books_progress', task_id=task_id))

@misc_bp.route('/api/import/progress/<task_id>')
@login_required
def api_import_progress(task_id):
    # Legacy API import progress
    from app.routes.import_routes import api_import_progress as api_prog
    return api_prog(task_id)

@misc_bp.route('/api/import/errors/<task_id>')
@login_required
def api_import_errors(task_id):
    # Legacy API import errors
    from app.routes.import_routes import api_import_errors as api_err
    return api_err(task_id)

@misc_bp.route('/debug/import-jobs')
@login_required
def debug_import_jobs():
    # Legacy debug import jobs
    from app.routes.import_routes import debug_import_jobs as debug_jobs
    return debug_jobs()

@misc_bp.route('/migrate-sqlite', methods=['GET', 'POST'])
@login_required
def migrate_sqlite():
    # Legacy migrate sqlite route
    from app.routes.import_routes import migrate_sqlite as migrate
    return migrate()

@misc_bp.route('/migration-results')
@login_required
def migration_results():
    # Legacy migration results
    return redirect(url_for('import.migration_results'))

@misc_bp.route('/detect-sqlite', methods=['POST'])
@login_required
def detect_sqlite():
    # Legacy detect sqlite endpoint
    from app.routes.import_routes import detect_sqlite as detect
    return detect()

@misc_bp.route('/direct_import', methods=['GET', 'POST'])
@login_required
def direct_import():
    # Legacy direct import route
    from app.routes.import_routes import direct_import as direct_import_bp
    return direct_import_bp()
