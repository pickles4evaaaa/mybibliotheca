"""
Admin routes for debug system management.
"""

from flask import Blueprint, render_template, request, flash, redirect, url_for, jsonify
from flask_login import login_required, current_user
from datetime import datetime, timedelta

from app.debug_system import get_debug_manager, debug_log
from app.utils.audiobookshelf_settings import load_abs_settings, save_abs_settings
from app.services.audiobookshelf_sync_runner import get_abs_sync_runner
from app.services.audiobookshelf_service import get_client_from_settings
from app.services.audiobookshelf_listening_sync import AudiobookshelfListeningSync

bp = Blueprint('debug_admin', __name__, url_prefix='/auth/debug')


def admin_required(f):
    """Decorator to require admin access."""
    from functools import wraps
    
    @wraps(f)
    def decorated_function(*args, **kwargs):
        debug_manager = get_debug_manager()
        if not debug_manager.is_user_admin(current_user):
            flash('Admin access required.', 'error')
            return redirect(url_for('main.dashboard'))
        return f(*args, **kwargs)
    return decorated_function


@bp.route('/')
@login_required
@admin_required
def debug_dashboard():
    """Debug management dashboard."""
    debug_manager = get_debug_manager()
    status = debug_manager.get_debug_status()
    
    # Get recent logs
    recent_logs = debug_manager.get_debug_logs(limit=50)
    # ABS listening debug setting
    abs_settings = load_abs_settings()
    abs_debug = bool(abs_settings.get('debug_listening_sync'))
    
    return render_template('admin/debug_dashboard.html', 
                         debug_status=status, 
                         recent_logs=recent_logs,
                         abs_debug_listening=abs_debug)


@bp.route('/toggle', methods=['POST'])
@login_required
@admin_required
def toggle_debug_mode():
    """Toggle debug mode on/off."""
    debug_manager = get_debug_manager()
    
    action = request.form.get('action')
    user_id = str(current_user.id)
    
    if action == 'enable':
        if debug_manager.enable_debug_mode(user_id):
            flash('Debug mode enabled successfully.', 'success')
        else:
            flash('Failed to enable debug mode.', 'error')
    elif action == 'disable':
        if debug_manager.disable_debug_mode(user_id):
            flash('Debug mode disabled successfully.', 'success')
        else:
            flash('Failed to disable debug mode.', 'error')
    else:
        flash('Invalid action.', 'error')
    
    return redirect(url_for('debug_admin.debug_dashboard'))


@bp.route('/abs/toggle-listening-debug', methods=['POST'])
@login_required
@admin_required
def toggle_abs_listening_debug():
    """Toggle ABS listening sync debug flag stored in settings JSON."""
    action = request.form.get('action')
    settings = load_abs_settings()
    enabled = bool(settings.get('debug_listening_sync'))
    if action == 'enable':
        enabled = True
    elif action == 'disable':
        enabled = False
    else:
        # flip if no explicit action
        enabled = not enabled
    ok = save_abs_settings({'debug_listening_sync': enabled})
    if ok:
        flash(f"ABS listening debug {'enabled' if enabled else 'disabled' }.", 'success')
    else:
        flash('Failed to update ABS listening debug setting.', 'error')
    return redirect(url_for('debug_admin.debug_dashboard'))


@bp.route('/abs/run-listening-sync', methods=['POST'])
@login_required
@admin_required
def run_abs_listening_sync_now():
    """Enqueue a listening sessions sync job for the current user via background runner."""
    page_size = request.form.get('page_size')
    try:
        ps = int(page_size) if page_size else 200
    except Exception:
        ps = 200
    try:
        runner = get_abs_sync_runner()
        task_id = runner.enqueue_listening_sync(str(current_user.id), page_size=ps)
        flash(f"Listening sync started (task {task_id}). See Import Progress for details.", 'info')
    except Exception as e:
        flash(f"Failed to start listening sync: {e}", 'error')
    return redirect(url_for('debug_admin.debug_dashboard'))


@bp.route('/logs')
@login_required
@admin_required
def view_logs():
    """View debug logs."""
    debug_manager = get_debug_manager()
    
    # Get date from query parameter
    date_str = request.args.get('date', datetime.utcnow().strftime('%Y-%m-%d'))
    limit = int(request.args.get('limit', 100))
    category = request.args.get('category', '')
    
    logs = debug_manager.get_debug_logs(date_str, limit)
    
    # Filter by category if specified
    if category:
        logs = [log for log in logs if log.get('category', '').lower() == category.lower()]
    
    # Get available categories
    categories = set()
    for log in logs:
        categories.add(log.get('category', 'GENERAL'))
    
    return render_template('admin/debug_logs.html', 
                         logs=logs, 
                         current_date=date_str,
                         categories=sorted(categories),
                         selected_category=category)


@bp.route('/abs/probe', methods=['GET'])
@login_required
@admin_required
def abs_probe():
    """Probe ABS listening sessions using configured API and return a compact JSON diagnostic.

    Query params (optional):
      page_size: int (default 5)
      page: int (default 0)
      updated_after: str
      user_id: str (ABS user ID; if omitted uses /api/me)
    """
    try:
        ps = int(request.args.get('page_size', 5))
        pg = int(request.args.get('page', 0))
    except Exception:
        ps, pg = 5, 0
    updated_after = request.args.get('updated_after') or None
    user_id = request.args.get('user_id') or ''
    settings = load_abs_settings()
    client = get_client_from_settings(settings)
    if not client:
        return jsonify({'ok': False, 'error': 'abs_not_configured'}), 400
    # Resolve user id via /api/me if not supplied
    if not user_id:
        me = client.get_me()
        if me.get('ok'):
            m = me.get('user') or {}
            if isinstance(m, dict):
                user_id = str(m.get('id') or m.get('_id') or m.get('userId') or '')
    # Try primary sessions
    res = client.list_user_sessions(user_id=user_id or None, updated_after=updated_after, limit=ps, page=pg)
    sessions = res.get('sessions') or []
    total = res.get('total') or 0
    detail = {}
    if sessions:
        first = sessions[0]
        if isinstance(first, dict):
            item = first.get('item') or first.get('libraryItem') or {}
            li_id = first.get('libraryItemId') or first.get('itemId') or item.get('id') or item.get('_id')
            pos_ms = first.get('positionMs') or first.get('currentTimeMs')
            pos_sec = first.get('positionSec') or first.get('position_seconds') or first.get('currentTime')
            upd = first.get('updatedAt') or first.get('updated_at')
            detail = {
                'first_session_keys': sorted(list(first.keys())),
                'libraryItemId': li_id,
                'positionMs': pos_ms,
                'positionSec': pos_sec,
                'updatedAt': upd
            }
    return jsonify({
        'ok': True,
        'count': len(sessions),
        'total': total,
        'page': pg,
        'page_size': ps,
        'user_id': user_id or None,
        'updated_after': updated_after,
        'detail': detail
    })


@bp.route('/abs/sync-item', methods=['POST'])
@login_required
@admin_required
def abs_sync_single_item():
    """Admin helper: sync progress for a single ABS item id for the current user.

    Body: { item_id: string }
    Returns JSON with the sync result for diagnosis.
    """
    try:
        data = request.get_json(silent=True) or {}
        item_id = (data.get('item_id') or '').strip()
        if not item_id:
            return jsonify({'ok': False, 'message': 'missing item_id'}), 400
        settings = load_abs_settings()
        client = get_client_from_settings(settings)
        if not client:
            return jsonify({'ok': False, 'message': 'ABS not configured'}), 400
        listener = AudiobookshelfListeningSync(str(current_user.id), client)
        res = listener.sync_item_progress(item_id)
        return jsonify(res)
    except Exception as e:
        return jsonify({'ok': False, 'message': str(e)}), 500


@bp.route('/logs/api')
@login_required
@admin_required
def logs_api():
    """API endpoint for debug logs."""
    debug_manager = get_debug_manager()
    
    date_str = request.args.get('date', datetime.utcnow().strftime('%Y-%m-%d'))
    limit = int(request.args.get('limit', 100))
    category = request.args.get('category', '')
    
    logs = debug_manager.get_debug_logs(date_str, limit)
    
    if category:
        logs = [log for log in logs if log.get('category', '').lower() == category.lower()]
    
    return jsonify({
        'logs': logs,
        'total': len(logs),
        'date': date_str
    })


@bp.route('/status/api')
@login_required
@admin_required
def status_api():
    """API endpoint for debug status."""
    debug_manager = get_debug_manager()
    status = debug_manager.get_debug_status()
    
    return jsonify(status)


@bp.route('/test', methods=['POST'])
@login_required
@admin_required
def test_debug():
    """Test debug logging."""
    debug_manager = get_debug_manager()
    
    test_message = request.form.get('message', 'Test debug message')
    category = request.form.get('category', 'TEST')
    
    debug_manager.log_debug(
        test_message, 
        category=category,
        extra_data={
            'test_user': current_user.id,
            'test_timestamp': datetime.utcnow().isoformat(),
            'test_data': {'sample': 'value', 'number': 42}
        }
    )
    
    flash(f'Test debug message logged: {test_message}', 'info')
    return redirect(url_for('debug_admin.debug_dashboard'))
