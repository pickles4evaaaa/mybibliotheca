"""
Admin routes for debug system management.
"""

from flask import Blueprint, render_template, request, flash, redirect, url_for, jsonify
from flask_login import login_required, current_user
from datetime import datetime, timedelta

from app.debug_system import get_debug_manager, debug_log

bp = Blueprint('debug_admin', __name__, url_prefix='/admin/debug')


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
    
    return render_template('admin/debug_dashboard.html', 
                         debug_status=status, 
                         recent_logs=recent_logs)


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
