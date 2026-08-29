"""Admin endpoints for background book-repair operations.

The settings page is deliberately kept separate from these endpoints: a repair
can be started with a normal form POST even when the dynamically loaded panel
or its JavaScript is unavailable.
"""

from flask import Blueprint, abort, current_app, flash, jsonify, redirect, request, url_for
from flask_login import current_user, login_required


repairs_bp = Blueprint('repairs', __name__)

_SUPPORTED_REPAIR_ACTIONS = {'assign_missing_isbns', 'fetch_missing_covers'}


def _settings_repairs_redirect(**params):
    """Return to the Repairs settings panel without coupling it to this blueprint."""
    return redirect(url_for('auth.settings', section='server', panel='repairs', **params))


@repairs_bp.route('/settings/repairs/<string:action>', methods=['POST'])
@login_required
def start_repair(action: str):
    """Queue a repair job from the admin settings panel.

    Supports both ordinary form submissions and AJAX callers.  The form path
    remains a reliable fallback when a browser has stale or blocked scripts.
    """
    if not current_user.is_admin:
        abort(403)

    action = (action or '').strip().lower()
    current_app.logger.info(
        '[REPAIRS][DIRECT_REQUEST] action=%s user=%s',
        action or '<missing>', getattr(current_user, 'id', '<unknown>'),
    )
    if action not in _SUPPORTED_REPAIR_ACTIONS:
        flash('Unknown repair action.', 'warning')
        return _settings_repairs_redirect()

    try:
        from app.services.book_repair_service import start_repair_job

        job = start_repair_job(action)
        current_app.logger.info('[REPAIRS][QUEUED] action=%s job=%s', action, job.get('job_id'))
        if request.is_json or request.headers.get('X-Requested-With') == 'XMLHttpRequest' or request.headers.get('HX-Request'):
            return jsonify({'ok': True, 'job': job}), 202

        label = 'ISBN assignment' if action == 'assign_missing_isbns' else 'cover fetching'
        if job.get('status') in {'queued', 'running'}:
            flash(f'{label.capitalize()} started. Progress will appear in the Repairs panel.', 'info')
        else:
            flash('A repair is already running. Progress will appear in the Repairs panel.', 'info')
        return _settings_repairs_redirect(repair_job=job.get('job_id'))
    except Exception as err:
        current_app.logger.error('[REPAIRS][DIRECT_FAILURE] action=%s error=%s', action, err, exc_info=True)
        label = 'ISBN' if action == 'assign_missing_isbns' else 'cover'
        flash(f'Could not start the {label} repair. Check logs for details.', 'error')
        return _settings_repairs_redirect()


@repairs_bp.route('/settings/repairs/jobs/<string:job_id>', methods=['GET'])
@login_required
def repair_job_status(job_id: str):
    """Return progress for a queued ISBN or cover repair job."""
    if not current_user.is_admin:
        abort(403)

    from app.services.book_repair_service import get_repair_job

    job = get_repair_job(job_id)
    if not job:
        return jsonify({'ok': False, 'error': 'Repair job not found'}), 404
    return jsonify({'ok': True, 'job': job})
