"""Series related UI routes (list and detail pages).

Initial skeleton implementing list and detail rendering using the new
KuzuSeriesService. Enhancements (sorting modal, editing, cover upload)
will be layered on subsequently.
"""

from flask import Blueprint, render_template, request, abort, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
import logging
import uuid, time, traceback

from app.services import get_series_service
from app.utils.image_processing import process_image_from_filestorage, get_covers_dir
from pathlib import Path

series_bp = Blueprint('series', __name__, template_folder='../templates')

logger = logging.getLogger(__name__)


@series_bp.route('/')
@login_required
def list_series():
    svc = get_series_service()
    series_list = svc.get_all_series()
    try:
        from flask import current_app as _ca
        for s in series_list[:25]:  # limit log volume
            try:
                _ca.logger.debug(
                    "[SERIES][LIST_VIEW] id=%s name=%s cover=%s custom=%s gen=%s",
                    getattr(s,'id',None), getattr(s,'name',None), getattr(s,'cover_url',None), getattr(s,'custom_cover',None)
                )
            except Exception:
                pass
    except Exception:
        pass
    return render_template('series/list_series.html', series_list=series_list)


@series_bp.route('/<series_id>')
@login_required
def series_detail(series_id: str):
    svc = get_series_service()
    series_obj = svc.get_series(series_id)
    if not series_obj:
        abort(404)
    # Default sorting preference switched to 'volume' for clarity
    order = request.args.get('order', 'volume')
    books = svc.get_books_for_series(series_id, order=order)
    # Augment with contributors
    try:
        svc.add_contributors(books)
    except Exception:
        pass
    book_count = len(books)
    user_notes = svc.get_user_series_notes(str(current_user.id), series_id)  # type: ignore[attr-defined]
    return render_template('series/series_detail.html', series_obj=series_obj, books=books, order=order, book_count=book_count, user_notes=user_notes)


@series_bp.route('/<series_id>/rename', methods=['POST'])
@login_required
def rename_series(series_id: str):
    new_name = request.form.get('name', '').strip()
    if not new_name:
        flash('Series name required', 'warning')
        return redirect(url_for('series.series_detail', series_id=series_id))
    svc = get_series_service()
    if svc.update_series_name(series_id, new_name):
        flash('Series name updated', 'success')
    else:
        flash('Failed to update name', 'error')
    return redirect(url_for('series.series_detail', series_id=series_id))


@series_bp.route('/<series_id>/description', methods=['POST'])
@login_required
def update_series_description(series_id: str):
    desc = request.form.get('description', '').strip()
    svc = get_series_service()
    ok = svc.update_series_description(series_id, desc)
    # AJAX (fetch) request -> JSON response
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        if ok:
            return jsonify({'success': True, 'description': desc})
        return jsonify({'success': False, 'error': 'Failed to update description'}), 400
    if ok:
        flash('Description updated', 'success')
    else:
        flash('Failed to update description', 'error')
    return redirect(url_for('series.series_detail', series_id=series_id))


@series_bp.route('/<series_id>/notes', methods=['POST'])
@login_required
def update_series_notes(series_id: str):
    notes = request.form.get('notes', '')
    svc = get_series_service()
    ok = svc.upsert_user_series_notes(str(current_user.id), series_id, notes)  # type: ignore[attr-defined]
    if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
        if ok:
            return jsonify({'success': True, 'notes': notes})
        return jsonify({'success': False, 'error': 'Failed to save notes'}), 400
    if ok:
        flash('Notes saved', 'success')
    else:
        flash('Failed to save notes', 'error')
    return redirect(url_for('series.series_detail', series_id=series_id))


@series_bp.route('/<series_id>/upload_cover', methods=['POST'])
@login_required
def upload_series_cover(series_id: str):
    """Upload a custom series cover (mirrors book cover upload pattern)."""
    svc = get_series_service()
    series_obj = svc.get_series(series_id)
    if not series_obj:
        return jsonify({'success': False, 'error': 'Series not found'}), 404

    if 'cover_file' not in request.files:
        return jsonify({'success': False, 'error': 'No file uploaded'}), 400
    file = request.files['cover_file']
    if file.filename == '':
        return jsonify({'success': False, 'error': 'No file selected'}), 400

    allowed_ext = {'.jpg', '.jpeg', '.png', '.gif'}
    safe_name = file.filename or ''
    ext = Path(safe_name).suffix.lower()
    if ext not in allowed_ext:
        return jsonify({'success': False, 'error': 'Invalid file type'}), 400

    # Size limit 10MB
    file.seek(0, 2)
    size = file.tell()
    file.seek(0)
    if size > 10 * 1024 * 1024:
        return jsonify({'success': False, 'error': 'File too large (max 10MB).'}), 400

    # Only remove previous custom (user) cover file if replaced. We never touch inferred (book) covers.
    old_custom = getattr(series_obj, 'user_cover', None)
    try:
        new_cover_rel = process_image_from_filestorage(file)
        # Compose absolute URL if needed (consistency with books)
        abs_url = new_cover_rel
        if new_cover_rel.startswith('/'):
            abs_url = request.host_url.rstrip('/') + new_cover_rel
        svc.update_series_cover(series_id, abs_url, custom=True, generated_placeholder=False)
        # Cleanup prior custom file if local
        if old_custom and old_custom.startswith('/covers/'):
            try:
                fname = old_custom.split('/')[-1]
                old_path = get_covers_dir() / fname
                if old_path.exists():
                    old_path.unlink()
            except Exception:
                pass
        return jsonify({'success': True, 'cover_url': new_cover_rel})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


## Placeholder generation endpoint removed under minimal cover logic (user_cover or first-book cover only).

@series_bp.route('/<series_id>/clear_cover', methods=['POST'])
@login_required
def clear_series_cover(series_id: str):
    """Remove the uploaded cover and reveal the first-book fallback cover."""
    trace_id = uuid.uuid4().hex[:8]
    started = time.time()
    svc = get_series_service()
    series = svc.get_series(series_id)
    if not series:
        return jsonify({'success': False, 'error': 'Series not found', 'trace_id': trace_id}), 404

    old_cover = getattr(series, 'user_cover', None) or ''
    if not svc.update_series_cover(series_id, None, custom=False, generated_placeholder=False):
        return jsonify({'success': False, 'error': 'Could not clear series cover', 'trace_id': trace_id}), 500
    updated = svc.get_series(series_id)
    fallback_cover = getattr(updated, 'cover_url', None) if updated else None

    file_deleted = False
    file_delete_error = None
    if old_cover.startswith('/covers/'):
        try:
            old_path = get_covers_dir() / old_cover.split('/')[-1]
            if old_path.exists():
                old_path.unlink()
                file_deleted = True
        except Exception as exc:
            file_delete_error = str(exc)
            logger.exception('[SERIES][CLEAR_COVER][%s] Failed deleting old cover file', trace_id)

    elapsed_ms = (time.time() - started) * 1000.0
    return jsonify({
        'success': True,
        'cover_url': fallback_cover,
        'cleared': True,
        'trace_id': trace_id,
        'rows': 1,
        'file_deleted': file_deleted,
        'file_delete_error': file_delete_error,
        'first_row': [series_id, fallback_cover],
        'fallback_cover': fallback_cover,
        'elapsed_ms': elapsed_ms,
    })

@series_bp.route('/<series_id>/delete', methods=['POST'])
@login_required
def delete_series(series_id: str):
    """Delete a series and detach it from all books.

    This will remove the Series node and all PART_OF_SERIES relationships; books remain intact.
    """
    svc = get_series_service()
    if not svc.get_series(series_id):
        return jsonify({'success': False, 'error': 'Series not found'}), 404
    try:
        if not svc.delete_series(series_id):
            return jsonify({'success': False, 'error': 'Series could not be deleted'}), 500
        return jsonify({'success': True, 'redirect': url_for('series.list_series')})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ---------- Autocomplete / Creation API (JSON) ----------
@series_bp.route('/search')
@login_required
def search_series_api():
    """Return JSON list of matching series for typeahead.

    Params: q (query), limit (optional)
    """
    q = request.args.get('q', '').strip()
    limit_raw = request.args.get('limit', '15')
    try:
        limit = max(1, min(50, int(limit_raw)))
    except ValueError:
        limit = 15
    svc = get_series_service()
    try:
        matches = svc.search_series(q, limit=limit)
        return jsonify({'success': True, 'results': matches})
    except Exception as e:
        logger.error(f"/series/search error: {e}")
        return jsonify({'success': False, 'error': 'Search failed'}), 500


@series_bp.route('/create', methods=['POST'])
@login_required
def create_series_api():
    data = request.get_json(silent=True) or {}
    name = (data.get('name') or '').strip()
    if not name:
        return jsonify({'success': False, 'error': 'Name required'}), 400
    svc = get_series_service()
    try:
        s_obj = svc.create_series(name)
        if not s_obj:
            return jsonify({'success': False, 'error': 'Create failed'}), 500
        return jsonify({'success': True, 'series': {
            'id': s_obj.id,
            'name': s_obj.name,
            'normalized_name': s_obj.normalized_name
        }})
    except Exception as e:
        # Return raw error to aid debugging (consider hiding behind DEBUG flag later)
        logger.error(f"/series/create error: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@series_bp.route('/<series_id>/attach_book', methods=['POST'])
@login_required
def attach_book_api(series_id: str):
    data = request.get_json(silent=True) or {}
    book_id = (data.get('book_id') or '').strip()
    if not book_id:
        return jsonify({'success': False, 'error': 'book_id required'}), 400
    volume = (data.get('volume') or '').strip() or None
    order_number = data.get('order_number')
    try:
        if order_number is not None:
            order_number = int(order_number)
    except (ValueError, TypeError):
        order_number = None
    svc = get_series_service()
    try:
        ok = svc.attach_book(book_id, series_id, volume=volume, order_number=order_number)
        if not ok:
            return jsonify({'success': False, 'error': 'Attach failed'}), 500
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"attach_book_api error: {e}")
        return jsonify({'success': False, 'error': 'Server error'}), 500
