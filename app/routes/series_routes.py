"""Series related UI routes (list and detail pages).

Initial skeleton implementing list and detail rendering using the new
KuzuSeriesService. Enhancements (sorting modal, editing, cover upload)
will be layered on subsequently.
"""

from flask import Blueprint, render_template, request, abort, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
import logging
import uuid, time, traceback

from app.services.kuzu_series_service import get_series_service
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
    """Clear the custom (user uploaded) cover only.

    New minimal logic: we simply NULL s.user_cover and set s.custom_cover FALSE.
    We DO NOT modify any book nodes nor recompute anything. Frontend will fall back
    to first-book cover on next fetch/detail view load.
    """
    trace_id = uuid.uuid4().hex[:8]
    t0 = time.time()
    logger.info(f"[SERIES][CLEAR_COVER][{trace_id}] START series_id=%s user_id=%s", series_id, getattr(current_user, 'id', 'anon'))
    svc = get_series_service()
    series_obj = None
    try:
        series_obj = svc.get_series(series_id)
    except Exception:
        logger.exception(f"[SERIES][CLEAR_COVER][{trace_id}] Exception fetching series")
    if not series_obj:
        logger.warning(f"[SERIES][CLEAR_COVER][{trace_id}] Series not found")
        return jsonify({'success': False, 'error': 'Series not found', 'trace_id': trace_id}), 404

    # Snapshot current state
    try:
        logger.debug(
            f"[SERIES][CLEAR_COVER][{trace_id}] CurrentSeries id=%s name=%r user_cover=%r cover_url=%r custom_cover=%r generated_placeholder=%r",
            getattr(series_obj, 'id', None),
            getattr(series_obj, 'name', None),
            getattr(series_obj, 'user_cover', None),
            getattr(series_obj, 'cover_url', None),
            getattr(series_obj, 'custom_cover', None),
            getattr(series_obj, 'generated_placeholder', None),
        )
    except Exception:
        logger.exception(f"[SERIES][CLEAR_COVER][{trace_id}] Failed logging series snapshot")

    from app.infrastructure.kuzu_graph import safe_execute_kuzu_query
    # Updated query: clear user cover then derive fallback first-book cover in same round trip
    q = (
        "MATCH (s:Series {id:$id}) "
        "SET s.user_cover=NULL, s.custom_cover=FALSE "
        "WITH s "
        "OPTIONAL MATCH (b:Book)-[:PART_OF_SERIES]->(s) "
        "WITH s, MIN(CASE WHEN b.published_date IS NULL THEN date('9999-12-31') ELSE b.published_date END) AS firstPub "
        "OPTIONAL MATCH (fb:Book)-[:PART_OF_SERIES]->(s) "
        "WHERE (fb.published_date IS NOT NULL AND fb.published_date = firstPub) OR (firstPub = date('9999-12-31') AND fb.published_date IS NULL) "
        "RETURN s.id, fb.cover_url AS fallback_cover"
    )
    params = {"id": series_id}
    logger.debug(f"[SERIES][CLEAR_COVER][{trace_id}] Executing query=%s params=%s", q, params)
    old_custom = getattr(series_obj, 'user_cover', None) or ''
    res = None
    row_count = 0
    first_row = None
    try:
        res = safe_execute_kuzu_query(q, params)
        logger.debug(f"[SERIES][CLEAR_COVER][{trace_id}] safe_execute_kuzu_query returned type=%s", type(res).__name__)
        if res and hasattr(res, 'has_next'):
            while True:
                try:
                    if not res.has_next():  # type: ignore[attr-defined]
                        break
                    row = res.get_next()  # type: ignore[attr-defined]
                    if first_row is None:
                        first_row = list(row) if not isinstance(row, (list, tuple)) else row
                    row_count += 1
                except Exception as iter_e:
                    logger.error(f"[SERIES][CLEAR_COVER][{trace_id}] Iteration error: %s", iter_e)
                    break
        else:
            logger.warning(f"[SERIES][CLEAR_COVER][{trace_id}] Result object has no has_next attribute or is None")
    except Exception as qe:
        logger.exception(f"[SERIES][CLEAR_COVER][{trace_id}] Query execution failed: {qe}")
        return jsonify({'success': False, 'error': str(qe), 'trace_id': trace_id}), 500

    logger.debug(f"[SERIES][CLEAR_COVER][{trace_id}] Query rows=%d first_row=%r", row_count, first_row)
    ok = row_count > 0
    fallback_cover = None
    try:
        if first_row and len(first_row) > 1:
            fallback_cover = first_row[1]
    except Exception:
        logger.exception(f"[SERIES][CLEAR_COVER][{trace_id}] Failed extracting fallback_cover")

    # Attempt to delete old file if local
    file_deleted = False
    file_delete_error = None
    if old_custom and isinstance(old_custom, str) and old_custom.startswith('/covers/'):
        try:
            from app.utils.image_processing import get_covers_dir
            p = get_covers_dir() / old_custom.split('/')[-1]
            if p.exists():
                p.unlink()
                file_deleted = True
                logger.debug(f"[SERIES][CLEAR_COVER][{trace_id}] Deleted previous custom cover file path=%s", p)
            else:
                logger.debug(f"[SERIES][CLEAR_COVER][{trace_id}] Previous custom cover file not found path=%s", p)
        except Exception as fe:
            file_delete_error = str(fe)
            logger.error(f"[SERIES][CLEAR_COVER][{trace_id}] Failed deleting old cover: %s", fe)
            logger.debug("[SERIES][CLEAR_COVER][%s] Traceback:\n%s", trace_id, traceback.format_exc())
    else:
        logger.debug(f"[SERIES][CLEAR_COVER][{trace_id}] No old_custom file deletion needed value=%r", old_custom)

    dt = (time.time() - t0) * 1000.0
    logger.info(f"[SERIES][CLEAR_COVER][{trace_id}] END success=%s ms=%.2f rows=%d file_deleted=%s", ok, dt, row_count, file_deleted)
    # If fallback_cover available set as new effective cover_url so client updates immediately
    effective_cover = fallback_cover if fallback_cover else None
    logger.debug(f"[SERIES][CLEAR_COVER][{trace_id}] effective_cover=%r", effective_cover)
    return jsonify({
        'success': ok,
        'cover_url': effective_cover,
        'cleared': True,
        'trace_id': trace_id,
        'rows': row_count,
        'file_deleted': file_deleted,
        'file_delete_error': file_delete_error,
        'first_row': first_row,
        'fallback_cover': fallback_cover,
        'elapsed_ms': dt
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
        from app.infrastructure.kuzu_graph import safe_execute_kuzu_query
        # Detach relations then delete series
        q = (
            "MATCH (s:Series {id:$id}) DETACH DELETE s"
        )
        safe_execute_kuzu_query(q, {"id": series_id})
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
