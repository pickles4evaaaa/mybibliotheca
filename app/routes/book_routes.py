from flask import Blueprint, render_template, request, redirect, url_for, flash, abort, jsonify, make_response
"""
Core book management routes for the Bibliotheca application.
Handles book CRUD operations, library views, and book-specific actions.
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, current_app, jsonify, abort, make_response, send_file
from flask_login import login_required, current_user
from datetime import datetime, date
import uuid
import traceback
import requests
import re
import csv
import io
from pathlib import Path
from io import BytesIO

from app.services import book_service, reading_log_service, custom_field_service, user_service
from app.simplified_book_service import SimplifiedBookService, SimplifiedBook, BookAlreadyExistsError
from app.utils import fetch_book_data, get_google_books_cover, fetch_author_data, generate_month_review_image
from app.utils.book_utils import get_best_cover_for_book
from app.utils.image_processing import process_image_from_url, process_image_from_filestorage, get_covers_dir
from app.utils.safe_kuzu_manager import get_safe_kuzu_manager
from app.domain.models import Book as DomainBook

# Quiet mode for book routes; enable with VERBOSE=true or IMPORT_VERBOSE=true
import os as _os_for_verbose
_IMPORT_VERBOSE = (
    (_os_for_verbose.getenv('VERBOSE') or 'false').lower() == 'true'
    or (_os_for_verbose.getenv('IMPORT_VERBOSE') or 'false').lower() == 'true'
)
def _dprint(*args, **kwargs):
    if _IMPORT_VERBOSE:
        __builtins__.print(*args, **kwargs)
print = _dprint

# =============================
# Helper functions (refactored edit_book logic)
# =============================
def _scalar_diff_changes(user_book, form):
    """Compute scalar changes (no contributors/categories) - DEPRECATED: Use _get_changed_fields instead."""
    # This function is kept for any legacy callers but is no longer used in the main edit flow
    return _get_changed_fields(user_book, form)

def _get_changed_fields(user_book, form):
    """
    Get only the fields that actually changed from the form.
    
    Uses UnifiedCoverManager for intelligent cover preservation.
    """
    from app.services.unified_cover_manager import cover_manager
    
    def _norm(v):
        if v is None:
            return None
        if isinstance(v, str):
            return v.strip() or None
        return v

    changes = {}
    
    # All possible updatable fields (EXCLUDING cover_url - handled separately)
    all_fields = [
        'title', 'subtitle', 'description', 'publisher', 'isbn13', 'isbn10', 'published_date', 
        'language', 'asin', 'google_books_id', 'openlibrary_id', 'average_rating', 
        'rating_count', 'media_type', 'personal_notes', 'review', 
        'user_rating', 'reading_status', 'ownership_status'
    ]
    
    # Process regular fields
    for field in all_fields:
        if field not in form:
            continue
            
        new_val = _norm(form.get(field, ''))
        current_val = getattr(user_book, field, None)
        current_norm = current_val.strip() if isinstance(current_val, str) else current_val
        if isinstance(current_norm, str) and not current_norm:
            current_norm = None
            
        if new_val != current_norm:
            # Handle special field types
            if field == 'published_date' and new_val:
                try:
                    new_val = _convert_published_date_to_date(new_val)
                except Exception:
                    continue
            elif field in ('average_rating', 'user_rating') and new_val:
                try:
                    new_val = float(new_val)
                except Exception:
                    continue
            elif field == 'rating_count' and new_val:
                try:
                    new_val = int(new_val)
                except Exception:
                    continue
            
            changes[field] = new_val
    
    # Handle cover_url separately using UnifiedCoverManager
    cover_updates = cover_manager.process_cover_form_field(form, user_book)
    changes.update(cover_updates)
    
    return changes

def _handle_personal_only(uid, form):
    update_data = {}
    def _opt(name, cast=None):
        if name in form:
            val = form.get(name, '').strip()
            if cast and val:
                try:
                    return cast(val)
                except Exception:
                    return None
            return val or None
        return None
    update_data['personal_notes'] = _opt('personal_notes')
    update_data['review'] = _opt('review')
    ur = _opt('user_rating', float)
    if 'user_rating' in form:
        update_data['user_rating'] = ur
    rs = _opt('reading_status')
    if rs:
        update_data['reading_status'] = rs
    os_ = _opt('ownership_status')
    if os_:
        update_data['ownership_status'] = os_
    # media_type is Book metadata, not personal - don't handle it here
    # Prune Nones except those explicitly set to clear (user_rating allowed to be None)
    cleaned = {k:v for k,v in update_data.items() if v is not None or k == 'user_rating'}
    try:
        success = book_service.update_book_sync(uid, str(current_user.id), **cleaned)
        if success:
            updated = [
                ('personal_notes','personal notes'),('review','review'),('user_rating','rating'),
                ('reading_status','reading status'),('ownership_status','ownership status')
            ]
            names = [label for f,label in updated if f in cleaned]
            flash(f"Updated {', '.join(names)} successfully." if names else 'Personal data updated successfully.', 'success')
        else:
            flash('Failed to update personal data.', 'error')
    except Exception as e:  # pragma: no cover
        traceback.print_exc()
        flash(f'Error updating personal data: {str(e)}', 'error')
    return redirect(url_for('book.view_book_enhanced', uid=uid))

def _handle_book_only(uid, form):
    update_data = {}
    def _opt(name):
        if name in form:
            val = form.get(name, '').strip()
            return val or None
        return None
    for name in ['publisher','isbn13','isbn10','language','asin','google_books_id','openlibrary_id']:
        val = _opt(name)
        if name in form:
            update_data[name] = val
    if 'published_date' in form:
        pd = form.get('published_date','').strip()
        if pd:
            update_data['published_date'] = _convert_published_date_to_date(pd)
    if 'average_rating' in form:
        ar = form.get('average_rating','').strip()
        update_data['average_rating'] = float(ar) if ar else None
    if 'rating_count' in form:
        rc = form.get('rating_count','').strip()
        update_data['rating_count'] = int(rc) if rc else None
    try:
        success = book_service.update_book_sync(uid, str(current_user.id), **update_data)
        if success:
            label_map = {
                'publisher':'publisher','average_rating':'average rating','rating_count':'rating count',
                'isbn13':'ISBN-13','isbn10':'ISBN-10','published_date':'published date','language':'language',
                'asin':'ASIN','google_books_id':'Google Books ID','openlibrary_id':'OpenLibrary ID'
            }
            names = [label_map[k] for k in update_data.keys() if k in label_map]
            flash(f"Updated {', '.join(names)} successfully." if names else 'Book metadata updated successfully.', 'success')
        else:
            flash('Failed to update book metadata.', 'error')
    except Exception as e:  # pragma: no cover
        traceback.print_exc()
        flash(f'Error updating book metadata: {str(e)}', 'error')
    return redirect(url_for('book.view_book_enhanced', uid=uid))

def _handle_mixed(uid, form):
    book_update_data = {}
    personal_update_data = {}
    for field in ['publisher','isbn13','isbn10','published_date','language','asin','google_books_id','openlibrary_id','average_rating','rating_count']:
        if field in form:
            val = form.get(field,'').strip() or None
            if field == 'published_date' and val:
                val = _convert_published_date_to_date(val)
            elif field in ('average_rating',) and val:
                try: val = float(val)
                except Exception: continue
            elif field in ('rating_count',) and val:
                try: val = int(val)
                except Exception: continue
            book_update_data[field] = val
    for field in ['personal_notes','review','user_rating','reading_status','ownership_status']:
        if field in form:
            val = form.get(field,'').strip() or None
            if field == 'user_rating' and val:
                try: val = float(val)
                except Exception: continue
            personal_update_data[field] = val
    # media_type goes to book metadata, not personal
    if 'media_type' in form:
        val = form.get('media_type','').strip() or None
        if val:
            book_update_data['media_type'] = val
    try:
        b_ok = True
        if book_update_data:
            b_ok = book_service.update_book_sync(uid, str(current_user.id), **book_update_data)
        p_ok = True
        if personal_update_data:
            p_ok = book_service.update_book_sync(uid, str(current_user.id), **personal_update_data)
        if b_ok and p_ok:
            updated = []
            if 'publisher' in book_update_data: updated.append('publisher')
            if 'average_rating' in book_update_data: updated.append('average rating')
            if 'rating_count' in book_update_data: updated.append('rating count')
            if 'review' in personal_update_data: updated.append('review')
            if 'user_rating' in personal_update_data: updated.append('your rating')
            if 'personal_notes' in personal_update_data: updated.append('personal notes')
            flash(f"Updated {', '.join(updated)} successfully." if updated else 'Data updated successfully.', 'success')
        else:
            flash('Failed to update some data.', 'error')
    except Exception as e:  # pragma: no cover
        traceback.print_exc()
        flash(f'Error updating data: {str(e)}', 'error')
    return redirect(url_for('book.view_book_enhanced', uid=uid))

def _handle_edit_book_post(uid, user_book, form):
    """Unified POST handler that updates only the fields that actually changed."""
    form_keys = set(form.keys())
    
    # Check if this has contributors or categories (complex editing)
    has_contributor_fields = any(k.startswith('contributors[') for k in form_keys)
    has_category_fields = any(k == 'categories' or k.startswith('categories[') for k in form_keys)
    
    if has_contributor_fields or has_category_fields:
        # Return None to fall back to legacy full processing
        return None
    
    # Simple field updates - get only what changed
    changes = _get_changed_fields(user_book, form)
    
    if changes:
        try:
            ok = book_service.update_book_sync(uid, str(current_user.id), **changes)
            if ok:
                field_names = ', '.join(sorted(changes.keys()))
                flash(f"Updated: {field_names}", 'success')
            else:
                flash('No changes saved (update rejected).', 'warning')
        except Exception as e:
            current_app.logger.error(f"[BOOK_EDIT] Update failed: {e}")
            flash('Error saving changes.', 'error')
        return redirect(url_for('book.view_book_enhanced', uid=uid))
    else:
        flash('No changes detected.', 'info')
        return redirect(url_for('book.view_book_enhanced', uid=uid))

# Create book blueprint
book_bp = Blueprint('book', __name__)
api_book_bp = Blueprint('api_book', __name__, url_prefix='/api/book')

def _convert_query_result_to_list(result):
    """Convert SafeKuzuManager query result to legacy list format"""
    try:
        if hasattr(result, 'get_next'):
            rows = []
            while result.has_next():
                row = result.get_next()
                rows.append(row)
            return rows
        else:
            return list(result) if result else []
    except Exception as e:
        current_app.logger.error(f"Query result conversion error: {e}")
        return []

def _convert_published_date_to_date(published_date_str):
    """Convert published_date string to date object using enhanced date parser."""
    if not published_date_str or not isinstance(published_date_str, str):
        return None
    
    try:
        # Simple date parsing - handle common formats
        from datetime import datetime
        # Try common date formats
        formats = ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y', '%Y']
        for fmt in formats:
            try:
                return datetime.strptime(published_date_str.strip(), fmt).date()
            except ValueError:
                continue
        return None
    except Exception as e:
        current_app.logger.warning(f"Failed to parse published date '{published_date_str}': {e}")
        return None

def _safe_date_to_isoformat(date_obj):
    """Safely convert date object to ISO format string."""
    if date_obj and hasattr(date_obj, 'isoformat'):
        return date_obj.isoformat()
    return None

def _format_published_date_for_input(published_date_str):
    """Format published_date for HTML5 date input (YYYY-MM-DD)."""
    if not published_date_str or not isinstance(published_date_str, str):
        return None
    
    try:
        from datetime import datetime
        date_str = published_date_str.strip()
        
        # Try various date formats that APIs might return
        formats = [
            '%Y-%m-%d',        # 2023-12-25
            '%Y/%m/%d',        # 2023/12/25  
            '%m/%d/%Y',        # 12/25/2023
            '%d/%m/%Y',        # 25/12/2023
            '%Y-%m',           # 2023-12
            '%Y/%m',           # 2023/12
            '%m/%Y',           # 12/2023
            '%Y',              # 2023
            '%B %d, %Y',       # December 25, 2023
            '%b %d, %Y',       # Dec 25, 2023
            '%d %B %Y',        # 25 December 2023
            '%d %b %Y',        # 25 Dec 2023
            '%B %Y',           # December 2023
            '%b %Y',           # Dec 2023
        ]
        
        for fmt in formats:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                # Return YYYY-MM-DD format for HTML5 date input
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        # If no format matches, try to extract just the year
        import re
        year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
        if year_match:
            year = year_match.group()
            # Return January 1st of that year as default
            return f"{year}-01-01"
        
        return None
    except Exception as e:
        return None

@book_bp.route('/fetch_book/<isbn>', methods=['GET'])
def fetch_book(isbn):
    """Legacy endpoint: now powered by unified ISBN metadata with graceful fallback."""
    try:
        # Early ISBN validation to avoid unnecessary provider calls
        import re as _re
        raw = _re.sub(r'[^0-9Xx]', '', (isbn or '').strip())
        def _v10(v: str):
            if len(v)!=10 or not _re.match(r'^[0-9]{9}[0-9Xx]$', v): return False
            s=0
            for i,ch in enumerate(v[:9]): s += (10-i)*int(ch)
            s += 10 if v[9] in 'Xx' else int(v[9])
            return s%11==0
        def _v13(v: str):
            if len(v)!=13 or not v.isdigit(): return False
            if not (v.startswith('978') or v.startswith('979')): return False
            t=0
            for i,ch in enumerate(v[:12]): t += (1 if i%2==0 else 3)*int(ch)
            return (10-(t%10))%10 == int(v[12])
        if not raw or not (_v10(raw) or _v13(raw)):
            return jsonify({'error': 'Invalid ISBN'}), 400
        from app.utils.metadata_aggregator import fetch_unified_by_isbn
        unified = fetch_unified_by_isbn(isbn) or {}
        book_data = dict(unified)

        # Maintain compatibility fields
        if book_data and not book_data.get('isbn'):
            # Provide a generic 'isbn' field for legacy consumers
            book_data['isbn'] = book_data.get('isbn13') or book_data.get('isbn10') or isbn

        # Provide 'author' (comma-joined) for older UIs
        if not book_data.get('author') and book_data.get('authors'):
            if isinstance(book_data['authors'], list):
                book_data['author'] = ', '.join([a for a in book_data['authors'] if isinstance(a, str)])
            else:
                book_data['author'] = book_data['authors']

        # If neither source provides a cover, set a default
        from app.utils.image_processing import process_image_from_url
        # Select best cover (favor Google Books)
        try:
            best_cover = get_best_cover_for_book(isbn=isbn,
                                                title=book_data.get('title'),
                                                author=book_data.get('author') or (', '.join(book_data.get('authors', [])) if isinstance(book_data.get('authors'), list) else None))
            if best_cover.get('cover_url'):
                book_data['cover'] = best_cover['cover_url']
                book_data['cover_url'] = best_cover['cover_url']
                book_data['cover_source'] = best_cover.get('source')
        except Exception as ce:
            current_app.logger.debug(f"Best cover helper failed (unified path) for {isbn}: {ce}")

        cover_url = book_data.get('cover_url') or book_data.get('cover')
        # If no cover yet but we have title/author, perform a lightweight title/author search to attempt cover recovery
        if not cover_url and (book_data.get('title') and (book_data.get('author') or book_data.get('authors'))):
            try:
                from app.utils.metadata_aggregator import fetch_unified_by_title
                author_for_search = book_data.get('author')
                authors_list = book_data.get('authors')
                if not author_for_search and isinstance(authors_list, list):
                    str_authors = [a for a in authors_list if isinstance(a, str)]
                    if str_authors:
                        author_for_search = ', '.join(str_authors)
                title_for_search = book_data.get('title') or ''
                search_results = fetch_unified_by_title(str(title_for_search), max_results=4, author=author_for_search)
                for sr in search_results or []:
                    c = sr.get('cover_url') or sr.get('cover')
                    if c:
                        book_data['cover'] = c
                        book_data['cover_url'] = c
                        book_data['cover_source'] = sr.get('cover_source') or 'TitleFallback'
                        cover_url = c
                        current_app.logger.info(f"[COVER][ISBN_FALLBACK] Acquired via title/author search isbn={isbn}")
                        break
            except Exception as fb_err:
                current_app.logger.debug(f"[COVER][ISBN_FALLBACK_FAIL] isbn={isbn} err={fb_err}")
        async_requested = request.args.get('async_cover') == '1'
        if cover_url and not async_requested:
            try:
                from app.services.cover_service import cover_service
                if not cover_url.startswith('/covers/'):
                    cr = cover_service.fetch_and_cache(isbn=isbn,
                                                       title=book_data.get('title'),
                                                       author=book_data.get('author'))
                    if cr.cached_url:
                        cover_url = cr.cached_url
                        book_data['cover_source'] = cr.source
                        book_data['cover_quality'] = cr.quality
                book_data['cover'] = cover_url
                book_data['cover_url'] = cover_url
            except Exception as e:
                current_app.logger.error(f"[COVER][UNIFIED_FETCH] Failed cover service processing: {e}")
        elif async_requested:
            # Schedule background processing and return candidate immediately
            from app.services.cover_service import cover_service
            cand = cover_service.select_candidate(isbn=isbn, title=book_data.get('title'), author=book_data.get('author'))
            if cand:
                book_data['cover_candidate'] = cand
                book_data['cover_url'] = cand.get('url')
            job = cover_service.schedule_async_processing(isbn=isbn, title=book_data.get('title'), author=book_data.get('author'))
            book_data['cover_job_id'] = job['id']
        if not book_data.get('cover'):
            fallback = url_for('serve_static', filename='bookshelf.png', _external=True)
            book_data['cover'] = fallback
            book_data['cover_url'] = fallback

        # Date is already normalized by unified aggregator; still ensure input format
        if book_data.get('published_date'):
            original_date = book_data['published_date']
            formatted_date = _format_published_date_for_input(original_date)
            if formatted_date:
                book_data['published_date'] = formatted_date
                print(f"📅 [FETCH_BOOK] Formatted published_date: '{original_date}' -> '{formatted_date}'")

        # Debug logging
        print(f"🔍 [FETCH_BOOK] Unified data keys for ISBN {isbn}: {list(book_data.keys())}")
        return jsonify(book_data), 200 if book_data else 404
    except Exception as e:
        # Fallback to legacy behavior if unified fails
        current_app.logger.warning(f"Unified fetch failed for {isbn}: {e}. Falling back to legacy.")
        book_data = fetch_book_data(isbn) or {}
        google_data = get_google_books_cover(isbn, fetch_title_author=True)
        if google_data:
            for key, value in google_data.items():
                if key not in book_data or not book_data[key]:
                    book_data[key] = value
        # Apply best cover selection
        try:
            best_cover = get_best_cover_for_book(isbn=isbn,
                                                title=book_data.get('title'),
                                                author=book_data.get('author'))
            if best_cover.get('cover_url'):
                book_data['cover'] = best_cover['cover_url']
                book_data['cover_url'] = best_cover['cover_url']
                book_data['cover_source'] = best_cover.get('source')
        except Exception as ce:
            current_app.logger.debug(f"Best cover helper failed (fallback path) for {isbn}: {ce}")
            from app.utils.image_processing import process_image_from_url
            cover_url = google_data.get('cover_url')
            if cover_url and not book_data.get('cover'):
                try:
                    processed_cover = process_image_from_url(cover_url)
                    if processed_cover:
                        if processed_cover.startswith('/'):
                            book_data['cover'] = request.host_url.rstrip('/') + processed_cover
                        else:
                            book_data['cover'] = processed_cover
                except Exception as e:
                    current_app.logger.warning(f"Failed to process cover image: {cover_url} ({e})")
        if not book_data.get('isbn'):
            if google_data and google_data.get('isbn_13'):
                book_data['isbn'] = google_data['isbn_13']
            elif google_data and google_data.get('isbn_10'):
                book_data['isbn'] = google_data['isbn_10']
            else:
                book_data['isbn'] = isbn
        if not book_data.get('author') and book_data.get('authors'):
            book_data['author'] = book_data['authors']
        if book_data.get('published_date'):
            fmt = _format_published_date_for_input(book_data['published_date'])
            if fmt:
                book_data['published_date'] = fmt
        if not book_data.get('cover'):
            book_data['cover'] = url_for('serve_static', filename='bookshelf.png')
        return jsonify(book_data), 200 if book_data else 404
@api_book_bp.route('/quick_isbn/<isbn>', methods=['GET'])
def api_quick_isbn(isbn):
    """API-first fast ISBN metadata endpoint with optional async cover processing (no auth redirect)."""
    try:
        import time as _t_perf
        _marks = []
        def _mark(label):
            _marks.append((label, _t_perf.perf_counter()))
        def _summary():
            if not _marks:
                return ''
            base = _marks[0][1]
            last = base
            parts = []
            for label, t in _marks:
                parts.append(f"{label}+{(t-base)*1000:.1f}ms(Δ{(t-last)*1000:.1f}ms)")
                last = t
            parts.append(f"TOTAL={(last-base)*1000:.1f}ms")
            return ' | '.join(parts)
        _mark('start')
        from app.utils.metadata_aggregator import fetch_unified_by_isbn
        unified = fetch_unified_by_isbn(isbn) or {}
        _mark('unified_fetch')
        data = dict(unified)
        data['isbn'] = data.get('isbn13') or data.get('isbn10') or isbn
        if not data.get('author') and data.get('authors'):
            if isinstance(data['authors'], list):
                data['author'] = ', '.join([a for a in data['authors'] if isinstance(a, str)])
        # High-quality cover normalization (post-merge guard)
        try:
            from app.utils.book_utils import select_highest_google_image, upgrade_google_cover_url
            if data.get('image_links_all'):
                best = select_highest_google_image(data.get('image_links_all'))
                if best:
                    data['cover_url'] = upgrade_google_cover_url(best)
            elif data.get('cover_url'):
                data['cover_url'] = upgrade_google_cover_url(data.get('cover_url'))
        except Exception:
            pass
        _mark('cover_norm')
        async_mode = request.args.get('async_cover') == '1'
        from app.services.cover_service import cover_service
        if async_mode:
            cand = cover_service.select_candidate(isbn=isbn, title=data.get('title'), author=data.get('author'))
            if cand:
                data['cover_candidate'] = cand
                data['cover_url'] = cand.get('url')
            job = cover_service.schedule_async_processing(isbn=isbn, title=data.get('title'), author=data.get('author'))
            data['cover_job_id'] = job['id']
            _mark('async_schedule')
        else:
            cr = cover_service.fetch_and_cache(isbn=isbn, title=data.get('title'), author=data.get('author'))
            if cr.cached_url:
                data['cover_url'] = cr.cached_url
                data['cover_source'] = cr.source
                data['cover_quality'] = cr.quality
            _mark('cover_process')
        try:
            current_app.logger.info(f"[BOOK][QUICK_ISBN][PERF] isbn={isbn} { _summary() }")
        except Exception:
            pass
        return jsonify(data), 200 if data else 404
    except Exception as e:  # pragma: no cover
        return jsonify({'error': 'failed', 'message': str(e)}), 500

@book_bp.route('/')
@login_required
def index():
    """Redirect to library page as the main landing page"""
    return redirect(url_for('main.library'))

@book_bp.route('/add', methods=['GET', 'POST'])
@login_required
def add_book():
    """Add a new book to the library"""
    if request.method == 'GET':
        # Load existing custom fields for the user
        try:
            personal_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=False)
            global_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=True)
        except Exception as e:
            current_app.logger.error(f"Error loading custom fields for add book: {e}")
            personal_fields = []
            global_fields = []
        
        # Load AI configuration
        try:
            from app.admin import load_ai_config
            ai_config = load_ai_config()
        except Exception as e:
            current_app.logger.error(f"Error loading AI config: {e}")
            ai_config = {}
        
        return render_template('add_book.html', 
                             personal_fields=personal_fields,
                             global_fields=global_fields,
                             ai_config=ai_config)
    
    # Handle POST request for adding book
    # Forward to the manual add handler
    return add_book_manual()

@book_bp.route('/add/image', methods=['POST'])
@login_required
def add_book_from_image():
        """Handle image upload for ISBN extraction and book addition"""
        try:
            # Check if file was uploaded
            if 'image' not in request.files:
                return jsonify({'error': 'No image file provided'}), 400
            
            file = request.files['image']
            if file.filename == '':
                return jsonify({'error': 'No file selected'}), 400
            
            # Validate file type
            allowed_extensions = {'png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff', 'webp'}
            filename = file.filename or ''
            file_ext = filename.rsplit('.', 1)[1].lower() if '.' in filename and len(filename.rsplit('.', 1)) > 1 else ''
            if file_ext not in allowed_extensions:
                return jsonify({'error': 'Invalid file type. Please upload an image file.'}), 400
            
            # Import OCR scanner
            from app.ocr_scanner import extract_isbn_from_image, is_ocr_available
            
            # Check if OCR is available
            if not is_ocr_available():
                return jsonify({
                    'error': 'OCR functionality not available. Please install required dependencies: opencv-python, pyzbar, pytesseract'
                }), 500
            
            # Extract ISBN from image
            isbn = extract_isbn_from_image(file)
            
            if not isbn:
                # Return structured JSON with success flag so the UI can show a friendly message
                return jsonify({
                    'success': False,
                    'error': 'No ISBN found in image. Please try a clearer image with visible barcode or ISBN text.',
                    'suggestion': 'Make sure the barcode or ISBN text is clearly visible and well-lit'
                })
            
            # Optionally fetch unified book data using the extracted ISBN (not required by UI)
            # UI will call unified-metadata separately after we return ISBN, but we include
            # data here when available for completeness/debugging.
            try:
                # Use unified metadata aggregator (already normalizes covers downstream)
                from app.utils.unified_metadata import fetch_unified_by_isbn
                book_data = fetch_unified_by_isbn(isbn)
                # Ensure cover is processed and stored
                if book_data:
                    from app.utils.image_processing import process_image_from_url
                    cover_url = book_data.get('cover_url') or book_data.get('cover')
                    if cover_url:
                        try:
                            processed_cover = process_image_from_url(cover_url)
                            if processed_cover:
                                abs_cover_url = processed_cover
                                if processed_cover.startswith('/'):
                                    abs_cover_url = request.host_url.rstrip('/') + processed_cover
                                book_data['cover'] = abs_cover_url
                                book_data['cover_url'] = abs_cover_url
                        except Exception as e:
                            current_app.logger.warning(f"Failed to process cover image: {cover_url} ({e})")
            except Exception:
                book_data = None
            
            if book_data and 'title' in book_data:
                # Normalize fields for client consumption
                try:
                    import re as _re
                    try:
                        from app.utils.unified_metadata import _normalize_date as _nm
                    except Exception:
                        def _nm(val):
                            s = str(val).strip() if val else ''
                            if not s:
                                return None
                            if _re.fullmatch(r"\d{4}", s):
                                return f"{s}-01-01"
                            if _re.fullmatch(r"\d{4}-\d{2}", s):
                                return f"{s}-01"
                            m = _re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
                            if m:
                                mm, dd, yyyy = m.groups()
                                return f"{int(yyyy):04d}-{int(mm):02d}-{int(dd):02d}"
                            m2 = _re.search(r"(\d{4})", s)
                            return f"{int(m2.group(1)):04d}-01-01" if m2 else None

                    if book_data.get('published_date'):
                        nd = _nm(book_data.get('published_date'))
                        if nd:
                            book_data['published_date'] = nd
                    elif book_data.get('publication_year') and not book_data.get('published_date'):
                        year = str(book_data.get('publication_year')).strip()
                        if _re.fullmatch(r"\d{4}", year):
                            book_data['published_date'] = f"{year}-01-01"

                    raw_isbn = (book_data.get('isbn') or book_data.get('isbn13') or book_data.get('isbn_13')
                                or book_data.get('isbn10') or book_data.get('isbn_10'))
                    if raw_isbn:
                        digits = _re.sub(r"[^0-9Xx]", "", str(raw_isbn))
                        if len(digits) == 13:
                            book_data.setdefault('isbn13', digits)
                            book_data.setdefault('isbn_13', digits)
                        elif len(digits) == 10:
                            book_data.setdefault('isbn10', digits)
                            book_data.setdefault('isbn_10', digits)
                except Exception as _e:
                    current_app.logger.debug(f"OCR unified data post-process skipped: {_e}")

                return jsonify({
                    'success': True,
                    'isbn': isbn,
                    'book_data': book_data,
                    'message': f'Successfully extracted ISBN: {isbn}'
                })
            else:
                # Return the ISBN even if book data fetch failed
                return jsonify({
                    'success': True,
                    'isbn': isbn,
                    'book_data': None,
                    'message': f'ISBN extracted: {isbn}, but book data could not be fetched. You can enter details manually.'
                })
            
        except Exception as e:
            current_app.logger.error(f"Error processing image upload: {e}")
            return jsonify({'success': False, 'error': 'Failed to process image. Please try again.'}), 500

@book_bp.route('/add/image-ai', methods=['POST'])
@login_required
def add_book_from_image_ai():
    """Handle image upload for AI-powered book extraction"""
    try:
        current_app.logger.info("AI image processing request started")

        # Check if AI is enabled
        from app.admin import load_ai_config
        ai_config = load_ai_config()

        if ai_config.get('AI_BOOK_EXTRACTION_ENABLED') != 'true':
            current_app.logger.warning("AI book extraction is disabled")
            return jsonify({'success': False, 'error': 'AI book extraction is not enabled. Please contact your administrator.'})

        # Check if file was uploaded
        if 'image' not in request.files:
            current_app.logger.error("No image file in request")
            return jsonify({'success': False, 'error': 'No image file provided'}), 400

        file = request.files['image']
        if not file or file.filename == '':
            current_app.logger.error("Empty filename")
            return jsonify({'success': False, 'error': 'No file selected'}), 400

        current_app.logger.info(f"Processing file: {file.filename}")

        # Validate file type
        allowed_extensions = {'png', 'jpg', 'jpeg', 'gif', 'bmp', 'tiff', 'webp'}
        filename = file.filename or ''
        file_ext = filename.rsplit('.', 1)[1].lower() if '.' in filename and len(filename.rsplit('.', 1)) > 1 else ''
        if file_ext not in allowed_extensions:
            current_app.logger.error(f"Invalid file extension: {file_ext}")
            return jsonify({'success': False, 'error': 'Invalid file type. Please upload an image file.'}), 400

        # Process image with AI
        from app.services.ai_service import AIService
        ai_service = AIService(ai_config)

        # Read file content
        file.seek(0)  # Reset file pointer
        file_content = file.read()

        current_app.logger.info(f"File read successfully, size: {len(file_content)} bytes")

        # Extract book information using AI
        book_data = ai_service.extract_book_info_from_image(file_content, filename)

        current_app.logger.info(f"AI extraction result: {book_data}")

        if book_data:
            # Do not auto-search or enhance with external APIs here.
            # We only normalize obvious fields and let the user run title search manually.

            # Normalize published_date and map ISBN variants
            try:
                import re as _re
                try:
                    from app.utils.unified_metadata import _normalize_date as _nm
                except Exception:
                    def _nm(val):
                        s = str(val).strip() if val else ''
                        if not s:
                            return None
                        if _re.fullmatch(r"\d{4}", s):
                            return f"{s}-01-01"
                        if _re.fullmatch(r"\d{4}-\d{2}", s):
                            return f"{s}-01"
                        m = _re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
                        if m:
                            mm, dd, yyyy = m.groups()
                            return f"{int(yyyy):04d}-{int(mm):02d}-{int(dd):02d}"
                        m2 = _re.search(r"(\d{4})", s)
                        return f"{int(m2.group(1)):04d}-01-01" if m2 else None

                if book_data.get('published_date'):
                    norm = _nm(book_data.get('published_date'))
                    if norm:
                        book_data['published_date'] = norm
                elif book_data.get('publication_year') and not book_data.get('published_date'):
                    year = str(book_data.get('publication_year')).strip()
                    if _re.fullmatch(r"\d{4}", year):
                        book_data['published_date'] = f"{year}-01-01"

                raw_isbn = (book_data.get('isbn') or book_data.get('isbn13') or book_data.get('isbn_13') or book_data.get('isbn10') or book_data.get('isbn_10'))
                if raw_isbn:
                    digits = _re.sub(r"[^0-9Xx]", "", str(raw_isbn))
                    if len(digits) == 13:
                        book_data.setdefault('isbn13', digits)
                        book_data.setdefault('isbn_13', digits)
                    elif len(digits) == 10:
                        book_data.setdefault('isbn10', digits)
                        book_data.setdefault('isbn_10', digits)
            except Exception as _e:
                current_app.logger.debug(f"AI data post-process skipped: {_e}")

            return jsonify({'success': True, 'message': 'AI extraction successful. Use the title search (magnifying glass) to select the correct edition.', **book_data})
        else:
            current_app.logger.warning("AI extraction returned no data")
            return jsonify({
                'success': False,
                'error': 'Could not extract book information from image. Please try a clearer image or use manual entry.',
                'suggestion': 'Make sure the book cover and text are clearly visible and well-lit'
            })

    except Exception as e:
        current_app.logger.error(f"Error processing AI image upload: {e}")
        current_app.logger.error("AI image upload error traceback:", exc_info=True)
        return jsonify({'success': False, 'error': f'Failed to process image with AI: {str(e)}'}), 500

@book_bp.route('/search_details', methods=['POST'])
@login_required
def search_book_details():
    """Search for books by title and/or author and return multiple results for user selection"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'No search criteria provided'}), 400
        
        title = data.get('title', '').strip()
        author = data.get('author', '').strip()
        
        if not title and not author:
            return jsonify({'success': False, 'message': 'Please provide at least a title or author to search'}), 400
        
        current_app.logger.info(f"Book search request: title='{title}', author='{author}'")
        
        # Import search functions
        from app.utils import search_book_by_title_author, fetch_book_data
        import requests
        
        # Basic in-memory request cache (per-process) to prevent duplicate searches during rapid retries
        global _TITLE_AUTHOR_SEARCH_CACHE  # module-level simple cache
        if '_TITLE_AUTHOR_SEARCH_CACHE' not in globals():
            _TITLE_AUTHOR_SEARCH_CACHE = {}
        _SEARCH_CACHE = _TITLE_AUTHOR_SEARCH_CACHE
        cache_key = f"{title.lower()}|{author.lower()}"
        if cache_key in _SEARCH_CACHE:
            cached = _SEARCH_CACHE[cache_key]
            current_app.logger.debug("[SEARCH] Cache hit for title/author combination")
            return jsonify(cached)

        results = []

        # Build query components once
        ol_query = ' '.join([q for q in [title or author, author if (title and author) else None] if q])
        gb_parts = []
        if title:
            gb_parts.append(f'intitle:"{title}"')
        if author:
            gb_parts.append(f'inauthor:"{author}"')
        gb_query = '+'.join(gb_parts)

        import concurrent.futures
        import requests as _req

        def _fetch_openlibrary():
            try:
                if not ol_query:
                    return []
                url = f"https://openlibrary.org/search.json?q={ol_query}&limit=8"
                r = _req.get(url, timeout=6)
                r.raise_for_status()
                data = r.json()
                docs = data.get('docs', [])[:8]
                out = []
                for doc in docs:
                    doc_title = doc.get('title','')
                    doc_authors = doc.get('author_name', []) if isinstance(doc.get('author_name'), list) else [doc.get('author_name','')]
                    doc_isbn = doc.get('isbn', []) or []
                    best_isbn = next((i for i in doc_isbn if len(i)==13), (doc_isbn[0] if doc_isbn else None))
                    res = {
                        'title': doc_title,
                        'authors': ', '.join(doc_authors) if doc_authors else '',
                        'authors_list': doc_authors,
                        'isbn': best_isbn,
                        'publisher': ', '.join(doc.get('publisher', [])) if isinstance(doc.get('publisher'), list) else doc.get('publisher',''),
                        'published_date': str(doc.get('first_publish_year','')) if doc.get('first_publish_year') else '',
                        'page_count': doc.get('number_of_pages_median'),
                        'language': (doc.get('language') or ['en'])[0],
                        'openlibrary_id': doc.get('key','').replace('/works/','') if doc.get('key') else None,
                        'source': 'OpenLibrary'
                    }
                    # Cover selection (non-blocking catch)
                    try:
                        from app.utils.book_utils import get_best_cover_for_book
                        cov = get_best_cover_for_book(isbn=best_isbn, title=doc_title, author=', '.join(doc_authors) if doc_authors else None)
                        if cov and cov.get('cover_url'):
                            res['cover_url'] = cov['cover_url']
                            res['cover_source'] = cov.get('source')
                            res['cover_quality'] = cov.get('quality')
                    except Exception:
                        pass
                    out.append(res)
                return out
            except Exception as e:
                current_app.logger.debug(f"[SEARCH] OpenLibrary failed: {e}")
                return []

        def _fetch_google():
            try:
                if not gb_query:
                    return []
                g_url = f"https://www.googleapis.com/books/v1/volumes?q={gb_query}&maxResults=8"
                r = _req.get(g_url, timeout=5)
                r.raise_for_status()
                data = r.json()
                items = data.get('items', [])[:8]
                out = []
                for item in items:
                    info = item.get('volumeInfo', {})
                    gb_title = info.get('title','')
                    gb_authors = info.get('authors', []) or []
                    identifiers = info.get('industryIdentifiers', []) or []
                    gb_isbn = None
                    for ident in identifiers:
                        t = ident.get('type')
                        if t == 'ISBN_13':
                            gb_isbn = ident.get('identifier'); break
                        if t == 'ISBN_10' and not gb_isbn:
                            gb_isbn = ident.get('identifier')
                    gb_cover = None
                    try:
                        from app.utils.book_utils import select_highest_google_image, upgrade_google_cover_url
                        gb_cover = upgrade_google_cover_url(select_highest_google_image(info.get('imageLinks', {})))
                    except Exception:
                        pass
                    out.append({
                        'title': gb_title,
                        'authors': ', '.join(gb_authors) if gb_authors else '',
                        'authors_list': gb_authors,
                        'isbn': gb_isbn,
                        'publisher': info.get('publisher',''),
                        'published_date': info.get('publishedDate',''),
                        'description': info.get('description',''),
                        'page_count': info.get('pageCount'),
                        'language': info.get('language','en'),
                        'cover_url': gb_cover,
                        'source': 'Google Books'
                    })
                return out
            except Exception as e:
                current_app.logger.debug(f"[SEARCH] Google Books failed: {e}")
                return []

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            fut_ol = ex.submit(_fetch_openlibrary)
            fut_gb = ex.submit(_fetch_google)
            ol_results = fut_ol.result()
            results.extend(ol_results)
            # Early stop if we already have many
            gb_results = fut_gb.result() if len(results) < 8 else []
            # Deduplicate
            existing_keys = {(r.get('title','').lower(), r.get('authors','').lower()) for r in results}
            for r in gb_results:
                key = (r.get('title','').lower(), r.get('authors','').lower())
                if key not in existing_keys:
                    results.append(r)
                    existing_keys.add(key)

        payload = {
            'success': bool(results),
            'results': results,
            'message': f"Found {len(results)} books matching your search" if results else 'No books found matching your search criteria. Try different keywords or check spelling.'
        }
        # Store small cache entry (TTL not implemented; acceptable for session)
        _SEARCH_CACHE[cache_key] = payload
        return jsonify(payload)
        
        if results:
            current_app.logger.info(f"Returning {len(results)} search results")
            return jsonify({
                'success': True,
                'results': results,
                'message': f'Found {len(results)} books matching your search'
            })
        else:
            current_app.logger.info("No search results found")
            return jsonify({
                'success': False,
                'message': 'No books found matching your search criteria. Try different keywords or check spelling.'
            })
    
    except Exception as e:
        current_app.logger.error(f"Error in book search: {e}")
        current_app.logger.error("Book search error traceback:", exc_info=True)
        return jsonify({
            'success': False,
            'message': f'An error occurred while searching: {str(e)}'
        }), 500

@book_bp.route('/search', methods=['GET', 'POST'])
@login_required
def search_books():
    """Redirect to library page where search functionality is now integrated"""
    flash('Search functionality has been moved to the Library page for a better experience.', 'info')
    return redirect(url_for('main.library'))

@book_bp.route('/month_wrapup')
@login_required
def month_wrapup():
    """Redirect to stats page where month wrap-up is now integrated"""
    flash('Month Wrap-up has been moved to the Stats page along with other reading statistics.', 'info')
    return redirect(url_for('main.stats'))

@book_bp.route('/community_activity')
@login_required
def community_activity():
    """Redirect to stats page where community activity is now integrated"""
    flash('Community activity has been moved to the Stats page along with your personal reading statistics.', 'info')
    return redirect(url_for('main.stats'))

@book_bp.route('/bulk_import', methods=['GET', 'POST'])
@login_required
def bulk_import():
    """Redirect to new import interface."""
    flash('Book import has been upgraded! You can now map CSV fields and track progress in real-time.', 'info')
    return redirect(url_for('main.import_books'))

# Legacy route removed - all book views now use enhanced view directly

# Module-level lightweight search cache (title+author -> payload)
_TITLE_AUTHOR_SEARCH_CACHE: dict = {}

@book_bp.route('/book/<uid>/log', methods=['POST'])
@login_required
def log_reading(uid):
    # Get book through service layer
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
    
    log_date_str = request.form.get('log_date')
    log_date = datetime.strptime(log_date_str, '%Y-%m-%d').date() if log_date_str else date.today()
    
    # Check for existing log using service layer
    book_id = user_book.get('id') if isinstance(user_book, dict) else getattr(user_book, 'id', None)
    if book_id:
        existing_log = reading_log_service.get_existing_log_sync(book_id, current_user.id, log_date)
        if existing_log:
            flash('You have already logged reading for this day.')
        else:
            # Create reading log using service layer
            reading_log_service.create_reading_log_sync(book_id, current_user.id, log_date)
            flash('Reading day logged.')
    else:
        flash('Error: Book ID not found.', 'error')
    return redirect(url_for('book.view_book_enhanced', uid=uid))

@book_bp.route('/book/<uid>/delete', methods=['POST'])
@login_required
def delete_book(uid):
    # Delete book through service layer
    success = book_service.delete_book_sync(uid, str(current_user.id))
    
    if success:
        flash('Book deleted from your library.')
    else:
        flash('Failed to delete book.', 'error')
        
    return redirect(url_for('main.library'))

@book_bp.route('/book/<uid>/toggle_finished', methods=['POST'])
@login_required
def toggle_finished(uid):
    # Get book through service layer
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
    
    finish_date = user_book.get('finish_date') if isinstance(user_book, dict) else getattr(user_book, 'finish_date', None)
    if finish_date:
        # Mark as currently reading
        book_service.update_book_sync(uid, str(current_user.id), finish_date=None)
        flash('Book marked as currently reading.')
    else:
        # Mark as finished
        book_service.update_book_sync(uid, str(current_user.id), finish_date=date.today())
        flash('Book marked as finished.')
    
    return redirect(url_for('book.view_book_enhanced', uid=uid))

@book_bp.route('/book/<uid>/start_reading', methods=['POST'])
@login_required
def start_reading(uid):
    # Get book through service layer
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
    
    update_data = {'want_to_read': False}
    start_date = user_book.get('start_date') if isinstance(user_book, dict) else getattr(user_book, 'start_date', None)
    if not start_date:
        update_data['start_date'] = datetime.today().date()  # type: ignore
    
    book_service.update_book_sync(uid, str(current_user.id), **update_data)
    title = user_book.get('title', 'Unknown Title') if isinstance(user_book, dict) else getattr(user_book, 'title', 'Unknown Title')
    flash(f'Started reading "{title}".')
    return redirect(url_for('main.library'))

@book_bp.route('/book/<uid>/update_status', methods=['POST'])
@login_required
def update_status(uid):
    # Get book through service layer
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
    
    # Set status based on checkboxes
    want_to_read = 'want_to_read' in request.form
    library_only = 'library_only' in request.form
    finished = 'finished' in request.form
    currently_reading = 'currently_reading' in request.form

    update_data = {
        'want_to_read': want_to_read,
        'library_only': library_only
    }

    if finished:
        update_data.update({  # type: ignore
            'finish_date': datetime.now().date(),
            'want_to_read': False,
            'library_only': False
        })
    elif currently_reading:
        update_data.update({  # type: ignore
            'finish_date': None,
            'want_to_read': False,
            'library_only': False
        })
        start_date = user_book.get('start_date') if isinstance(user_book, dict) else getattr(user_book, 'start_date', None)
        if not start_date:
            update_data['start_date'] = datetime.now().date()  # type: ignore
    elif want_to_read:
        update_data.update({  # type: ignore
            'finish_date': None,
            'library_only': False
        })
    elif library_only:
        update_data.update({  # type: ignore
            'finish_date': None,
            'want_to_read': False
        })

    book_service.update_book_sync(uid, str(current_user.id), **update_data)
    flash('Book status updated.')
    return redirect(url_for('book.view_book_enhanced', uid=uid))

@book_bp.route('/library')
@login_required
def library():
    # Get filter parameters from URL - default to "all" to show all books
    status_filter = request.args.get('status_filter', 'all')  # Changed from 'reading' to 'all'
    category_filter = request.args.get('category', '')
    publisher_filter = request.args.get('publisher', '')
    language_filter = request.args.get('language', '')
    location_filter = request.args.get('location', '')
    search_query = request.args.get('search', '')
    sort_option = request.args.get('sort', 'title_asc')  # Default to title A-Z

    # Pagination parameters: rows*cols determines per_page; default rows via settings
    try:
        from app.utils.user_settings import get_effective_rows_per_page
        default_rows = get_effective_rows_per_page(str(current_user.id)) or 4
    except Exception:
        default_rows = 4
    page = request.args.get('page', 1, type=int)
    cols = request.args.get('cols', 0, type=int)
    rows = request.args.get('rows', default_rows, type=int)
    # cols can be 0 on first load; client JS will detect and reload if needed. Fallback to 5 typical desktop cols.
    effective_cols = cols if cols and cols > 0 else 5
    per_page = max(1, rows) * max(1, effective_cols)

    # Total count first so we can clamp page to a valid range (cache for short TTL)
    try:
        from app.utils.simple_cache import cache_get, cache_set
        _tc_key = f"total_count:{current_user.id}"
        total_books = cache_get(_tc_key)
        if total_books is None:
            total_books = book_service.get_total_book_count_sync()
            cache_set(_tc_key, int(total_books or 0), ttl_seconds=300)
    except Exception:
        total_books = 0

    # Compute total pages and clamp page
    import math
    total_pages = max(1, math.ceil(total_books / per_page)) if per_page > 0 else 1
    page = max(1, min(page, total_pages))

    # Decide data retrieval strategy: if any filter is active, pull all then filter across full set
    offset = (page - 1) * per_page
    has_filter = any([
        status_filter and status_filter != 'all',
        bool(search_query.strip()) if isinstance(search_query, str) else False,
        bool(category_filter.strip()) if isinstance(category_filter, str) else False,
        bool(publisher_filter.strip()) if isinstance(publisher_filter, str) else False,
        bool(language_filter.strip()) if isinstance(language_filter, str) else False,
        bool(location_filter.strip()) if isinstance(location_filter, str) else False,
    ])
    if has_filter:
        try:
            # Use short-lived cache for expensive all-books call
            from app.utils.simple_cache import cache_get, cache_set, get_user_library_version
            version = get_user_library_version(str(current_user.id))
            cache_key = f"all_books_overlay:{current_user.id}:v{version}"
            user_books = cache_get(cache_key)
            if user_books is None:
                user_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
                cache_set(cache_key, user_books, ttl_seconds=120)
        except Exception:
            user_books = []
    else:
        # Cache paginated slice for short TTL
        try:
            from app.utils.simple_cache import cache_get, cache_set, get_user_library_version
            version = get_user_library_version(str(current_user.id))
            cache_key = f"books_page:{current_user.id}:{per_page}:{offset}:{sort_option}:v{version}"
            user_books = cache_get(cache_key)
            if user_books is None:
                user_books = book_service.get_books_with_user_overlay_paginated_sync(str(current_user.id), per_page, offset, sort_option)
                cache_set(cache_key, user_books, ttl_seconds=60)
        except Exception:
            user_books = book_service.get_books_with_user_overlay_paginated_sync(str(current_user.id), per_page, offset, sort_option)
    
    # Add location debugging via debug system
    from app.debug_system import debug_log
    books_with_locations = 0
    books_without_locations = 0
    location_counts = {}
    
    for book in user_books:
        # Handle both dict and object formats for compatibility
        book_title = book.get('title') if isinstance(book, dict) else getattr(book, 'title', 'Unknown Title')
        book_locations = book.get('locations') if isinstance(book, dict) else getattr(book, 'locations', None)
        
        if book_locations:
            books_with_locations += 1
            for location in book_locations:
                # Extract location ID from location object/dict
                loc_id = location.get('id') if isinstance(location, dict) else getattr(location, 'id', location)
                location_counts[loc_id] = location_counts.get(loc_id, 0) + 1
        else:
            books_without_locations += 1
    
    
    # Calculate statistics for filter buttons - handle both dict and object formats
    def get_reading_status(book):
        """Get a canonical reading status for a book.

        Normalizes common variants to one of:
        - 'read'
        - 'currently_reading'
        - 'on_hold'
        - 'plan_to_read'
        Returns None only if no status can be determined.
        """
        if isinstance(book, dict):
            # First try direct field, then nested under ownership, then legacy field
            status = (book.get('reading_status') or
                      book.get('ownership', {}).get('reading_status') or
                      book.get('status'))  # legacy field
        else:
            status = getattr(book, 'reading_status', None)

        # Normalize
        if isinstance(status, str):
            rs = status.strip().lower()
        else:
            rs = status

        if rs in (None, '', 'unknown', 'library_only'):
            # Empty/default status remains empty to reflect no personal status set
            rs = ''
        elif rs in ('reading', 'currently reading'):
            rs = 'currently_reading'
        elif rs in ('onhold', 'on-hold', 'paused'):
            rs = 'on_hold'
        elif rs in ('finished', 'complete', 'completed'):
            rs = 'read'
        elif rs in ('want_to_read', 'wishlist_reading'):
            rs = 'plan_to_read'

        return rs
    
    def get_ownership_status(book):
        if isinstance(book, dict):
            # First try direct field, then nested under ownership
            status = (book.get('ownership_status') or 
                     book.get('ownership', {}).get('ownership_status'))
            return status
        return getattr(book, 'ownership_status', None)
    
    # Global status counts (not page-limited)
    try:
        from app.utils.simple_cache import cache_get, cache_set, get_user_library_version
        _ver = get_user_library_version(str(current_user.id))
        _sc_key = f"status_counts:{current_user.id}:v{_ver}"
        global_counts = cache_get(_sc_key)
        if global_counts is None:
            global_counts = book_service.get_library_status_counts_sync(str(current_user.id))
            cache_set(_sc_key, global_counts, ttl_seconds=180)
    except Exception:
        global_counts = {'read': 0, 'currently_reading': 0, 'plan_to_read': 0, 'on_hold': 0, 'wishlist': 0}

    stats = {
        'total_books': total_books,
        'books_read': int(global_counts.get('read', 0)),
        'currently_reading': int(global_counts.get('currently_reading', 0)),
        'want_to_read': int(global_counts.get('plan_to_read', 0)),
        'on_hold': int(global_counts.get('on_hold', 0)),
        'wishlist': int(global_counts.get('wishlist', 0)),
        # Add location stats (page sample)
        'books_with_locations': books_with_locations,
        'books_without_locations': books_without_locations,
        'location_counts': location_counts
    }
    
    # Apply status filter first
    filtered_books = user_books
    if status_filter and status_filter != 'all':
        if status_filter == 'wishlist':
            filtered_books = [book for book in filtered_books if get_ownership_status(book) == 'wishlist']
        elif status_filter == 'reading':
            # Handle both 'reading' and 'currently_reading' for backwards compatibility
            filtered_books = [book for book in filtered_books if get_reading_status(book) in ['reading', 'currently_reading']]
        else:
            filtered_books = [book for book in filtered_books if get_reading_status(book) == status_filter]
    
    # Apply other filters
    if search_query:
        search_lower = search_query.lower()
        filtered_books = [
            book for book in filtered_books 
            if (search_lower in (book.get('title', '') if isinstance(book, dict) else getattr(book, 'title', '')).lower()) or
               (search_lower in (book.get('author', '') if isinstance(book, dict) else getattr(book, 'author', '')).lower()) or
               (search_lower in (book.get('description', '') if isinstance(book, dict) else getattr(book, 'description', '')).lower())
        ]
    
    if publisher_filter:
        filtered_books = [
            book for book in filtered_books 
            if (book.get('publisher') if isinstance(book, dict) else getattr(book, 'publisher', None)) and 
               publisher_filter.lower() in (book.get('publisher', '') if isinstance(book, dict) else getattr(book, 'publisher', '')).lower()
        ]
    
    if language_filter:
        filtered_books = [
            book for book in filtered_books 
            if (book.get('language') if isinstance(book, dict) else getattr(book, 'language', None)) == language_filter
        ]
    
    if location_filter:
        # Handle locations which are now returned as strings (location names) from KuzuIntegrationService
        filtered_books = [
            book for book in filtered_books 
            if (book.get('locations') if isinstance(book, dict) else getattr(book, 'locations', None)) and any(
                location_filter.lower() in (loc.lower() if isinstance(loc, str) else 
                                           (loc.get('name', '') if isinstance(loc, dict) else getattr(loc, 'name', '')).lower())
                for loc in (book.get('locations', []) if isinstance(book, dict) else getattr(book, 'locations', []))
            )
        ]
    
    if category_filter:
        filtered_books = [
            book for book in filtered_books 
            if (book.get('categories') if isinstance(book, dict) else getattr(book, 'categories', None)) and any(
                category_filter.lower() in (cat.get('name', '') if isinstance(cat, dict) else getattr(cat, 'name', '')).lower() 
                for cat in (book.get('categories', []) if isinstance(book, dict) else getattr(book, 'categories', []))
            )
        ]

    # Apply sorting
    def get_author_name(book):
        """Helper function to get author name safely"""
        if isinstance(book, dict):
            authors = book.get('authors', [])
            author = book.get('author', '')
            if authors and isinstance(authors, list) and len(authors) > 0:
                first_author = authors[0]
                if isinstance(first_author, dict):
                    return first_author.get('name', 'Unknown Author')
                elif hasattr(first_author, 'name'):
                    return first_author.name
                else:
                    return str(first_author)
            elif author:
                return author
            return "Unknown Author"
        else:
            # Handle object format
            if hasattr(book, 'authors') and book.authors:
                # Handle list of Author objects
                if isinstance(book.authors, list) and len(book.authors) > 0:
                    author = book.authors[0]
                    if hasattr(author, 'name'):
                        return author.name
                    elif hasattr(author, 'first_name') and hasattr(author, 'last_name'):
                        return f"{author.first_name} {author.last_name}".strip()
                    else:
                        return str(author)
                else:
                    return str(book.authors)
            elif hasattr(book, 'author') and book.author:
                return book.author
            return "Unknown Author"
    
    def get_author_last_first(book):
        """Helper function to get author name in Last, First format"""
        author_name = get_author_name(book)
        if ',' in author_name:
            return author_name  # Already in Last, First format
        name_parts = author_name.split()
        if len(name_parts) >= 2:
            last_name = name_parts[-1]
            first_names = ' '.join(name_parts[:-1])
            return f"{last_name}, {first_names}"
        return author_name
    
    if sort_option == 'title_asc':
        filtered_books.sort(key=lambda x: (x.get('title', '') if isinstance(x, dict) else getattr(x, 'title', '')).lower())
    elif sort_option == 'title_desc':
        filtered_books.sort(key=lambda x: (x.get('title', '') if isinstance(x, dict) else getattr(x, 'title', '')).lower(), reverse=True)
    elif sort_option == 'author_first_asc':
        filtered_books.sort(key=lambda x: get_author_name(x).lower())
    elif sort_option == 'author_first_desc':
        filtered_books.sort(key=lambda x: get_author_name(x).lower(), reverse=True)
    elif sort_option == 'author_last_asc':
        filtered_books.sort(key=lambda x: get_author_last_first(x).lower())
    elif sort_option == 'author_last_desc':
        filtered_books.sort(key=lambda x: get_author_last_first(x).lower(), reverse=True)
    else:
        # Default to title A-Z
        filtered_books.sort(key=lambda x: (x.get('title', '') if isinstance(x, dict) else getattr(x, 'title', '')).lower())

    # After filters, paginate across full set when filters are active
    if has_filter:
        import math as _math
        filtered_total = len(filtered_books)
        total_pages = max(1, _math.ceil(filtered_total / per_page)) if per_page > 0 else 1
        page = max(1, min(page, total_pages))
        offset = (page - 1) * per_page
        books = filtered_books[offset: offset + per_page]
    else:
        books = filtered_books

    # Convert dictionary books to object-like structures for template compatibility
    converted_books = []
    for book in books:
        if isinstance(book, dict):
            # Create an object-like structure that the template can work with
            class BookObj:
                def __init__(self, data):
                    for key, value in data.items():
                        setattr(self, key, value)
                    # Ensure common attributes have defaults
                    # Note: authors property is derived from contributors, don't set directly
                    if not hasattr(self, 'contributors'):
                        self.contributors = []
                    if not hasattr(self, 'categories'):
                        self.categories = []
                    if not hasattr(self, 'publisher'):
                        self.publisher = None
                    if not hasattr(self, 'series'):
                        self.series = None
                    if not hasattr(self, 'locations'):
                        self.locations = []
                    # Handle ownership data
                    ownership = data.get('ownership', {})
                    for key, value in ownership.items():
                        setattr(self, key, value)
                    # Add normalized fields for template filtering
                    try:
                        setattr(self, 'normalized_reading_status', get_reading_status(data) or '')
                    except Exception:
                        setattr(self, 'normalized_reading_status', (getattr(self, 'reading_status', None) or ''))
                    try:
                        owner = (data.get('ownership_status') or data.get('ownership', {}).get('ownership_status'))
                        owner = owner.strip().lower() if isinstance(owner, str) else owner
                        setattr(self, 'normalized_ownership_status', owner or 'owned')
                    except Exception:
                        setattr(self, 'normalized_ownership_status', (getattr(self, 'ownership_status', None) or 'owned'))
                
                def get_contributors_by_type(self, contribution_type):
                    """Get contributors by type for template compatibility."""
                    if hasattr(self, 'contributors') and self.contributors:
                        return [c for c in self.contributors if getattr(c, 'contribution_type', None) == contribution_type]
                    return []
            
            converted_books.append(BookObj(book))
        else:
            # Ensure normalized fields exist on object instances too
            try:
                setattr(book, 'normalized_reading_status', get_reading_status(book) or '')
            except Exception:
                pass
            try:
                owner = getattr(book, 'ownership_status', None)
                owner = owner.strip().lower() if isinstance(owner, str) else owner
                setattr(book, 'normalized_ownership_status', owner or 'owned')
            except Exception:
                pass
            converted_books.append(book)
    
    books = converted_books

    # Get distinct values for filter dropdowns (ideally from all books; fallback to current page sample for now)
    # Build dropdown options from the full working set (all when filtering, else current page dataset)
    all_books = user_books if has_filter else user_books
    
    categories = set()
    publishers = set()
    languages = set()
    locations = set()

    for book in all_books:
        # Handle categories
        book_categories = book.get('categories', []) if isinstance(book, dict) else getattr(book, 'categories', [])
        if book_categories:
            # book.categories is a list of Category objects, not a string
            for cat in book_categories:
                if isinstance(cat, dict):
                    categories.add(cat.get('name', ''))
                elif hasattr(cat, 'name'):
                    categories.add(cat.name)
                else:
                    categories.add(str(cat))
        
        # Handle publisher
        book_publisher = book.get('publisher') if isinstance(book, dict) else getattr(book, 'publisher', None)
        if book_publisher:
            # Handle Publisher domain object or string
            if isinstance(book_publisher, dict):
                publisher_name = book_publisher.get('name', str(book_publisher))
            elif hasattr(book_publisher, 'name'):
                publisher_name = book_publisher.name
            else:
                publisher_name = str(book_publisher)
            publishers.add(publisher_name)
        
        # Handle language
        book_language = book.get('language') if isinstance(book, dict) else getattr(book, 'language', None)
        if book_language:
            languages.add(book_language)
        
        # Handle locations - they are now returned as strings (location names) from KuzuIntegrationService
        book_locations = book.get('locations', []) if isinstance(book, dict) else getattr(book, 'locations', [])
        if book_locations:
            for loc in book_locations:
                if isinstance(loc, str) and loc:
                    # Location is already a string (location name) and not empty
                    locations.add(loc)
                elif isinstance(loc, dict) and loc.get('name'):
                    locations.add(loc.get('name'))
                elif hasattr(loc, 'name') and getattr(loc, 'name', None):
                    locations.add(getattr(loc, 'name'))
                else:
                    locations.add(str(loc))

    # Get users through Kuzu service layer
    domain_users = user_service.get_all_users_sync() or []
    
    # Convert domain users to simple objects for template compatibility
    users = []
    for domain_user in domain_users:
        user_data = {
            'id': domain_user.id,
            'username': domain_user.username,
            'email': domain_user.email
        }
        users.append(type('User', (), user_data))

    # Optional JSON output for fast client-side rendering
    if request.args.get('format') == 'json':
        # Minimal payload for grid
        payload = []
        for b in books:
            bd = {
                'uid': getattr(b, 'uid', None) if not isinstance(b, dict) else b.get('uid'),
                'title': getattr(b, 'title', '') if not isinstance(b, dict) else b.get('title', ''),
                'author': getattr(b, 'author', '') if not isinstance(b, dict) else b.get('author', ''),
                'cover_url': getattr(b, 'cover_url', None) if not isinstance(b, dict) else b.get('cover_url'),
                'average_rating': getattr(b, 'average_rating', None) if not isinstance(b, dict) else b.get('average_rating'),
                'rating_count': getattr(b, 'rating_count', None) if not isinstance(b, dict) else b.get('rating_count'),
                'normalized_reading_status': getattr(b, 'normalized_reading_status', '') if not isinstance(b, dict) else (b.get('normalized_reading_status') or ''),
                'locations': getattr(b, 'locations', []) if not isinstance(b, dict) else b.get('locations', []),
            }
            payload.append(bd)
        # ETag based on user, page/filter/sort, and version
        from app.utils.simple_cache import get_user_library_version
        version = get_user_library_version(str(current_user.id))
        etag = f"W/\"lib:{current_user.id}:{page}:{rows}:{cols}:{per_page}:{status_filter}:{category_filter}:{publisher_filter}:{language_filter}:{location_filter}:{search_query}:{sort_option}:v{version}\""
        if request.headers.get('If-None-Match') == etag:
            return ('', 304)
        resp = make_response(jsonify({
            'items': payload,
            'page': page,
            'per_page': per_page,
            'total_pages': total_pages,
            'total': (len(filtered_books) if has_filter else total_books)
        }))
        resp.headers['ETag'] = etag
        resp.headers['Cache-Control'] = 'private, max-age=60, stale-while-revalidate=120'
        return resp

    # ETag for HTML response too
    from app.utils.simple_cache import get_user_library_version
    _version = get_user_library_version(str(current_user.id))
    _html_etag = f"W/\"libhtml:{current_user.id}:{page}:{rows}:{cols}:{per_page}:{status_filter}:{category_filter}:{publisher_filter}:{language_filter}:{location_filter}:{search_query}:{sort_option}:v{_version}\""
    if request.headers.get('If-None-Match') == _html_etag:
        return ('', 304)

    resp = make_response(render_template(
        'library_enhanced.html',
        books=books,
        stats=stats,
        page=page,
        per_page=per_page,
        rows=rows,
        cols=cols,
        total_books=(len(filtered_books) if has_filter else total_books),
        total_pages=total_pages,
        has_prev=(page > 1),
        has_next=(page < total_pages),
        categories=sorted([cat for cat in categories if cat is not None and cat != '']),
        publishers=sorted([pub for pub in publishers if pub is not None and pub != '']),
        languages=sorted([lang for lang in languages if lang is not None and lang != '']),
        locations=sorted([loc for loc in locations if loc is not None and loc != '']),
        current_status_filter=status_filter,
        current_category=category_filter,
        current_publisher=publisher_filter,
        current_language=language_filter,
        current_location=location_filter,
        current_search=search_query,
        current_sort=sort_option,
        users=users
    ))
    resp.headers['ETag'] = _html_etag
    resp.headers['Cache-Control'] = 'private, max-age=60, stale-while-revalidate=120'
    # Hint the browser to warm the next page JSON in the background
    try:
        if page < total_pages:
            from urllib.parse import urlencode
            # Preserve existing args but override page and add format=json
            # request.args may be a MultiDict; use flat values for cleanliness
            flat_params = {k: request.args.get(k) for k in request.args.keys()}
            flat_params['page'] = str(page + 1)
            flat_params['format'] = 'json'
            next_json_url = f"{request.base_url}?{urlencode(flat_params)}"
            # Append to existing Link header if present
            existing_link = resp.headers.get('Link')
            preload_hint = f"<{next_json_url}>; rel=preload; as=fetch"
            if existing_link:
                resp.headers['Link'] = existing_link + ", " + preload_hint
            else:
                resp.headers['Link'] = preload_hint
    except Exception:
        pass
    return resp

@book_bp.route('/public-library')
def public_library():
    filter_status = request.args.get('filter', 'all')
    
    # Use Kuzu service to get all books from all users
    # TODO: Implement public library functionality in Kuzu service
    # For now, return empty list
    books = []
    
    return render_template('public_library.html', books=books, filter_status=filter_status)

@book_bp.route('/book/<uid>/edit', methods=['GET', 'POST'])
@login_required
def edit_book(uid):
    try:
        # Get book through service layer
        user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
        if not user_book:
            abort(404)

        # Early GET handling: render edit form without touching POST-specific fields
        if request.method == 'GET':
            book_categories = []
            try:
                book_id = user_book.get('id') if isinstance(user_book, dict) else getattr(user_book, 'id', None)
                if book_id:
                    book_categories = book_service.get_book_categories_sync(book_id)
            except Exception:
                pass
            # Normalize to object for template
            if isinstance(user_book, dict):
                class BookObj:
                    def __init__(self, data):
                        for k, v in data.items():
                            setattr(self, k, v)
                        if not hasattr(self, 'contributors'):
                            self.contributors = []
                        if not hasattr(self, 'categories'):
                            self.categories = []
                        if not hasattr(self, 'publisher'):
                            self.publisher = None
                    def get_contributors_by_type(self, contribution_type):
                        return []
                user_book_obj = BookObj(user_book)
            else:
                user_book_obj = user_book
            return render_template('edit_book_enhanced.html', book=user_book_obj, book_categories=book_categories)
        
        if request.method == 'POST':
            # Refactored: delegate POST handling to helper orchestrator
            response = _handle_edit_book_post(uid, user_book, request.form)
            if response is not None:
                return response
        
        new_isbn13 = request.form.get('isbn13', '').strip() or None
        new_isbn10 = request.form.get('isbn10', '').strip() or None
        
        # Process contributors
        contributors = []
        contributor_data = {}
        
        # Parse contributor form data
        for key, value in request.form.items():
            if key.startswith('contributors[') and '][' in key:
                # Extract index and field from key like "contributors[0][name]"
                parts = key.split('][')
                if len(parts) == 2:
                    index_part = parts[0].replace('contributors[', '')
                    field = parts[1].replace(']', '')
                    
                    if index_part not in contributor_data:
                        contributor_data[index_part] = {}
                    contributor_data[index_part][field] = value
        
        # Debug: Log contributor data received
        current_app.logger.info(f"[CONTRIB_DEBUG] Received contributor data: {contributor_data}")
        
        # Process categories
        categories = []
        category_data = {}
        
        # Parse category form data
        for key, value in request.form.items():
            if key.startswith('categories[') and '][' in key:
                # Extract index and field from key like "categories[0][name]"
                parts = key.split('][')
                if len(parts) == 2:
                    index_part = parts[0].replace('categories[', '')
                    field = parts[1].replace(']', '')
                    
                    if index_part not in category_data:
                        category_data[index_part] = {}
                    category_data[index_part][field] = value
        
        # Create category list for processing - handle new category creation
        for cat_data in category_data.values():
            if cat_data.get('name'):
                category_name = cat_data['name'].strip()
                is_new = cat_data.get('is_new', 'false').lower() == 'true'
                
                # If it's a new category, try to create it first
                if is_new:
                    try:
                        # Use the same service that handles category creation
                        new_category_data = {
                            'name': category_name,
                            'description': '',  # Default empty description
                            'parent_id': None   # Default to root level
                        }
                        created_category = book_service.create_category_sync(new_category_data)
                        if created_category:
                            current_app.logger.info(f"Created new category: {category_name}")
                        else:
                            current_app.logger.warning(f"Failed to create new category: {category_name}")
                    except Exception as e:
                        current_app.logger.error(f"Error creating category {category_name}: {e}")
                        # Continue anyway - the raw_categories processing might still handle it
                
                categories.append(category_name)  # Add to list for raw_categories processing

        # Also accept hierarchical raw category paths from hidden field (JSON array)
        raw_categories_payload = None
        try:
            raw_cats_json = request.form.get('raw_categories')
            if raw_cats_json:
                import json as _json
                raw_cats_list = _json.loads(raw_cats_json)
                if isinstance(raw_cats_list, list):
                    # Dedupe and clean raw list only; keep categories (chips) unchanged
                    cleaned = []
                    seen = set()
                    for item in raw_cats_list:
                        if isinstance(item, str):
                            s = item.strip()
                            if s and s not in seen:
                                cleaned.append(s)
                                seen.add(s)
                    raw_categories_payload = cleaned if cleaned else None
        except Exception as _e:
            current_app.logger.warning(f"[CATEGORIES] Failed to parse raw_categories JSON: {_e}")
        
        # Create BookContribution objects
        for contrib_index, contrib in contributor_data.items():
            if contrib.get('name'):
                current_app.logger.info(f"[CONTRIB_DEBUG] Processing contributor {contrib_index}: {contrib}")
                from app.domain.models import Person, BookContribution, ContributionType
                
                person_name = contrib['name']
                
                try:
                    # Always use find_or_create approach - the most reliable method
                    
                    # Use SafeKuzuManager for all database operations
                    safe_manager = get_safe_kuzu_manager()
                    
                    # Search for existing person by name (same as repository method)
                    normalized_name = person_name.strip().lower()
                    
                    # Query for existing person by normalized name
                    find_person_query = """
                    MATCH (p:Person)
                    WHERE toLower(p.name) = $normalized_name OR toLower(p.normalized_name) = $normalized_name
                    RETURN p.id as id, p.name as name, p.normalized_name as normalized_name,
                           p.birth_year as birth_year, p.death_year as death_year,
                           p.birth_place as birth_place, p.bio as bio, p.website as website
                    LIMIT 1
                    """
                    
                    person_result = safe_manager.execute_query(find_person_query, {'normalized_name': normalized_name})
                    person_data_list = _convert_query_result_to_list(person_result)
                    
                    person = None
                    if person_data_list:
                        raw_person_data = person_data_list[0]
                        # Some query result conversions produce col_0..col_N keys instead of aliases
                        # Query column order (see find_person_query): id, name, normalized_name, birth_year, death_year, birth_place, bio, website
                        if 'id' not in raw_person_data and 'col_0' in raw_person_data:
                            person_data = {
                                'id': raw_person_data.get('col_0'),
                                'name': raw_person_data.get('col_1'),
                                'normalized_name': raw_person_data.get('col_2'),
                                'birth_year': raw_person_data.get('col_3'),
                                'death_year': raw_person_data.get('col_4'),
                                'birth_place': raw_person_data.get('col_5'),
                                'bio': raw_person_data.get('col_6'),
                                'website': raw_person_data.get('col_7'),
                            }
                        else:
                            person_data = raw_person_data

                        # Defensive: some Kuzu result rows may be returned as list/tuple rather than dict
                        if isinstance(person_data, (list, tuple)):
                            # Expected column order from query: id, name, normalized_name, birth_year, death_year, birth_place, bio, website
                            # Build dict safely with length checks
                            cols = list(person_data)
                            person_data = {
                                'id': cols[0] if len(cols) > 0 else None,
                                'name': cols[1] if len(cols) > 1 else None,
                                'normalized_name': cols[2] if len(cols) > 2 else None,
                                'birth_year': cols[3] if len(cols) > 3 else None,
                                'death_year': cols[4] if len(cols) > 4 else None,
                                'birth_place': cols[5] if len(cols) > 5 else None,
                                'bio': cols[6] if len(cols) > 6 else None,
                                'website': cols[7] if len(cols) > 7 else None,
                            }

                        # Convert back to Person object using normalized mapping
                        person = Person(
                            id=person_data.get('id'),
                            name=person_data.get('name', '') or '',
                            normalized_name=(person_data.get('normalized_name') or '').strip().lower(),
                            birth_year=person_data.get('birth_year'),
                            death_year=person_data.get('death_year'),
                            birth_place=person_data.get('birth_place'),
                            bio=person_data.get('bio'),
                            website=person_data.get('website'),
                            created_at=datetime.now(),  # Set defaults for dates
                            updated_at=datetime.now()
                        )
                    
                    # If not found, create new person using clean repository (with auto-fetch)
                    if not person:
                        from app.infrastructure.kuzu_repositories import KuzuPersonRepository
                        from app.utils.safe_kuzu_manager import safe_get_connection
                        
                        try:
                            # Use clean repository with auto-fetch capability
                            with safe_get_connection(user_id=str(current_user.id), operation="create_contributor") as kuzu_connection:
                                person_repo = KuzuPersonRepository()
                                
                                # Create person using repository (will auto-fetch OpenLibrary metadata)
                                person_dict = {
                                    'id': str(uuid.uuid4()),
                                    'name': person_name,
                                    'normalized_name': normalized_name,
                                    'created_at': datetime.now().isoformat(),
                                    'updated_at': datetime.now().isoformat()
                                }
                            
                            created_person = person_repo.create(person_dict)
                            if created_person:
                                # Convert back to Person object for the rest of the workflow
                                person = Person(
                                    id=created_person.get('id'),
                                    name=created_person.get('name', ''),
                                    normalized_name=created_person.get('normalized_name', ''),
                                    birth_year=created_person.get('birth_year'),
                                    death_year=created_person.get('death_year'),
                                    birth_place=created_person.get('birth_place'),
                                    bio=created_person.get('bio'),
                                    website=created_person.get('website'),
                                    openlibrary_id=created_person.get('openlibrary_id'),
                                    image_url=created_person.get('image_url'),
                                    created_at=datetime.now(),
                                    updated_at=datetime.now()
                                )
                            else:
                                continue
                        except Exception as repo_error:
                            continue
                    
                    # Validate person has valid ID
                    if not person or not person.id:
                        continue
                    
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    continue
                
                # Map contribution type
                contrib_type_map = {
                    'authored': ContributionType.AUTHORED,
                    'edited': ContributionType.EDITED,
                    'translated': ContributionType.TRANSLATED,
                    'illustrated': ContributionType.ILLUSTRATED,
                    'narrated': ContributionType.NARRATED,
                    'gave_foreword': ContributionType.GAVE_FOREWORD,
                    'gave_introduction': ContributionType.GAVE_INTRODUCTION,
                    'gave_afterword': ContributionType.GAVE_AFTERWORD,
                    'compiled': ContributionType.COMPILED,
                    'contributed': ContributionType.CONTRIBUTED,
                    'co_authored': ContributionType.CO_AUTHORED,
                    'ghost_wrote': ContributionType.GHOST_WROTE
                }
                
                contrib_type = contrib_type_map.get(contrib.get('type', 'authored'), ContributionType.AUTHORED)
                
                current_app.logger.info(f"[CONTRIB_DEBUG] Creating contribution: person={person.name}, type={contrib_type}")
                
                contribution = BookContribution(
                    person=person,
                    contribution_type=contrib_type,
                    created_at=datetime.now()
                )
                contributors.append(contribution)
        
        update_data = {
            'title': request.form['title'],
            'subtitle': request.form.get('subtitle', '').strip() or None,
            'description': request.form.get('description', '').strip() or None,
            'published_date': _convert_published_date_to_date(request.form.get('published_date', '').strip()) if request.form.get('published_date', '').strip() else None,
            'page_count': int(page_count_str) if (page_count_str := request.form.get('page_count', '').strip()) else None,
            'language': request.form.get('language', '').strip() or 'en',
            'cover_url': request.form.get('cover_url', '').strip() or None,
            'isbn13': new_isbn13,
            'isbn10': new_isbn10,
            'series': request.form.get('series', '').strip() or None,
            'series_volume': request.form.get('series_volume', '').strip() or None,
            'series_order': int(series_order_str) if (series_order_str := request.form.get('series_order', '').strip()) else None,
            'contributors': contributors,
            'raw_categories': (raw_categories_payload if raw_categories_payload is not None else (categories if categories else None)),
            # Additional metadata fields - these are Book properties, not user-specific
            'publisher': request.form.get('publisher', '').strip() or None,
            'asin': request.form.get('asin', '').strip() or None,
            'google_books_id': request.form.get('google_books_id', '').strip() or None,
            'openlibrary_id': request.form.get('openlibrary_id', '').strip() or None,
            'average_rating': float(avg_rating_str) if (avg_rating_str := request.form.get('average_rating', '').strip()) else None,
            'rating_count': int(rating_count_str) if (rating_count_str := request.form.get('rating_count', '').strip()) else None,
            # User-specific fields
            'reading_status': request.form.get('reading_status', '').strip() or None,
            'ownership_status': request.form.get('ownership_status', '').strip() or None,
            'media_type': request.form.get('media_type', '').strip() or None,
            'personal_notes': request.form.get('personal_notes', '').strip() or None,
            'review': request.form.get('review', '').strip() or None,
        }
        
    # Remove properties that belong to relationships from the node update payload (avoids Kuzu errors)
        # They are handled elsewhere (e.g., publisher via separate relationship updates)
        if 'publisher' in update_data:
            update_data.pop('publisher', None)

        # Handle user rating
        user_rating = request.form.get('user_rating', '').strip()
        if user_rating:
            try:
                update_data['user_rating'] = float(user_rating)
            except ValueError:
                pass  # Invalid rating, skip it
        
        # Remove None values except for specific fields that can be null
        filtered_data = {}
        for k, v in update_data.items():
            if k in ['contributors', 'raw_categories', 'series', 'series_volume', 'series_order', 'publisher', 'asin', 'google_books_id', 'openlibrary_id', 'average_rating', 'rating_count', 'cover_url'] or v is not None:
                filtered_data[k] = v
        
        try:
            success = book_service.update_book_sync(uid, str(current_user.id), **filtered_data)
        except Exception as e:
            import traceback
            traceback.print_exc()
            flash(f'Error updating book: {str(e)}', 'error')
            return redirect(url_for('book.view_book_enhanced', uid=uid))
        
        # Handle location update separately
        location_id = request.form.get('location_id', '').strip()
        if location_id is not None:  # Allow empty string to clear location
            # Use the location service to update the book location
            try:
                from app.location_service import LocationService
                
                location_service = LocationService()
                
                # Convert empty string to None for clearing location
                location_success = location_service.set_book_location(
                    uid, 
                    location_id if location_id else None, 
                    str(current_user.id)
                )
                if not location_success:
                    pass
            except Exception as e:
                pass
        
        if success:
            flash('Book updated successfully.', 'success')
        else:
            flash('Failed to update book.', 'error')
        return redirect(url_for('book.view_book_enhanced', uid=uid))
    
        # GET request handling
        # Get book categories for editing
        book_categories = []
        try:
            book_id = user_book.get('id') if isinstance(user_book, dict) else getattr(user_book, 'id', None)
            if book_id:
                book_categories = book_service.get_book_categories_sync(book_id)
        except Exception as e:
            pass
        
        # Convert dictionary to object-like structure for template compatibility
        if isinstance(user_book, dict):
            class BookObj:
                def __init__(self, data):
                    for key, value in data.items():
                        setattr(self, key, value)
                    # Ensure common attributes have defaults
                    # Note: authors property is derived from contributors, don't set directly
                    if not hasattr(self, 'contributors'):
                        self.contributors = []
                    if not hasattr(self, 'categories'):
                        self.categories = []
                    if not hasattr(self, 'publisher'):
                        self.publisher = None
                    # Handle ownership data
                    ownership = data.get('ownership', {})
                    for key, value in ownership.items():
                        setattr(self, key, value)
                
                def get_contributors_by_type(self, contribution_type):
                    """Get contributors filtered by type."""
                    if not hasattr(self, 'contributors') or not self.contributors:
                        return []
                    
                    # Handle both string and enum contribution types
                    type_str = contribution_type.upper() if isinstance(contribution_type, str) else str(contribution_type).upper()
                    
                    filtered = []
                    for contributor in self.contributors:
                        if isinstance(contributor, dict):
                            role = contributor.get('role', '').upper()
                            if role == type_str or role == type_str.replace('ED', ''):  # Handle past tense
                                filtered.append(contributor)
                        else:
                            # Handle object-like contributors
                            role = getattr(contributor, 'role', '').upper()
                            if role == type_str or role == type_str.replace('ED', ''):
                                filtered.append(contributor)
                    
                    return filtered
            
            user_book = BookObj(user_book)
        
        return render_template('edit_book_enhanced.html', book=user_book, book_categories=book_categories)

    except Exception as e:
        import traceback
        traceback.print_exc()
        flash(f'Error processing request: {str(e)}', 'error')
        return redirect(url_for('book.view_book_enhanced', uid=uid))


@book_bp.route('/book/<uid>/enhanced')
@login_required
def view_book_enhanced(uid):
    """Enhanced book view with new status system."""
    
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    
    if not user_book:
        abort(404)

    title = user_book.get('title', 'Unknown Title') if isinstance(user_book, dict) else getattr(user_book, 'title', 'Unknown Title')

    # Convert dictionary to object-like structure for template compatibility
    if isinstance(user_book, dict):
        # Create an object-like structure that the template can work with
        class BookObj:
            def __init__(self, data):
                for key, value in data.items():
                    setattr(self, key, value)
                # Ensure common attributes have defaults
                # Note: authors property is derived from contributors, don't set directly
                if not hasattr(self, 'contributors'):
                    self.contributors = []
                if not hasattr(self, 'categories'):
                    self.categories = []
                if not hasattr(self, 'publisher'):
                    self.publisher = None
                if not hasattr(self, 'series'):
                    self.series = None
                if not hasattr(self, 'custom_metadata'):
                    self.custom_metadata = {}
                # DEBUG: Log all cover-related fields
                cover_url_value = getattr(self, 'cover_url', None)
                
                # Cover URL field is already correct (cover_url is the database field)
                # No field mapping needed since we're using the correct field name everywhere
                
                # DEBUG: Log all ISBN-related fields
                isbn13_value = getattr(self, 'isbn13', None)
                isbn10_value = getattr(self, 'isbn10', None)
                isbn_value = getattr(self, 'isbn', None)
                
                # Handle ISBN field consistency - Template expects isbn13/isbn10 specifically
                if not hasattr(self, 'isbn') and (hasattr(self, 'isbn13') or hasattr(self, 'isbn10')):
                    self.isbn = getattr(self, 'isbn13', None) or getattr(self, 'isbn10', None)
                
                # REVERSE NORMALIZATION: If we have 'isbn' but not isbn13/isbn10, normalize it
                isbn_value = getattr(self, 'isbn', None)
                if isbn_value and (not hasattr(self, 'isbn13') or not getattr(self, 'isbn13')):
                    import re
                    # Clean and normalize the ISBN
                    clean_isbn = re.sub(r'[^0-9X]', '', str(isbn_value).upper())
                    if len(clean_isbn) == 13:
                        self.isbn13 = clean_isbn
                    elif len(clean_isbn) == 10:
                        self.isbn10 = clean_isbn
                
                # Ensure both fields exist for template (even if None)
                if not hasattr(self, 'isbn13'):
                    self.isbn13 = None
                if not hasattr(self, 'isbn10'):
                    self.isbn10 = None
                
                # DEBUG: Log all category-related fields
                categories_value = getattr(self, 'categories', None)
                genre_value = getattr(self, 'genre', None)
                genres_value = getattr(self, 'genres', None)
                
                # Handle category field consistency
                if not hasattr(self, 'categories') or not getattr(self, 'categories', None):
                    if hasattr(self, 'genres') and getattr(self, 'genres', None):
                        self.categories = getattr(self, 'genres')
                    elif hasattr(self, 'genre') and getattr(self, 'genre', None):
                        genre_val = getattr(self, 'genre')
                        self.categories = [genre_val] if isinstance(genre_val, str) else genre_val
                
                # Final debug
                final_cover_url = getattr(self, 'cover_url', None)
                final_cover_url_alt = getattr(self, 'cover_url', None)
                
                # Handle location field - if location_id exists but locations is empty, get location name
                location_id = getattr(self, 'location_id', None)
                if hasattr(self, 'location_id') and location_id and (not hasattr(self, 'locations') or not getattr(self, 'locations', None)):
                    try:
                        from app.location_service import LocationService
                        
                        location_service = LocationService()
                        
                        # Get all available locations (universal, no user dependency)
                        all_locations = location_service.get_all_locations()
                        
                        # Find the location object by ID
                        for location in all_locations:
                            if hasattr(location, 'id') and str(location.id) == str(location_id):
                                self.locations = [{'id': location.id, 'name': location.name}]
                                break
                        
                        # If location not found by ID, check if location_id is actually a name
                        if not hasattr(self, 'locations') or not self.locations:
                            for location in all_locations:
                                if hasattr(location, 'name') and str(location.name) == str(location_id):
                                    self.locations = [{'id': location.id, 'name': location.name}]
                                    break
                                    
                    except Exception as e:
                        print(f"Error populating location data: {e}")
                        # Fallback: treat location_id as the name
                        if location_id:
                            self.locations = [{'id': location_id, 'name': location_id}]
                
                # Handle ownership data
                ownership = data.get('ownership', {})
                for key, value in ownership.items():
                    setattr(self, key, value)
            
            def get_contributors_by_type(self, contribution_type):
                """Get contributors filtered by type."""
                if not hasattr(self, 'contributors') or not self.contributors:
                    return []
                
                # Handle both string and enum contribution types
                type_str = contribution_type.upper() if isinstance(contribution_type, str) else str(contribution_type).upper()
                
                filtered = []
                for contributor in self.contributors:
                    if isinstance(contributor, dict):
                        role = contributor.get('role', '').upper()
                        if role == type_str or role == type_str.replace('ED', ''):  # Handle past tense
                            filtered.append(contributor)
                    else:
                        # Handle object-like contributors
                        role = getattr(contributor, 'role', '').upper()
                        if role == type_str or role == type_str.replace('ED', ''):
                            filtered.append(contributor)
                
                return filtered
        
        user_book = BookObj(user_book)
        
        # Debug ISBN fields specifically and ensure final normalization
        isbn13 = getattr(user_book, 'isbn13', None)
        isbn10 = getattr(user_book, 'isbn10', None) 
        isbn_generic = getattr(user_book, 'isbn', None)
        cover_url = getattr(user_book, 'cover_url', None)
        
        
        # FINAL NORMALIZATION: Ensure ISBN fields are available for template
        if not isbn13 and not isbn10 and isbn_generic:
            import re
            clean_isbn = re.sub(r'[^0-9X]', '', str(isbn_generic).upper())
            if len(clean_isbn) == 13:
                user_book.isbn13 = clean_isbn
            elif len(clean_isbn) == 10:
                user_book.isbn10 = clean_isbn
        
        # Ensure both fields exist (template expects them)
        if not hasattr(user_book, 'isbn13'):
            user_book.isbn13 = isbn13
        if not hasattr(user_book, 'isbn10'):
            user_book.isbn10 = isbn10
        if not hasattr(user_book, 'isbn'):
            user_book.isbn = isbn13 or isbn10  # Fallback for any other template expectations

    # Get book authors
    try:
        book_id = getattr(user_book, 'id', None)
        if book_id and (hasattr(user_book, 'contributors') and not user_book.contributors):
            # Fetch authors from database using the same pattern as categories
            from app.utils.safe_kuzu_manager import safe_get_connection
            from app.domain.models import BookContribution, Person, ContributionType
            
            with safe_get_connection(user_id=str(current_user.id), operation="fetch_book_authors") as kuzu_connection:
                
                query = """
                MATCH (p:Person)-[rel:AUTHORED]->(b:Book {id: $book_id})
                RETURN p.name as name, p.id as id, rel.role as role, rel.order_index as order_index
                ORDER BY rel.order_index ASC
                """
                
                results = kuzu_connection.execute(query, {"book_id": book_id})
                
                # Handle both single QueryResult and list[QueryResult]
                if isinstance(results, list):
                    result = results[0] if results else None
                else:
                    result = results
                
                contributors = []
                if result:
                    result_list = _convert_query_result_to_list(result)
                    for row_data in result_list:
                        person_name = row_data.get('name', '')
                        person_id = row_data.get('id', '')
                        order_index = row_data.get('order_index', 0)
                        
                        if person_name and person_id:  # Ensure both name and id exist
                            person = Person(
                                id=person_id,
                                name=person_name,
                                normalized_name=person_name.lower()
                            )
                            
                            contribution = BookContribution(
                                person_id=person_id,
                                book_id=book_id,
                                contribution_type=ContributionType.AUTHORED,
                                order=order_index,
                                person=person
                            )
                            
                            contributors.append(contribution)
            
            # Update the book object with the fetched contributors
            user_book.contributors = contributors
            
            # Authors property is automatically derived from contributors
            # No need to set it manually since it's a read-only property
                    
        else:
            # Even if contributors exist, make sure they have the proper structure
            # Authors property is automatically derived from contributors
            if hasattr(user_book, 'contributors') and user_book.contributors:
                pass
    except Exception as e:
        current_app.logger.error(f"Error loading book authors: {e}")

    # Get book categories
    book_categories = []
    try:
        book_id = getattr(user_book, 'id', None)
        if book_id:
            book_categories = book_service.get_book_categories_sync(book_id)
        else:
            pass
    except Exception as e:
        current_app.logger.error(f"Error loading book categories: {e}")

    # Get custom metadata for display
    global_metadata_display = []
    personal_metadata_display = []  # Initialize as empty list
    
    # Get available custom fields for edit mode
    personal_fields = []
    global_fields = []
    current_metadata = {}
    
    try:
        # Get custom metadata using the custom field service
        custom_metadata = custom_field_service.get_custom_metadata_sync(uid, str(current_user.id))
        current_metadata = custom_metadata or {}
        
        if custom_metadata:
            
            # Separate global and personal metadata for display
            global_metadata = {}
            personal_metadata = {}
            
            # Separate fields based on their definitions
            for field_name, field_value in custom_metadata.items():
                if field_value is not None and field_value != '':
                    field_def = custom_field_service._get_field_definition(field_name)
                    if field_def and field_def.get('is_global', False):
                        global_metadata[field_name] = field_value
                    else:
                        personal_metadata[field_name] = field_value
            
            # Convert to display format separately
            global_metadata_display = custom_field_service.get_custom_metadata_for_display(
                global_metadata
            ) or []
            personal_metadata_display = custom_field_service.get_custom_metadata_for_display(
                personal_metadata
            ) or []
            
        else:
            pass
            
        # Get available custom fields for the edit mode
        personal_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=False) or []
        global_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=True) or []
        
    except Exception as e:
        current_app.logger.error(f"Error loading custom metadata for display: {e}")
    
    # Get user locations for the location dropdown and debug info
    user_locations = []
    try:
        from app.location_service import LocationService
        
        location_service = LocationService()
        # Get all available locations, not just those with books
        user_locations = location_service.get_all_locations()
    except Exception as e:
        current_app.logger.error(f"Error loading user locations: {e}")
    
    # Prepare template data
    template_data = {
        'book': user_book,
        'book_categories': book_categories,
        'global_metadata_display': global_metadata_display,
        'personal_metadata_display': personal_metadata_display,
        'user_locations': user_locations,
        'personal_fields': personal_fields,
        'global_fields': global_fields,
        'current_metadata': current_metadata
    }
    
    # Get all persons for contributor search
    all_persons = book_service.list_all_persons_sync()
    if not isinstance(all_persons, list):
        all_persons = []
    
    template_data.update({
        'all_persons': all_persons
    })
    
    
    return render_template(
        'view_book_enhanced.html', 
        book=user_book,
        book_categories=book_categories,
        global_metadata_display=global_metadata_display,
        personal_metadata_display=personal_metadata_display,
        user_locations=user_locations,
        personal_fields=personal_fields,
        global_fields=global_fields,
        current_metadata=current_metadata,
        all_persons=all_persons
    )


@book_bp.route('/book/<uid>/update_details', methods=['POST'])
@login_required
def update_book_details(uid):
    """Update book details including new status system."""
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
    
    update_data = {}
    
    # Reading status
    reading_status = request.form.get('reading_status')
    if reading_status:
        update_data['reading_status'] = reading_status
    
    # Ownership status
    ownership_status = request.form.get('ownership_status')
    if ownership_status:
        update_data['ownership_status'] = ownership_status
    
    # Media type
    media_type = request.form.get('media_type')
    if media_type:
        update_data['media_type'] = media_type
    
    # Location (handle separately as it's a relationship property, not a book property)
    location_id = request.form.get('location_id')
    
    # Borrowing details
    borrowed_from = request.form.get('borrowed_from', '').strip()
    borrowed_due_date = request.form.get('borrowed_due_date')
    if ownership_status == 'borrowed':
        update_data['borrowed_from'] = borrowed_from if borrowed_from else None
        if borrowed_due_date:
            update_data['borrowed_due_date'] = datetime.strptime(borrowed_due_date, '%Y-%m-%d').date()
    
    # Loaning details
    loaned_to = request.form.get('loaned_to', '').strip()
    loaned_due_date = request.form.get('loaned_due_date')
    if ownership_status == 'loaned':
        update_data['loaned_to'] = loaned_to if loaned_to else None
        if loaned_due_date:
            update_data['loaned_due_date'] = datetime.strptime(loaned_due_date, '%Y-%m-%d').date()
    
    # Update reading dates based on status
    start_date = user_book.get('start_date') if isinstance(user_book, dict) else getattr(user_book, 'start_date', None)
    finish_date = user_book.get('finish_date') if isinstance(user_book, dict) else getattr(user_book, 'finish_date', None)
    
    if reading_status == 'reading' and not start_date:
        update_data['start_date'] = date.today()
    elif reading_status == 'read' and not finish_date:
        update_data['finish_date'] = date.today()
        if not start_date:
            update_data['start_date'] = date.today()
    
    # Use service layer to update
    try:
        updated_book = book_service.update_book_sync(uid, str(current_user.id), **update_data)
        
        # Handle location update separately
        if location_id is not None:  # Allow empty string to clear location
            # Use the location service to update the book location
            try:
                from app.location_service import LocationService
                
                location_service = LocationService()
                
                # Convert empty string to None for clearing location
                location_success = location_service.set_book_location(
                    uid, 
                    location_id if location_id.strip() else None, 
                    str(current_user.id)
                )
                if not location_success:
                        pass
            except Exception as e:
                pass
        else:
            pass
        
        if updated_book is not None:
            flash('Book details updated successfully.', 'success')
            
            # Redirect back to library with appropriate filter if requested
            redirect_to_library = request.form.get('redirect_to_library')
            if redirect_to_library:
                # Determine the appropriate filter based on the new reading status
                if reading_status:
                    if reading_status == 'wishlist':
                        return redirect(url_for('main.library', status_filter='wishlist'))
                    else:
                        return redirect(url_for('main.library', status_filter=reading_status))
                elif ownership_status == 'wishlist':
                    return redirect(url_for('main.library', status_filter='wishlist'))
                else:
                    return redirect(url_for('main.library'))
        else:
            flash('Failed to update book details.', 'error')
    except Exception as e:
        flash(f'Error updating book: {str(e)}', 'error')
    
    return redirect(url_for('book.view_book_enhanced', uid=uid))


@book_bp.route('/book/<uid>/replace_cover', methods=['POST'])
@login_required
def replace_cover(uid):
    """Replace the book's cover image with a new image from URL."""
    try:
        user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
        if not user_book:
            return jsonify({'success': False, 'error': 'Book not found'}), 404

        data = request.get_json()
        new_cover_url = data.get('new_cover_url')
        allow_downgrade = bool(data.get('allow_downgrade'))

        if not new_cover_url:
            return jsonify({'success': False, 'error': 'No cover URL provided'}), 400

        # Store old cover URL for cleanup
        old_cover_url = user_book.cover_url

        # Download, process, and cache the new cover image (unified pipeline)
        try:
            # Normalize source URL before processing to ensure consistent final asset
            try:
                from app.utils.book_utils import normalize_cover_url
                new_cover_url = normalize_cover_url(new_cover_url)
            except Exception:
                pass
            current_app.logger.info(f"[COVER][REPLACE] START uid={uid} src={new_cover_url}")
            if not isinstance(new_cover_url, str):
                return jsonify({'success': False, 'error': 'Invalid cover URL'}), 400
            new_cached_cover_url = process_image_from_url(new_cover_url)
            abs_cover_url = new_cached_cover_url
            if new_cached_cover_url.startswith('/'):
                abs_cover_url = request.host_url.rstrip('/') + new_cached_cover_url

            # Optional downgrade protection: if existing local cover is significantly larger than new one, skip replacement
            try:
                if old_cover_url and not allow_downgrade:
                    # Only evaluate if both old and new are locally cached covers
                    def _cover_file_size(url: str) -> int:
                        try:
                            if not url:
                                return 0
                            if url.startswith('http://') or url.startswith('https://'):
                                return 0  # remote, skip
                            fname = url.split('/')[-1]
                            fpath = get_covers_dir() / fname
                            if fpath.exists():
                                return fpath.stat().st_size
                            return 0
                        except Exception:
                            return 0

                    old_size = _cover_file_size(old_cover_url)
                    new_size = _cover_file_size(new_cached_cover_url)
                    downgrade = False
                    if old_size and new_size:
                        # Heuristics: new very small absolute OR less than 50% of old
                        if (new_size < 12000 and old_size > 20000) or (new_size / old_size) < 0.5:
                            downgrade = True
                    if downgrade:
                        # Remove newly created (inferior) file to avoid clutter
                        try:
                            nf = get_covers_dir() / new_cached_cover_url.split('/')[-1]
                            if nf.exists():
                                nf.unlink()
                        except Exception:
                            pass
                        current_app.logger.info(
                            f"[COVER][REPLACE][DOWNGRADE_SKIP] uid={uid} old_size={old_size} new_size={new_size} kept_old_cover"
                        )
                        return jsonify({
                            'success': True,
                            'cover_url': old_cover_url,
                            'message': 'Kept existing higher-quality cover (skip downgrade)'
                        })
            except Exception as downgrade_err:
                current_app.logger.debug(f"[COVER][REPLACE][DOWNGRADE_CHECK_FAIL] uid={uid} err={downgrade_err}")

            book_service.update_book_sync(user_book.uid, str(current_user.id), cover_url=abs_cover_url)
            current_app.logger.info(f"[COVER][REPLACE] STORED uid={uid} cached={new_cached_cover_url}")

            # Clean up old cover file if it exists and is a local file
            if old_cover_url and (old_cover_url.startswith('/covers/') or old_cover_url.startswith('/static/covers/')):
                try:
                    old_filename = old_cover_url.split('/')[-1]
                    old_filepath = get_covers_dir() / old_filename
                    if old_filepath.exists():
                        old_filepath.unlink()
                        current_app.logger.debug(f"Cleaned up old cover file: {old_filename}")
                except Exception as cleanup_error:
                    current_app.logger.debug(f"Failed to clean up old cover file: {cleanup_error}")

            current_app.logger.info(f"[COVER][REPLACE] DONE uid={uid} src={new_cover_url}")

            return jsonify({
                'success': True,
                'cover_url': new_cached_cover_url,
                'message': 'Cover updated successfully'
            })

        except requests.RequestException as e:
            current_app.logger.error(f"[COVER][REPLACE] REQUEST_FAIL uid={uid} src={new_cover_url} err={e}")
            return jsonify({'success': False, 'error': 'Failed to download new cover image'}), 500
        except Exception as e:
            current_app.logger.error(f"[COVER][REPLACE] FAIL uid={uid} src={new_cover_url} err={e}")
            return jsonify({'success': False, 'error': 'Failed to process new cover image'}), 500

    except Exception as e:
        current_app.logger.error(f"Error replacing cover: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@book_bp.route('/book/<uid>/upload_cover', methods=['POST'])
@login_required
def upload_cover(uid):
    """Upload a cover image file from user's device."""
    try:
        user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
        if not user_book:
            return jsonify({'success': False, 'error': 'Book not found'}), 404
        
        # Check if file was uploaded
        if 'cover_file' not in request.files:
            return jsonify({'success': False, 'error': 'No file uploaded'}), 400
        
        file = request.files['cover_file']
        if file.filename == '':
            return jsonify({'success': False, 'error': 'No file selected'}), 400
        
        # Validate file type
        allowed_extensions = {'.jpg', '.jpeg', '.png', '.gif'}
        if not file.filename:
            return jsonify({'success': False, 'error': 'Invalid filename'}), 400
        
        file_ext = Path(file.filename).suffix.lower()
        if file_ext not in allowed_extensions:
            return jsonify({'success': False, 'error': 'Invalid file type. Please use JPG, PNG, or GIF.'}), 400
        
        # Validate file size (10MB limit)
        file.seek(0, 2)  # Seek to end to get file size
        file_size = file.tell()
        file.seek(0)  # Reset to beginning
        
        if file_size > 10 * 1024 * 1024:  # 10MB
            return jsonify({'success': False, 'error': 'File size must be less than 10MB.'}), 400
        
        # Store old cover URL for cleanup
        old_cover_url = user_book.cover_url
        
        # Process the uploaded image via unified helper
        new_cover_url = process_image_from_filestorage(file)
        abs_cover_url = new_cover_url
        if new_cover_url.startswith('/'):
            abs_cover_url = request.host_url.rstrip('/') + new_cover_url
        # Update the book with the new cover URL
        book_service.update_book_sync(user_book.uid, str(current_user.id), cover_url=abs_cover_url)
        
        # Clean up old cover file if it exists and is a local file
        if old_cover_url and (old_cover_url.startswith('/covers/') or old_cover_url.startswith('/static/covers/')):
            try:
                old_filename = old_cover_url.split('/')[-1]
                old_filepath = get_covers_dir() / old_filename
                if old_filepath.exists():
                    old_filepath.unlink()
                current_app.logger.debug(f"Cleaned up old cover file: {old_filename}")
            except Exception as cleanup_error:
                current_app.logger.debug(f"Failed to clean up old cover file: {cleanup_error}")
        
        current_app.logger.debug(f"Uploaded new cover for book {uid}")
        
        return jsonify({
            'success': True, 
            'cover_url': new_cover_url,
            'message': 'Cover uploaded successfully'
        })
        
    except Exception as e:
        current_app.logger.error(f"Error uploading cover: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@book_bp.route('/book/<uid>/update_reading_dates', methods=['POST'])
@login_required
def update_reading_dates(uid):
    """Update reading start and finish dates."""
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
    
    update_data = {}
    
    start_date_str = request.form.get('start_date', '').strip()
    finish_date_str = request.form.get('finish_date', '').strip()
    
    if start_date_str:
        update_data['start_date'] = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    
    if finish_date_str:
        update_data['finish_date'] = datetime.strptime(finish_date_str, '%Y-%m-%d').date()
    
    try:
        success = book_service.update_book_sync(uid, str(current_user.id), **update_data)
        if success:
            flash('Reading dates updated successfully.', 'success')
        else:
            flash('Failed to update reading dates.', 'error')
    except Exception as e:
        flash(f'Error updating dates: {str(e)}', 'error')
    
    return redirect(url_for('book.view_book_enhanced', uid=uid))


@book_bp.route('/book/<uid>/update_notes', methods=['POST'])
@login_required
def update_book_notes(uid):
    """Update personal notes and rating."""
    user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
    if not user_book:
        abort(404)
    
    update_data = {}
    
    personal_notes = request.form.get('personal_notes', '').strip()
    user_rating = request.form.get('user_rating', '').strip()
    
    update_data['personal_notes'] = personal_notes if personal_notes else None
    
    if user_rating:
        try:
            update_data['user_rating'] = float(user_rating)
        except ValueError:
            flash('Invalid rating value.', 'error')
            return redirect(url_for('book.view_book_enhanced', uid=uid))
    
    try:
        success = book_service.update_book_sync(uid, str(current_user.id), **update_data)
        if success:
            flash('Notes and rating updated successfully.', 'success')
        else:
            flash('Failed to update notes and rating.', 'error')
    except Exception as e:
        flash(f'Error updating notes: {str(e)}', 'error')
    
    return redirect(url_for('book.view_book_enhanced', uid=uid))


@book_bp.route('/book/<uid>/custom_metadata', methods=['GET', 'POST'])
@login_required
def edit_book_custom_metadata(uid):
    """Edit custom metadata for a book."""
    from app.debug_system import debug_log, debug_metadata_operation, debug_service_call, debug_template_data
    
    try:
        
        # Get user book with relationship data (includes custom metadata)
        user_book = book_service.get_book_by_uid_sync(uid, str(current_user.id))
        
        if not user_book:
            flash('Book not found in your library.', 'error')
            return redirect(url_for('main.library'))
        
        # Enhanced metadata debugging
        book_id = getattr(user_book, 'id', 'NO_ID')
        existing_metadata = getattr(user_book, 'custom_metadata', {})
        debug_metadata_operation(book_id, uid, str(current_user.id), existing_metadata, "LOAD")
        
        title = user_book.get('title', 'Unknown Title') if isinstance(user_book, dict) else getattr(user_book, 'title', 'Unknown Title')
        
        if request.method == 'POST':
            
            # Process form data for custom metadata
            # Note: In current architecture, we're storing everything as personal metadata
            personal_metadata = {}
            
            # Get available fields (treating all as personal for now)
            personal_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=False)
            
            global_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=True)
            
            # Ensure we have lists, not None
            personal_fields = personal_fields or []
            global_fields = global_fields or []
            all_fields = personal_fields + global_fields
            
            
            # Process all fields as personal metadata
            for field in all_fields:
                # Check both global_ and personal_ prefixes for backward compatibility
                field_name = field.get('name', '')
                personal_key = f'personal_{field_name}'
                global_key = f'global_{field_name}'
                
                personal_value = request.form.get(personal_key, '').strip()
                global_value = request.form.get(global_key, '').strip()
                
                
                value = personal_value or global_value
                if value:
                    personal_metadata[field_name] = value
            
            print(f"📝 [EDIT_META] Final processed metadata: {personal_metadata}")
            
            # Validate metadata - TODO: Implement validation
            valid, errors = True, []  # custom_field_service.validate_and_save_metadata(personal_metadata, current_user.id, is_global=False)
            
            if valid:
                print(f"📝 [EDIT_META] Updating personal metadata: {personal_metadata}")
                
                # Update user book relationship with personal metadata
                try:
                    # Store custom metadata using the custom field service
                    success = custom_field_service.save_custom_metadata_sync(
                        uid, str(current_user.id), personal_metadata
                    )
                    
                    if success:
                        flash('Custom metadata updated successfully!', 'success')
                        return redirect(url_for('book.view_book_enhanced', uid=uid))
                    else:
                        flash('Failed to update custom metadata.', 'error')
                except Exception as e:
                    flash('Failed to update custom metadata.', 'error')
            else:
                # Show validation errors
                for error in errors:
                    flash(f'Validation error: {error}', 'error')
        
        # Get display data for template
        # For now, treat all fields as personal since that's how they're stored
        personal_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=False) or []
        global_fields = custom_field_service.get_available_fields_sync(current_user.id, is_global=True) or []
        
        print(f"   📋 [EDIT_META] Personal fields count: {len(personal_fields)}")
        print(f"   📋 [EDIT_META] Global fields count: {len(global_fields)}")
        
        # Get existing custom metadata using the custom field service
        existing_metadata = custom_field_service.get_custom_metadata_sync(uid, str(current_user.id))
        print(f"   📊 [EDIT_META] Existing metadata: {existing_metadata}")
        
        # Prepare template data
        global_metadata = {}  # Empty since we're storing everything as personal
        personal_metadata = existing_metadata
        
        print(f"   📤 [EDIT_META] Passing to template:")
        print(f"      🌐 global_fields: {[f.get('name', '') for f in global_fields]}")
        print(f"      👤 personal_fields: {[f.get('name', '') for f in personal_fields]}")
        print(f"      🌐 global_metadata: {global_metadata}")
        print(f"      👤 personal_metadata: {personal_metadata}")
        
        return render_template(
            'edit_book_custom_metadata.html',
            book=user_book,
            user_book=user_book,
            global_fields=global_fields,
            personal_fields=personal_fields,
            global_metadata=global_metadata,  # Empty since we're storing everything as personal
            personal_metadata=personal_metadata,
            global_metadata_display=[],
            personal_metadata_display=custom_field_service.get_custom_metadata_for_display(existing_metadata)
        )
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        flash(f'Error loading custom metadata: {str(e)}', 'error')
        return redirect(url_for('book.view_book_enhanced', uid=uid))


@book_bp.route('/month_review/<int:year>/<int:month>.jpg')
@book_bp.route('/month_review/<int:year>/<int:month>.png')
@login_required  
def month_review(year, month):
    # Query books finished in the given month/year by current user using global book visibility
    user_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
    
    # Filter books finished in the specified month/year
    start_date = datetime(year, month, 1).date()
    if month == 12:
        end_date = datetime(year + 1, 1, 1).date()
    else:
        end_date = datetime(year, month + 1, 1).date()
    
    books = [
        book for book in user_books 
        if (finish_date := getattr(book, 'finish_date', None)) and start_date <= finish_date < end_date
    ]
    
    if not books:
        # This should only be accessed if there are books (from month_wrapup)
        return "No books found", 404

    # Convert to format expected by generate_month_review_image
    # The function likely expects objects with title, author, cover_url attributes
    book_objects = []
    for book in books:
        # Create a simple object with the expected attributes
        book_obj = type('Book', (), {
            'title': book.get('title', '') if isinstance(book, dict) else getattr(book, 'title', ''),
            'author': book.get('author', '') if isinstance(book, dict) else getattr(book, 'author', ''),
            'cover_url': book.get('cover_url', '') if isinstance(book, dict) else getattr(book, 'cover_url', ''),
            'finish_date': book.get('finish_date') if isinstance(book, dict) else getattr(book, 'finish_date', None)
        })()
        book_objects.append(book_obj)

    img_buffer = generate_month_review_image(book_objects, month, year)
    
    # Check if image generation was successful
    if not img_buffer:
        return "Error generating month review image", 500
    
    # img_buffer is already a BytesIO object from generate_month_review_image
    return send_file(img_buffer, mimetype='image/png', as_attachment=True, download_name=f"month_review_{year}_{month}.png")

@book_bp.route('/add_book_from_search', methods=['POST'])
@login_required
def add_book_from_search():
    """Add a book from search results - redirect to manual add for consistency."""
    # All book addition now goes through the unified manual add route
    return add_book_manual()


@book_bp.route('/download_db', methods=['GET'])
@login_required
def download_db():
    """Export user data from Kuzu to CSV format."""
    try:
        # Get all user books with global visibility
        user_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
        
        # Create CSV export
        import io
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow(['Title', 'Author', 'ISBN', 'Reading Status', 'Start Date', 'Finish Date', 'Rating', 'Notes'])
        
        # Write book data
        for book in user_books:
            # Handle both dict and object book types
            if isinstance(book, dict):
                author_names = ', '.join([author.get('name', '') for author in book.get('authors', [])]) if book.get('authors') else ''
                isbn = book.get('isbn13', '') or book.get('isbn10', '') or ''
                start_date = book.get('start_date', '').isoformat() if book.get('start_date') and hasattr(book.get('start_date'), 'isoformat') else ''
                finish_date = book.get('finish_date', '').isoformat() if book.get('finish_date') and hasattr(book.get('finish_date'), 'isoformat') else ''
                title = book.get('title', '')
            else:
                author_names = ', '.join([author.name for author in getattr(book, 'authors', [])]) if hasattr(book, 'authors') and getattr(book, 'authors', []) else ''
                isbn = getattr(book, 'isbn13', '') or getattr(book, 'isbn10', '') or ''
                start_date_val = getattr(book, 'start_date', None)
                start_date = start_date_val.isoformat() if start_date_val and hasattr(start_date_val, 'isoformat') else ''
                finish_date_val = getattr(book, 'finish_date', None)
                finish_date = finish_date_val.isoformat() if finish_date_val and hasattr(finish_date_val, 'isoformat') else ''
                title = getattr(book, 'title', '')
            
            rating = getattr(book, 'user_rating', '') or ''
            notes = getattr(book, 'personal_notes', '') or ''
            reading_status = getattr(book, 'reading_status', '') or ''
            writer.writerow([title, author_names, isbn, reading_status, start_date, finish_date, rating, notes])
        
        # Create response
        output.seek(0)
        response = make_response(output.getvalue())
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = f'attachment; filename=bibliotheca_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        
        return response
    except Exception as e:
        current_app.logger.error(f"Error exporting data: {e}")
        flash('Error exporting data.', 'danger')
        return redirect(url_for('main.reading_history'))

@book_bp.route('/community_activity/active_readers')
@login_required
def community_active_readers():
    """Redirect to stats page where community activity is now integrated"""
    flash('Community activity has been moved to the Stats page.', 'info')
    return redirect(url_for('main.stats'))

@book_bp.route('/community_activity/books_this_month')
@login_required
def community_books_this_month():
    """Redirect to stats page where community activity is now integrated"""
    flash('Community activity has been moved to the Stats page.', 'info')
    return redirect(url_for('main.stats'))

@book_bp.route('/community_activity/currently_reading')
@login_required
def community_currently_reading():
    """Redirect to stats page where community activity is now integrated"""
    flash('Community activity has been moved to the Stats page.', 'info')
    return redirect(url_for('main.stats'))

@book_bp.route('/community_activity/recent_activity')
@login_required
def community_recent_activity():
    """Redirect to stats page where community activity is now integrated"""
    flash('Community activity has been moved to the Stats page.', 'info')
    return redirect(url_for('main.stats'))

@book_bp.route('/user/<string:user_id>/profile')
@login_required
def user_profile(user_id):
    """Show public profile for a user if they're sharing"""
    try:
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            abort(404)
        
        # Check if user allows profile viewing
        if not user.share_reading_activity:
            flash('This user has not enabled profile sharing.', 'warning')
            return redirect(url_for('main.stats'))
        
        # Get user's books with global visibility
        all_user_books = book_service.get_all_books_with_user_overlay_sync(str(user_id))
        
        # Calculate statistics from the book list
        current_year = datetime.now().year
        current_month = datetime.now().date().replace(day=1)
        
        # Helper function to safely get date from finish_date (handles both date and datetime objects)
        def get_finish_date_safe(book):
            finish_date = getattr(book, 'finish_date', None)
            if finish_date is None:
                return None
            # Convert datetime to date if needed
            if isinstance(finish_date, datetime):
                return finish_date.date()
            return finish_date
        
        total_books = len([book for book in all_user_books if get_finish_date_safe(book)])
        
        books_this_year = len([book for book in all_user_books 
                              if (finish_date := get_finish_date_safe(book)) and 
                              finish_date >= date(current_year, 1, 1)])
        
        books_this_month = len([book for book in all_user_books 
                               if (finish_date := get_finish_date_safe(book)) and 
                               finish_date >= current_month])
        
        currently_reading = [book for book in all_user_books 
                           if getattr(book, 'start_date', None) and not getattr(book, 'finish_date', None)] if user.share_current_reading else []
        
        recent_finished = sorted([book for book in all_user_books if get_finish_date_safe(book)],
                                key=lambda x: get_finish_date_safe(x) or date.min, reverse=True)[:10]
        
        # Get reading logs count from service
        reading_logs_count = reading_log_service.get_user_logs_count_sync(user_id)
        
        return render_template('user_profile.html',
                             profile_user=user,
                             total_books=total_books,
                             books_this_year=books_this_year,
                             books_this_month=books_this_month,
                             currently_reading=currently_reading,
                             recent_finished=recent_finished,
                             reading_logs_count=reading_logs_count)
    
    except Exception as e:
        current_app.logger.error(f"Error loading user profile {user_id}: {e}")
        flash('Error loading user profile.', 'danger')
        return redirect(url_for('main.stats'))

@book_bp.route('/book/<uid>/assign', methods=['POST'])
@login_required
def assign_book(uid):
    try:
        book = book_service.get_book_by_uid_sync(uid, current_user.id)
        if not book:
            abort(404)
            
        if not current_user.is_admin:
            flash('Only admins can assign books.', 'danger')
            return redirect(url_for('main.library'))

        user_id = request.form.get('user_id')
        if not user_id:
            flash('No user selected.', 'danger')
            return redirect(url_for('main.library'))
            
        user = user_service.get_user_by_id_sync(user_id)
        if not user:
            flash('Invalid user selected.', 'danger')
            return redirect(url_for('main.library'))

        # For Kuzu, this would involve transferring the book to another user
        # This is a complex operation that would need to be implemented in the service layer
        flash('Book assignment feature needs to be implemented for Kuzu backend.', 'warning')
        return redirect(url_for('main.library'))
    except Exception as e:
        current_app.logger.error(f"Error assigning book: {e}")
        flash('Error assigning book.', 'danger')
        return redirect(url_for('main.library'))

@book_bp.route('/books/bulk_delete', methods=['POST'])
@login_required
def bulk_delete_books():
    """Delete multiple books selected from the library view."""
    print(f"Bulk delete route called by user {current_user.id}")
    print(f"Request method: {request.method}")
    print(f"Request form keys: {list(request.form.keys())}")
    print(f"Full form data: {dict(request.form)}")
    
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
    if failed_count > 0:
        flash(f'Failed to delete {failed_count} book(s).', 'error')
    
    return redirect(url_for('main.library'))

@book_bp.route('/csrf-guide')
def csrf_guide():
    """Demo page showing CSRF protection implementation."""
    return render_template('csrf_guide.html')

@book_bp.route('/stats')
@login_required
def stats():
    """Redirect to new stats page."""
    return redirect(url_for('stats.index'))

@book_bp.route('/search_books_in_library', methods=['POST'])
@login_required
def search_books_in_library():
    """Search for books to add from the library page"""
    query = request.form.get('query', '')
    results = []
    
    if query:
        # Google Books API search
        resp = requests.get(
            'https://www.googleapis.com/books/v1/volumes',
            params={'q': query, 'maxResults': 10}
        )
        data = resp.json()
        for item in data.get('items', []):
            volume_info = item.get('volumeInfo', {})
            image_links = volume_info.get('imageLinks', {})
            image = None
            try:
                from app.utils.book_utils import select_highest_google_image, upgrade_google_cover_url
                raw = select_highest_google_image(image_links)
                if raw:
                    image = upgrade_google_cover_url(raw)
            except Exception:
                image = image_links.get('thumbnail')
            isbn = None
            for iden in volume_info.get('industryIdentifiers', []):
                if iden['type'] in ('ISBN_13', 'ISBN_10'):
                    isbn = iden['identifier']
                    break
            results.append({
                'title': volume_info.get('title'),
                'authors': ', '.join(volume_info.get('authors', [])),
                'image': image,
                'isbn': isbn
            })
    
    # Get all the data needed for library page
    # Get filter parameters from URL
    category_filter = request.args.get('category', '')
    publisher_filter = request.args.get('publisher', '')
    language_filter = request.args.get('language', '')
    location_filter = request.args.get('location', '')
    search_query = request.args.get('search', '')

    # Use service layer with global book visibility
    user_books = book_service.get_all_books_with_user_overlay_sync(str(current_user.id))
    
    # Apply filters in Python (Kuzu doesn't have complex querying like SQL)
    filtered_books = user_books
    
    if search_query:
        search_lower = search_query.lower()
        filtered_books = [
            book for book in filtered_books 
            if (search_lower in (book.get('title', '') if isinstance(book, dict) else getattr(book, 'title', '')).lower()) or
               (search_lower in (book.get('author', '') if isinstance(book, dict) else getattr(book, 'author', '')).lower()) or
               (search_lower in (book.get('description', '') if isinstance(book, dict) else getattr(book, 'description', '')).lower())
        ]
    if publisher_filter:
        filtered_books = [
            book for book in filtered_books 
            if (book.get('publisher', '') if isinstance(book, dict) else getattr(book, 'publisher', '')) and publisher_filter.lower() in (book.get('publisher', '') if isinstance(book, dict) else getattr(book, 'publisher', '')).lower()
        ]
    if language_filter:
        filtered_books = [
            book for book in filtered_books 
            if (book.get('language', '') if isinstance(book, dict) else getattr(book, 'language', '')) == language_filter
        ]
    if location_filter:
        filtered_books = [
            book for book in filtered_books 
            if (book.get('locations', []) if isinstance(book, dict) else getattr(book, 'locations', [])) and any(
                location_filter.lower() in (loc.get('name', '') if isinstance(loc, dict) else getattr(loc, 'name', '')).lower() 
                for loc in (book.get('locations', []) if isinstance(book, dict) else getattr(book, 'locations', []))
                if (loc.get('name', '') if isinstance(loc, dict) else getattr(loc, 'name', ''))
            )
        ]
    if category_filter:
        filtered_books = [
            book for book in filtered_books 
            if (book.get('categories', []) if isinstance(book, dict) else getattr(book, 'categories', [])) and any(
                category_filter.lower() in (cat.get('name', '') if isinstance(cat, dict) else getattr(cat, 'name', '')).lower() 
                for cat in (book.get('categories', []) if isinstance(book, dict) else getattr(book, 'categories', []))
                if (cat.get('name', '') if isinstance(cat, dict) else getattr(cat, 'name', ''))
            )
        ]

    # Books are already in the right format for the template
    books = filtered_books

    # Get distinct values for filter dropdowns
    all_books = user_books  # Use same data
    
    categories = set()
    publishers = set()
    languages = set()
    locations = set()

    for book in all_books:
        book_categories = book.get('categories', []) if isinstance(book, dict) else getattr(book, 'categories', [])
        if book_categories:
            # book.categories is a list of Category objects, not a string
            categories.update([
                cat.get('name', '') if isinstance(cat, dict) else getattr(cat, 'name', '')
                for cat in book_categories 
                if (cat.get('name', '') if isinstance(cat, dict) else getattr(cat, 'name', ''))
            ])
        
        book_publisher = book.get('publisher', None) if isinstance(book, dict) else getattr(book, 'publisher', None)
        if book_publisher:
            # Handle Publisher domain object or string
            if isinstance(book_publisher, dict):
                publisher_name = book_publisher.get('name', str(book_publisher))
            else:
                publisher_name = getattr(book_publisher, 'name', str(book_publisher))
            publishers.add(publisher_name)
        
        book_language = book.get('language', '') if isinstance(book, dict) else getattr(book, 'language', '')
        if book_language:
            languages.add(book_language)
        
        book_locations = book.get('locations', []) if isinstance(book, dict) else getattr(book, 'locations', [])
        if book_locations:
            for loc in book_locations:
                if isinstance(loc, str) and loc:
                    # Location is already a string (location name) and not empty
                    locations.add(loc)
                elif isinstance(loc, dict) and loc.get('name'):
                    locations.add(loc.get('name'))
                elif hasattr(loc, 'name') and getattr(loc, 'name', None):
                    locations.add(getattr(loc, 'name'))

    # Get users through Kuzu service layer
    domain_users = user_service.get_all_users_sync() or []
    
    # Convert domain users to simple objects for template compatibility
    users = []
    for domain_user in domain_users:
        user_data = {
            'id': domain_user.id,
            'username': domain_user.username,
            'email': domain_user.email
        }
        users.append(type('User', (), user_data))

    # Calculate statistics for filter buttons
    stats = {
        'total_books': len(user_books),
        'books_read': len([b for b in user_books if getattr(b, 'reading_status', None) == 'read']),
        'currently_reading': len([b for b in user_books if getattr(b, 'reading_status', None) == 'reading']),
        'want_to_read': len([b for b in user_books if getattr(b, 'reading_status', None) == 'plan_to_read']),
        'on_hold': len([b for b in user_books if getattr(b, 'reading_status', None) == 'on_hold']),
        'wishlist': len([b for b in user_books if getattr(b, 'ownership_status', None) == 'wishlist']),
    }

    return render_template(
        'library_enhanced.html',
        books=books,
        stats=stats,
        categories=sorted([cat for cat in categories if cat is not None and cat != '']),
        publishers=sorted([pub for pub in publishers if pub is not None and pub != '']),
        languages=sorted([lang for lang in languages if lang is not None and lang != '']),
        locations=sorted([loc for loc in locations if loc is not None and loc != '']),
        current_status_filter='all',
        current_category=category_filter,
        current_publisher=publisher_filter,
        current_language=language_filter,
        current_location=location_filter,
        current_search=search_query,
        current_sort='title_asc',
        users=users,
        search_results=results,
        search_query=query
    )

@book_bp.route('/add_book_manual', methods=['POST'])
@login_required
def add_book_manual():
    """Add a book manually from the library page"""
    # 🔥 SIMPLIFIED ARCHITECTURE INTERCEPT
    # Use simplified service to avoid complex transaction issues
    try:
        import json

        # Get form data
        title = request.form['title'].strip()
        if not title:
            flash('Error: Title is required to add a book.', 'danger')
            return redirect(url_for('main.library'))

        # Handle new form structure with contributor JSON data
        contributors_data = {}
        try:
            for contrib_type in ['authored', 'narrated', 'edited', 'translated', 'illustrated']:
                json_field = f'contributors_{contrib_type}'
                contributors_data[contrib_type] = json.loads(request.form[json_field]) if json_field in request.form else []
        except json.JSONDecodeError as e:
            current_app.logger.error(f"Error parsing contributor JSON: {e}")
            contributors_data = {}

        # Handle categories from JSON (visual chips) and raw hierarchical paths
        categories = []
        raw_categories = None
        try:
            if 'categories' in request.form:
                category_data = json.loads(request.form['categories'])
                categories = [cat['name'] for cat in category_data if 'name' in cat]
            if 'raw_categories' in request.form and request.form['raw_categories']:
                try:
                    raw_categories = json.loads(request.form['raw_categories'])
                except json.JSONDecodeError:
                    raw_categories = [s.strip() for s in request.form['raw_categories'].split(',') if s.strip()]
        except json.JSONDecodeError as e:
            current_app.logger.error(f"Error parsing categories JSON: {e}")
            manual_cats = request.form.get('manual_categories')
            if manual_cats:
                categories = [cat.strip() for cat in manual_cats.split(',') if cat.strip()]

        # Extract primary author from contributors or fallback to form field
        author = contributors_data['authored'][0]['name'] if contributors_data.get('authored') else request.form.get('author', '').strip()

        # Handle additional authors
        additional_authors_list = [contrib['name'] for contrib in (contributors_data.get('authored') or [])[1:]] if contributors_data.get('authored') and len(contributors_data['authored']) > 1 else []
        additional_authors_form = ', '.join(additional_authors_list) if additional_authors_list else ''

        # Extract other contributor types
        narrator_list = [contrib['name'] for contrib in contributors_data.get('narrated', [])]
        editor_list = [contrib['name'] for contrib in contributors_data.get('edited', [])]
        translator_list = [contrib['name'] for contrib in contributors_data.get('translated', [])]
        illustrator_list = [contrib['name'] for contrib in contributors_data.get('illustrated', [])]

        # Basic form fields
        isbn = request.form.get('isbn', '').strip()
        subtitle = request.form.get('subtitle', '').strip()
        publisher_name = request.form.get('publisher', '').strip()
        description = request.form.get('description', '').strip()
        page_count_str = request.form.get('page_count', '').strip()
        language = request.form.get('language', '').strip() or 'en'
        cover_url = request.form.get('cover_url', '').strip()
        published_date_str = request.form.get('published_date', '').strip()
        series = request.form.get('series', '').strip()
        series_volume = request.form.get('series_volume', '').strip()

        # Parse page count
        page_count = None
        if page_count_str:
            try:
                page_count = int(page_count_str)
            except ValueError:
                page_count = None

        # Get location and ownership details
        location_id = request.form.get('location_id')
        reading_status = request.form.get('reading_status', '')
        ownership_status = request.form.get('ownership_status', 'owned')
        media_type = request.form.get('media_type', 'physical')

        # Personal info
        personal_notes = request.form.get('personal_notes', '').strip()
        review = request.form.get('review', '').strip()
        user_rating = request.form.get('user_rating', '')
        start_date = request.form.get('start_date', '').strip()
        finish_date = request.form.get('finish_date', '').strip()

        # Additional identifiers
        isbn13_form = request.form.get('isbn13', '').strip()
        isbn10_form = request.form.get('isbn10', '').strip()
        google_books_id_form = request.form.get('google_books_id', '').strip()

        # Enhanced ISBN processing with comprehensive field mapping
        isbn13 = None
        isbn10 = None
        api_data = None
        cached_cover_url = None

        if isbn:
            import re
            import uuid
            from pathlib import Path

            clean_isbn = re.sub(r'[^0-9X]', '', str(isbn).upper())

            if len(clean_isbn) == 10:
                isbn10 = clean_isbn
                isbn13_base = "978" + clean_isbn[:9]
                check_sum = 0
                for i, digit in enumerate(isbn13_base):
                    check_sum += int(digit) * (1 if i % 2 == 0 else 3)
                check_digit = (10 - (check_sum % 10)) % 10
                isbn13 = isbn13_base + str(check_digit)
            elif len(clean_isbn) == 13:
                isbn13 = clean_isbn
                if clean_isbn.startswith('978'):
                    isbn10_base = clean_isbn[3:12]
                    check_sum = 0
                    for i, digit in enumerate(isbn10_base):
                        check_sum += int(digit) * (10 - i)
                    check_digit = (11 - (check_sum % 11)) % 11
                    if check_digit == 10:
                        check_digit = 'X'
                    elif check_digit == 11:
                        check_digit = '0'
                    isbn10 = isbn10_base + str(check_digit)

            normalized_isbn = isbn13 or isbn10
            if normalized_isbn:
                try:
                    from app.utils.metadata_aggregator import fetch_unified_by_isbn as unified_fetch
                    api_data = unified_fetch(normalized_isbn) or {}

                    print(f"🎯 [MANUAL] Unified API returned: {bool(api_data)} for ISBN {normalized_isbn}")
                    if api_data:
                        if not cover_url and api_data.get('cover_url'):
                            cover_url = api_data['cover_url']
                        if not description and api_data.get('description'):
                            description = api_data['description']
                        if not publisher_name and api_data.get('publisher'):
                            publisher_name = api_data['publisher']
                        if not page_count and api_data.get('page_count'):
                            page_count = api_data['page_count']
                        if not published_date_str and api_data.get('published_date'):
                            published_date_str = api_data['published_date']
                            print(f"🎯 [MANUAL] Using unified published_date: '{published_date_str}' (type: {type(published_date_str)})")
                        if not categories and api_data.get('categories'):
                            categories = api_data['categories']

                        if cover_url:
                            try:
                                covers_dir = Path('/app/data/covers')
                                if not covers_dir.exists():
                                    data_dir = getattr(current_app.config, 'DATA_DIR', None)
                                    if data_dir:
                                        covers_dir = Path(data_dir) / 'covers'
                                    else:
                                        base_dir = Path(current_app.root_path).parent
                                        covers_dir = base_dir / 'data' / 'covers'
                                covers_dir.mkdir(parents=True, exist_ok=True)
                                cached_cover_url = process_image_from_url(cover_url)
                                cover_url = cached_cover_url
                            except Exception as e:
                                print(f"Cover caching failed: {e}")
                except Exception as e:
                    print(f"❌ [MANUAL] Unified metadata fetch failed: {e}")

        # Process API contributors and merge with form contributors
        additional_authors = None
        editor = None
        translator = None
        narrator = None
        illustrator = None
        google_books_id = google_books_id_form
        openlibrary_id = None

        if not google_books_id and api_data and api_data.get('google_books_id'):
            google_books_id = api_data['google_books_id']
            print(f"🎯 [MANUAL] Set Google Books ID from API: {google_books_id}")

        if api_data and api_data.get('openlibrary_id'):
            openlibrary_id = api_data['openlibrary_id']
            print(f"🎯 [MANUAL] Set OpenLibrary ID: {openlibrary_id}")

        if api_data and api_data.get('contributors'):
            api_contributors = api_data['contributors']
            print(f"🎯 [MANUAL] Processing {len(api_contributors)} contributors from API")
            api_authors = [c['name'] for c in api_contributors if c.get('role') == 'author']
            api_editors = [c['name'] for c in api_contributors if c.get('role') == 'editor']
            api_translators = [c['name'] for c in api_contributors if c.get('role') == 'translator']
            api_narrators = [c['name'] for c in api_contributors if c.get('role') == 'narrator']
            api_illustrators = [c['name'] for c in api_contributors if c.get('role') == 'illustrator']

            if not author and api_authors:
                author = api_authors[0]
                print(f"🎯 [MANUAL] Set primary author from API: {author}")

            if not additional_authors_form and api_authors and len(api_authors) > 1:
                primary_author = author or "Unknown Author"
                additional_author_names = [a for a in api_authors if a.lower() != primary_author.lower()]
                if additional_author_names:
                    additional_authors_form = ', '.join(additional_author_names)
                    print(f"🎯 [MANUAL] Set additional_authors from API: {additional_authors_form}")

            if not editor_list and api_editors:
                editor_list = api_editors
                print(f"🎯 [MANUAL] Set editors from API: {editor_list}")
            if not translator_list and api_translators:
                translator_list = api_translators
                print(f"🎯 [MANUAL] Set translators from API: {translator_list}")
            if not narrator_list and api_narrators:
                narrator_list = api_narrators
                print(f"🎯 [MANUAL] Set narrators from API: {narrator_list}")
            if not illustrator_list and api_illustrators:
                illustrator_list = api_illustrators
                print(f"🎯 [MANUAL] Set illustrators from API: {illustrator_list}")

        # Convert contributor lists to strings
        additional_authors = additional_authors_form
        editor = ', '.join(editor_list) if editor_list else None
        translator = ', '.join(translator_list) if translator_list else None
        narrator = ', '.join(narrator_list) if narrator_list else None
        illustrator = ', '.join(illustrator_list) if illustrator_list else None

        # Create simplified book data with enhanced API fields including contributors
        book_data = SimplifiedBook(
            title=title,
            author=author or "Unknown Author",
            isbn13=isbn13,
            isbn10=isbn10,
            subtitle=subtitle,
            description=description,
            publisher=publisher_name,
            published_date=published_date_str,
            page_count=page_count,
            language=language,
            cover_url=cover_url,
            series=series,
            series_volume=series_volume,
            categories=categories,
            additional_authors=additional_authors,
            editor=editor,
            translator=translator,
            narrator=narrator,
            illustrator=illustrator,
            google_books_id=google_books_id,
            openlibrary_id=openlibrary_id
        )

        print(f"🎯 [MANUAL] Final SimplifiedBook data:")
        print(f"   Title: {book_data.title}")
        print(f"   Author: {book_data.author}")
        print(f"   Additional Authors: {book_data.additional_authors}")
        print(f"   Editor: {book_data.editor}")
        print(f"   Translator: {book_data.translator}")
        print(f"   Narrator: {book_data.narrator}")
        print(f"   Illustrator: {book_data.illustrator}")
        print(f"   ISBN13: {book_data.isbn13}")
        print(f"   ISBN10: {book_data.isbn10}")
        print(f"   Cover URL: {book_data.cover_url}")
        print(f"   Categories: {book_data.categories}")
        print(f"   Description: {book_data.description[:100] if book_data.description else 'None'}...")
        print(f"   Publisher: {book_data.publisher}")
        print(f"   Published Date: '{book_data.published_date}' (type: {type(book_data.published_date)})")
        print(f"   Page count: {book_data.page_count}")
        print(f"   Google Books ID: {book_data.google_books_id}")
        print(f"   OpenLibrary ID: {book_data.openlibrary_id}")

        service = SimplifiedBookService()
        try:
            success = service.add_book_to_user_library_sync(
                book_data=book_data,
                user_id=current_user.id,
                reading_status=reading_status,
                ownership_status=ownership_status,
                media_type=media_type,
                location_id=location_id
            )

            if success:
                from app.services import book_service
                try:
                    books = book_service.get_user_books_sync(current_user.id)
                    created_book = None
                    for book in books:
                        book_author_name = None
                        if hasattr(book, 'primary_author') and getattr(book, 'primary_author'):
                            book_author_name = getattr(book, 'primary_author')
                        elif hasattr(book, 'contributors') and book.contributors:
                            try:
                                first_contrib = book.contributors[0]
                                if hasattr(first_contrib, 'person') and first_contrib.person and hasattr(first_contrib.person, 'name'):
                                    book_author_name = first_contrib.person.name
                                elif hasattr(first_contrib, 'name'):
                                    book_author_name = first_contrib.name
                            except Exception:
                                book_author_name = None
                        if book.title == title and (not author or not book_author_name or book_author_name == author):
                            created_book = book
                            break

                    if created_book:
                        update_data = {}
                        if personal_notes:
                            update_data['personal_notes'] = personal_notes
                        if review:
                            update_data['review'] = review
                        if user_rating:
                            try:
                                update_data['user_rating'] = int(user_rating)
                            except ValueError:
                                pass
                        if start_date:
                            update_data['start_date'] = start_date
                        if finish_date:
                            update_data['finish_date'] = finish_date
                        if isbn13_form and isbn13_form != (isbn13 or ''):
                            update_data['isbn13'] = isbn13_form
                        if isbn10_form and isbn10_form != (isbn10 or ''):
                            update_data['isbn10'] = isbn10_form
                        if raw_categories:
                            update_data['raw_categories'] = raw_categories
                        if update_data:
                            book_service.update_book_sync(created_book.uid, str(current_user.id), **update_data)
                            print(f"🎯 [MANUAL] Updated book with personal information: {list(update_data.keys())}")
                except Exception as update_e:
                    print(f"Warning: Could not update personal information: {update_e}")

                flash(f'Successfully added "{title}" to your library!', 'success')
                return redirect(url_for('main.library'))
            else:
                flash('Failed to add book. Please try again.', 'danger')
        except BookAlreadyExistsError as e:
            flash(f'This book already exists in the library! Redirecting to edit the existing copy.', 'info')
            return redirect(url_for('book.edit_book', uid=e.book_id))

        return redirect(url_for('main.library'))
    except Exception as e:
        flash('Error adding book. Please try again.', 'danger')
        return redirect(url_for('main.library'))
    
    # ORIGINAL FUNCTION BELOW (kept as fallback, but should never be reached)
    # Validate required fields
    title = request.form['title'].strip()
    if not title:
        flash('Error: Title is required to add a book.', 'danger')
        return redirect(url_for('main.library'))

    # Extract all form fields
    isbn = request.form.get('isbn', '').strip()
    author = request.form.get('author', '').strip()
    subtitle = request.form.get('subtitle', '').strip()
    publisher_name = request.form.get('publisher', '').strip()
    page_count_str = request.form.get('page_count', '').strip()
    language = request.form.get('language', '').strip() or 'en'
    cover_url = request.form.get('cover_url', '').strip()
    published_date_str = request.form.get('published_date', '').strip()
    series = request.form.get('series', '').strip()
    series_volume = request.form.get('series_volume', '').strip()
    series_order_str = request.form.get('series_order', '').strip()
    genres = request.form.get('genres', '').strip()  # Changed from 'genre' to 'genres'
    reading_status = request.form.get('reading_status', '').strip()
    ownership_status = request.form.get('ownership_status', '').strip()
    media_type = request.form.get('media_type', '').strip()
    location_id = request.form.get('location_id', '').strip()
    user_rating_str = request.form.get('user_rating', '').strip()
    personal_notes = request.form.get('personal_notes', '').strip()
    user_tags = request.form.get('user_tags', '').strip()
    description = request.form.get('description', '').strip()
    
    # Convert numeric fields
    page_count = None
    if page_count_str:
        try:
            page_count = int(page_count_str)
        except ValueError:
            pass
    
    series_order = None
    if series_order_str:
        try:
            series_order = int(series_order_str)
        except ValueError:
            pass
    
    user_rating = None
    if user_rating_str:
        try:
            user_rating = float(user_rating_str)
        except ValueError:
            pass
    
    # Convert date fields
    published_date = None
    if published_date_str:
        try:
            published_date = datetime.strptime(published_date_str, '%Y-%m-%d').date()
        except ValueError:
            pass
    
    # Enhanced ISBN processing with proper normalization
    isbn10 = None
    isbn13 = None
    normalized_isbn = None
    
    if isbn:
        # Enhanced ISBN normalization
        import re
        clean_isbn = re.sub(r'[^0-9X]', '', str(isbn).upper())
        
        if len(clean_isbn) == 10:
            # ISBN10 - validate and convert to ISBN13
            isbn10 = clean_isbn
            # Convert ISBN10 to ISBN13
            isbn13_base = "978" + clean_isbn[:9]
            check_sum = 0
            for i, digit in enumerate(isbn13_base):
                check_sum += int(digit) * (1 if i % 2 == 0 else 3)
            check_digit = (10 - (check_sum % 10)) % 10
            isbn13 = isbn13_base + str(check_digit)
            normalized_isbn = isbn13  # Prefer ISBN13 for API lookups
        elif len(clean_isbn) == 13:
            # ISBN13 - try to convert to ISBN10 if it starts with 978
            isbn13 = clean_isbn
            normalized_isbn = isbn13
            if clean_isbn.startswith('978'):
                isbn10_base = clean_isbn[3:12]
                check_sum = 0
                for i, digit in enumerate(isbn10_base):
                    check_sum += int(digit) * (10 - i)
                check_digit = (11 - (check_sum % 11)) % 11
                if check_digit == 10:
                    check_digit = 'X'
                elif check_digit == 11:
                    check_digit = '0'
                isbn10 = isbn10_base + str(check_digit)
        
    
    # Process manual genre input
    manual_categories = []
    if genres:
        # Split by comma and clean up
        manual_categories = [cat.strip() for cat in genres.split(',') if cat.strip()]
    
    start_date_str = request.form.get('start_date') or None
    finish_date_str = request.form.get('finish_date') or None
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
    finish_date = datetime.strptime(finish_date_str, '%Y-%m-%d').date() if finish_date_str else None

    # Process custom metadata - only allow selection of existing fields
    custom_metadata = {}
    
    # Get all submitted custom field selections
    for key, value in request.form.items():
        if key.startswith('custom_field_') and value and value.strip():
            # Extract field name from the form key (custom_field_<field_name>)
            field_name = key.replace('custom_field_', '')
            field_value = value.strip()
            
            print(f"📋 [MANUAL] Selected existing custom field: {field_name} = {field_value}")
            custom_metadata[field_name] = field_value


    # Enhanced API lookup with comprehensive field mapping
    api_data = None
    final_cover_url = cover_url
    cached_cover_url = None
    
    if normalized_isbn:
        
        # Enhanced Google Books API lookup
        def enhanced_google_books_lookup(isbn):
            url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
            try:
                import requests
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                items = data.get("items")
                if not items:
                    return None
                
                volume_info = items[0]["volumeInfo"]
                
                # Extract ISBN and ASIN information from industryIdentifiers
                api_isbn10 = None
                api_isbn13 = None
                asin = None
                
                industry_identifiers = volume_info.get('industryIdentifiers', [])
                for identifier in industry_identifiers:
                    id_type = identifier.get('type', '')
                    id_value = identifier.get('identifier', '')
                    
                    if id_type == 'ISBN_10':
                        api_isbn10 = id_value
                    elif id_type == 'ISBN_13':
                        api_isbn13 = id_value
                    elif id_type == 'ASIN':
                        # Validate ASIN format
                        if id_value and len(id_value.strip()) == 10 and id_value.strip().isalnum():
                            asin = id_value.strip().upper()
                    elif id_type == 'OTHER' and 'ASIN' in id_value:
                        # Fallback for incorrectly categorized ASINs
                        # Extract ASIN pattern from the identifier value
                        asin_match = re.search(r'[A-Z0-9]{10}', str(id_value).upper())
                        if asin_match:
                            candidate_asin = asin_match.group()
                            if len(candidate_asin) == 10 and candidate_asin.isalnum():
                                asin = candidate_asin
                
                # Get best quality cover image
                image_links = volume_info.get("imageLinks", {})
                cover_url = None
                for size in ['extraLarge', 'large', 'medium', 'thumbnail', 'smallThumbnail']:
                    if size in image_links:
                        cover_url = image_links[size]
                        break
                
                # Force HTTPS for cover URLs
                if cover_url and cover_url.startswith('http://'):
                    cover_url = cover_url.replace('http://', 'https://')
                
                return {
                    'title': volume_info.get('title', ''),
                    'subtitle': volume_info.get('subtitle', ''),
                    'description': volume_info.get('description', ''),
                    'authors_list': volume_info.get('authors', []),
                    'publisher': volume_info.get('publisher', ''),
                    'published_date': volume_info.get('publishedDate', ''),
                    'page_count': volume_info.get('pageCount'),
                    'language': volume_info.get('language', 'en'),
                    'average_rating': volume_info.get('averageRating'),
                    'rating_count': volume_info.get('ratingsCount'),
                    'categories': volume_info.get('categories', []),
                    'isbn10': api_isbn10,
                    'isbn13': api_isbn13,
                    'asin': asin,
                    'cover_url': cover_url,
                    'google_books_id': items[0].get('id', ''),
                    'source': 'google_books'
                }
            except Exception as e:
                return None
        
        # Enhanced OpenLibrary API lookup
        def enhanced_openlibrary_lookup(isbn):
            url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
            try:
                import requests
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                book_key = f"ISBN:{isbn}"
                if book_key not in data:
                    return None
                
                book = data[book_key]
                
                # Extract authors
                authors_list = []
                authors_data = book.get('authors', [])
                for author in authors_data:
                    if isinstance(author, dict) and 'name' in author:
                        authors_list.append(author['name'])
                    elif isinstance(author, str):
                        authors_list.append(author)
                
                # Extract publisher
                publishers = book.get('publishers', [])
                publisher = ''
                if publishers:
                    if isinstance(publishers[0], dict):
                        publisher = publishers[0].get('name', '')
                    else:
                        publisher = str(publishers[0])
                
                # Extract categories/subjects
                subjects = book.get('subjects', [])
                categories = []
                for subject in subjects[:10]:  # Limit to 10 categories
                    if isinstance(subject, dict):
                        categories.append(subject.get('name', ''))
                    else:
                        categories.append(str(subject))
                categories = [cat for cat in categories if cat]
                
                # Extract cover image
                cover_data = book.get('cover', {})
                cover_url = None
                for size in ['large', 'medium', 'small']:
                    if size in cover_data:
                        cover_url = cover_data[size]
                        break
                
                # Description handling
                description = book.get('notes', {})
                if isinstance(description, dict):
                    description = description.get('value', '')
                elif not isinstance(description, str):
                    description = ''
                
                return {
                    'title': book.get('title', ''),
                    'subtitle': book.get('subtitle', ''),
                    'description': description,
                    'authors_list': authors_list,
                    'publisher': publisher,
                    'published_date': book.get('publish_date', ''),
                    'page_count': book.get('number_of_pages'),
                    'language': 'en',  # Default, could be enhanced
                    'categories': categories,
                    'cover_url': cover_url,
                    'openlibrary_id': book.get('key', '').replace('/books/', '') if book.get('key') else '',
                    'source': 'openlibrary'
                }
            except Exception as e:
                return None
        
        # Perform API lookups
        google_data = enhanced_google_books_lookup(normalized_isbn)
        ol_data = enhanced_openlibrary_lookup(normalized_isbn)
        
        # Merge API data with priority to Google Books
        if google_data or ol_data:
            if google_data and ol_data:
                # Merge strategy: Google Books for most fields, OpenLibrary for fallbacks
                api_data = google_data.copy()
                
                # Use OpenLibrary data for missing fields
                if not api_data.get('description') and ol_data.get('description'):
                    api_data['description'] = ol_data['description']
                if not api_data.get('publisher') and ol_data.get('publisher'):
                    api_data['publisher'] = ol_data['publisher']
                if not api_data.get('page_count') and ol_data.get('page_count'):
                    api_data['page_count'] = ol_data['page_count']
                
                # Merge categories
                google_cats = set(api_data.get('categories', []))
                ol_cats = set(ol_data.get('categories', []))
                api_data['categories'] = list(google_cats.union(ol_cats))
                
                # Add OpenLibrary ID
                if ol_data.get('openlibrary_id'):
                    api_data['openlibrary_id'] = ol_data['openlibrary_id']
                
                api_data['sources'] = ['google_books', 'openlibrary']
                
            elif google_data:
                api_data = google_data
            elif ol_data:
                api_data = ol_data
            
            
            # Enhanced ISBN handling from API
            if not isbn13 and api_data.get('isbn13'):
                isbn13 = api_data['isbn13']
            if not isbn10 and api_data.get('isbn10'):
                isbn10 = api_data['isbn10']
            
            # Enhanced cover image handling with caching
            if api_data.get('cover_url') and not cover_url:  # Only if no manual cover URL provided
                final_cover_url = api_data['cover_url']
                
                # Download and cache cover image
                try:
                    import requests
                    from pathlib import Path
                    
                    # Use persistent covers directory in data folder
                    import os
                    covers_dir = Path('/app/data/covers')
                    
                    # Fallback to local development path if Docker path doesn't exist
                    if not covers_dir.exists():
                        # Check config for data directory path
                        data_dir = getattr(current_app.config, 'DATA_DIR', None)
                        if data_dir:
                            covers_dir = Path(data_dir) / 'covers'
                        else:
                            # Last resort - use relative path from app root
                            base_dir = Path(current_app.root_path).parent
                            covers_dir = base_dir / 'data' / 'covers'
                    
                    covers_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Unified processing pipeline
                    cached_cover_url = process_image_from_url(final_cover_url)
                    
                except Exception as e:
                    cached_cover_url = final_cover_url  # Fallback to original URL
        else:
            pass
    
    # Enhanced field mapping with API fallbacks
    final_title = title or (api_data.get('title') if api_data else '')
    final_subtitle = subtitle if subtitle else (api_data.get('subtitle') if api_data else None)
    final_description = description or (api_data.get('description') if api_data else '')
    final_publisher = publisher_name or (api_data.get('publisher') if api_data else '')
    final_language = language or (api_data.get('language') if api_data else 'en')
    final_page_count = page_count or (api_data.get('page_count') if api_data else None)
    final_cover_url = cached_cover_url or final_cover_url or cover_url
    
    # Enhanced published date handling
    final_published_date = published_date
    if not final_published_date and api_data and api_data.get('published_date'):
        try:
            api_date_str = api_data['published_date']
            final_published_date = datetime.strptime(api_date_str, '%Y-%m-%d').date()
        except ValueError:
            try:
                final_published_date = datetime.strptime(api_date_str, '%Y').date()
            except ValueError:
                pass

    # Enhanced category processing with API integration
    final_categories = []
    
    # Add API categories first
    if api_data and api_data.get('categories'):
        api_categories = api_data['categories']
        if isinstance(api_categories, list):
            final_categories.extend(api_categories)
        elif isinstance(api_categories, str):
            # Handle comma-separated string categories
            final_categories.extend([cat.strip() for cat in api_categories.split(',') if cat.strip()])
    
    # Add manual categories
    final_categories.extend(manual_categories)
    
    # Remove duplicates while preserving order and normalize
    seen = set()
    unique_categories = []
    for cat in final_categories:
        cat_normalized = cat.lower().strip()
        if cat_normalized and cat_normalized not in seen:
            unique_categories.append(cat.strip())
            seen.add(cat_normalized)
    
    
    # Use manual publisher if provided, otherwise use API publisher
    final_publisher = publisher_name or publisher

    try:
        # Enhanced contributors creation from API data
        contributors = []
        
        # Prioritize individual authors from enhanced API data
        if api_data and api_data.get('authors_list'):
            for i, author_name in enumerate(api_data['authors_list']):
                author_name = author_name.strip()
                if author_name:
                    person = Person(id=str(uuid.uuid4()), name=author_name)
                    contribution = BookContribution(
                        person=person,
                        contribution_type=ContributionType.AUTHORED,
                        order=i
                    )
                    contributors.append(contribution)
                    print(f"👤 [MANUAL] Added author from API: {author_name}")
        elif author:
            # Fallback to manual form entry
            person = Person(id=str(uuid.uuid4()), name=author)
            contribution = BookContribution(
                person=person,
                contribution_type=ContributionType.AUTHORED,
                order=0
            )
            contributors.append(contribution)
            print(f"👤 [MANUAL] Added author from form: {author}")
            
        # Enhanced domain book object creation with comprehensive field mapping
        domain_book = DomainBook(
            id=str(uuid.uuid4()),
            title=final_title,
            subtitle=final_subtitle,
            description=final_description if final_description else None,
            published_date=final_published_date,
            page_count=final_page_count,
            language=final_language,
            cover_url=final_cover_url,
            isbn13=isbn13,
            isbn10=isbn10,
            asin=api_data.get('asin') if api_data else None,
            google_books_id=api_data.get('google_books_id') if api_data else None,
            openlibrary_id=api_data.get('openlibrary_id') if api_data else None,
            average_rating=api_data.get('average_rating') if api_data else None,
            rating_count=api_data.get('rating_count') if api_data else None,
            contributors=contributors,
            publisher=Publisher(id=str(uuid.uuid4()), name=final_publisher) if final_publisher else None,
            series=Series(id=str(uuid.uuid4()), name=series) if series else None,
            series_volume=series_volume if series_volume else None,
            series_order=series_order,
            categories=unique_categories or [],
            raw_categories=unique_categories,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        # Use find_or_create_book to avoid duplicates (global)
        existing_book = book_service.find_or_create_book_sync(domain_book)
        
        # Categories are already processed by find_or_create_book_sync, no need to process again
        if unique_categories:
            pass  # Categories already handled
        else:
            pass  # No categories to process
        
        # Add to user's library with custom metadata and location
        # Determine location to use: form-selected location takes priority, then default location
        final_locations = []
        try:
            from app.location_service import LocationService
            from app.utils.safe_kuzu_manager import safe_get_connection
            from config import Config
            
            location_service = LocationService()
            
            # Check if user selected a location in the form
            if location_id:
                final_locations = [location_id]
            else:
                # Get default location (universal)
                default_location = location_service.get_default_location()
                
                if default_location:
                    final_locations = [default_location.id]
                else:
                    # Check if user has any locations at all
                    all_locations = location_service.get_all_locations()
                    if not all_locations:
                        default_locations_created = location_service.setup_default_locations()
                        if default_locations_created:
                            final_locations = [default_locations_created[0].id]
                        else:
                            final_locations = []  # No default location available
                    else:
                        final_locations = [all_locations[0].id]  # Use first available location
                
        except Exception as e:
            import traceback
            traceback.print_exc()
        
        
        # Convert reading status string to enum
        reading_status_enum = ReadingStatus.PLAN_TO_READ  # Default
        if reading_status:
            try:
                reading_status_enum = ReadingStatus(reading_status)
            except ValueError:
                pass
        
        # Convert ownership status string to enum
        ownership_status_enum = None
        if ownership_status:
            try:
                from .domain.models import OwnershipStatus
                ownership_status_enum = OwnershipStatus(ownership_status)
            except ValueError:
                pass
        
        # Convert media type string to enum
        media_type_enum = None
        if media_type:
            try:
                from .domain.models import MediaType
                media_type_enum = MediaType(media_type)
            except ValueError:
                pass
        
        # Extract the first location ID for the simplified book service
        location_id = final_locations[0] if final_locations else None
        
        result = book_service.add_book_to_user_library_sync(
            user_id=current_user.id,
            book_id=existing_book.id,
            reading_status=reading_status_enum,
            location_id=location_id,
            custom_metadata=custom_metadata if custom_metadata else None
        )
        
        # Update additional fields if specified
        update_data = {}
        if ownership_status_enum:
            update_data['ownership_status'] = ownership_status_enum
        if media_type_enum:
            update_data['media_type'] = media_type_enum
        if user_rating:
            update_data['user_rating'] = user_rating
        if personal_notes:
            update_data['personal_notes'] = personal_notes
        if user_tags:
            update_data['user_tags'] = user_tags
        if start_date:
            update_data['start_date'] = start_date
        if finish_date:
            update_data['finish_date'] = finish_date
        # Note: location is already handled via the locations parameter above
            
        if update_data:
            book_service.update_book_sync(existing_book.uid, str(current_user.id), **update_data)
        
        if existing_book.id == domain_book.id:
            # New book was created
            if custom_metadata:
                flash(f'Book "{title}" added successfully with {len(custom_metadata)} custom fields.', 'success')
            else:
                flash(f'Book "{title}" added successfully to your library.', 'success')
            return redirect(url_for('main.library'))
        else:
            # Existing book was found
            if custom_metadata:
                flash(f'Book "{title}" already exists. Added to your collection with {len(custom_metadata)} custom fields.', 'info')
            else:
                flash(f'Book "{title}" already exists in the library. Added to your collection.', 'info')
            return redirect(url_for('main.library'))
            
    except Exception as e:
        current_app.logger.error(f"Error adding book manually: {e}")
        flash('An error occurred while adding the book. Please try again.', 'danger')

    return redirect(url_for('main.library'))