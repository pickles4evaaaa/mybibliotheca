"""
Reading Log Routes

Handles reading log functionality including creating, viewing, and managing reading sessions.
"""

from flask import Blueprint, request, jsonify, render_template, flash, redirect, url_for
from flask_login import login_required, current_user
from datetime import datetime, date, timezone, timedelta
from typing import Any, Dict, Optional
import logging

from app.forms import ReadingLogEntryForm
from app.services import reading_log_service, book_service
from app.services.personal_metadata_service import personal_metadata_service
from app.domain.models import ReadingLog, ReadingStatus
from app.utils.user_settings import get_effective_reading_defaults

logger = logging.getLogger(__name__)

reading_logs = Blueprint('reading_logs', __name__, url_prefix='/reading-logs')


@reading_logs.route('/create', methods=['POST'])
@login_required
def create_reading_log_entry():
    """Create a new reading log entry."""
    try:
        # Get form data
        book_id = request.form.get('book_id')
        start_page = request.form.get('start_page')
        end_page = request.form.get('end_page')
        pages_read = request.form.get('pages_read')
        minutes_read = request.form.get('minutes_read')
        notes = request.form.get('notes', '').strip()
        
        # Validate required fields
        if not book_id:
            return jsonify({
                'status': 'error',
                'message': 'Book selection is required'
            }), 400
        
        # Calculate pages read if not provided
        if not pages_read and start_page and end_page:
            try:
                start = int(start_page)
                end = int(end_page)
                if end >= start:
                    pages_read = str(end - start + 1)
            except (ValueError, TypeError):
                pass
        
        # Convert to integers with validation
        try:
            pages_read_int = int(pages_read) if pages_read else 0
            minutes_read_int = int(minutes_read) if minutes_read else 0
        except (ValueError, TypeError):
            return jsonify({
                'status': 'error',
                'message': 'Invalid page or minute values'
            }), 400
        
        # Apply defaults if both zero/missing
        if pages_read_int <= 0 and minutes_read_int <= 0:
            dp, dm = get_effective_reading_defaults(getattr(current_user, 'id', None))
            if (dp or 0) > 0:
                pages_read_int = int(dp)  # type: ignore[arg-type]
            if (dm or 0) > 0:
                minutes_read_int = int(dm)  # type: ignore[arg-type]
        # Re-validate after defaults
        if pages_read_int <= 0 and minutes_read_int <= 0:
            return jsonify({
                'status': 'error',
                'message': 'Please provide either pages read or minutes read'
            }), 400
        
        # Universal library: verify book exists globally (personal metadata optional)
        user_book = book_service.get_book_by_id_for_user_sync(book_id, current_user.id)
        if not user_book:
            user_book = book_service.get_book_by_id_sync(book_id)
        if not user_book:
            return jsonify({
                'status': 'error',
                'message': 'Book not found'
            }), 404
        
        # Create reading log entry
        log_entry_date = date.today()
        reading_log = ReadingLog(
            user_id=current_user.id,
            book_id=book_id,
            date=log_entry_date,
            pages_read=pages_read_int,
            minutes_read=minutes_read_int,
            notes=notes or None
        )
        
        # Use the reading log service to create the entry
        created_log = reading_log_service.create_reading_log_sync(reading_log)
        
        if created_log:
            # Ensure the personal start date is captured when logging begins
            try:
                existing_start = getattr(user_book, 'start_date', None)
                if not existing_start:
                    start_dt = datetime(
                        log_entry_date.year,
                        log_entry_date.month,
                        log_entry_date.day,
                        tzinfo=timezone.utc
                    )
                    personal_metadata_service.ensure_start_date(current_user.id, book_id, start_dt)

                # Promote reading status to "reading" when logging begins unless already finished/abandoned
                raw_status = getattr(user_book, 'reading_status', None)
                if isinstance(raw_status, ReadingStatus):
                    status_value = raw_status.value
                else:
                    status_value = (raw_status or '').strip() if isinstance(raw_status, str) else ''

                normalized_status = status_value.lower() if status_value else ''
                terminal_statuses = {'read', 'finished', 'completed', 'did_not_finish', 'did-not-finish', 'dnf'}

                if normalized_status not in terminal_statuses and normalized_status != ReadingStatus.READING.value:
                    personal_metadata_service.update_personal_metadata(
                        current_user.id,
                        book_id,
                        custom_updates={'reading_status': ReadingStatus.READING.value}
                    )
            except Exception as ensure_exc:
                logger.warning(f"Unable to ensure reading metadata for book {book_id}: {ensure_exc}")

            return jsonify({
                'status': 'success',
                'message': f'Logged {pages_read_int} pages' + (f' and {minutes_read_int} minutes' if minutes_read_int > 0 else '') + ' of reading!'
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to save reading log entry'
            }), 500
            
    except Exception as e:
        logger.error(f"Error creating reading log entry: {e}")
        return jsonify({
            'status': 'error',
            'message': 'An error occurred while logging your reading session'
        }), 500


@reading_logs.route('/my-logs')
@login_required
def my_reading_logs():
    """View current user's reading logs."""
    try:
        # Get user's reading logs
        logs = reading_log_service.get_user_reading_logs_sync(current_user.id, days_back=30)
        
        return render_template('reading_logs/my_logs.html', 
                             logs=logs,
                             title='My Reading Logs')
                             
    except Exception as e:
        logger.error(f"Error retrieving reading logs: {e}")
        flash('Error retrieving reading logs', 'error')
        return redirect(url_for('main.library'))


@reading_logs.route('/api/user/books')
@login_required
def api_user_books():
    """API endpoint to get user's books for the reading log modal, prioritizing recent reads.

        Returns a JSON object containing:
            - books: [{id, title, authors, series?}]
            - suggested_book (optional): most recent in-progress title for auto-prefill

        The first up-to-5 items in books are the most recently read titles (by
        recent reading logs), followed by newly added books and general library
        entries to reach the requested limit.
    """
    try:
        # Get limit from query parameter, default to 20 for recent reads, max 100
        limit = request.args.get('limit', default=20, type=int)
        limit = min(limit, 100)  # Cap at 100 books
        query = (request.args.get('q') or '').strip()

        def authors_string(book) -> str:
            """Build a concise authors string from contributors/authors."""
            authors_str = ''
            if hasattr(book, 'contributors') and getattr(book, 'contributors'):
                names = [c.person.name for c in book.contributors
                         if getattr(c, 'contribution_type', None) and getattr(c.contribution_type, 'value', None) in ['authored', 'co_authored']]
                if names:
                    authors_str = ', '.join(names[:3])
                    if len(names) > 3:
                        authors_str += ' et al.'
            elif hasattr(book, 'authors') and getattr(book, 'authors'):
                names = [a.name for a in book.authors]
                if names:
                    authors_str = ', '.join(names[:3])
                    if len(names) > 3:
                        authors_str += ' et al.'
            return authors_str

        def make_payload(book):
            series_obj = getattr(book, 'series', None)
            series_name = getattr(series_obj, 'name', None) if series_obj else None
            payload = {
                'id': book.id,
                'title': book.title,
                'authors': authors_string(book),
                'author': authors_string(book)
            }
            if series_name:
                payload['series'] = series_name
            return payload

        def qualifies_for_in_progress_prefill(book_obj: Optional[object], meta: Optional[Dict[str, Any]] = None) -> bool:
            if not book_obj:
                return False

            def _has_finish(value: Any) -> bool:
                if value is None:
                    return False
                if isinstance(value, str):
                    return bool(value.strip())
                return True

            meta_status_raw: Any = None
            meta_finish_raw: Any = None
            latest_log_raw: Any = None
            if isinstance(meta, dict):
                meta_status_raw = meta.get('reading_status') or meta.get('status') or meta.get('relationship_status')
                meta_finish_raw = meta.get('finish_date') or meta.get('finishDate')
                latest_log_raw = meta.get('latest_log_date') or meta.get('latestLogDate') or meta.get('last_logged_at')

            if _has_finish(meta_finish_raw) or _has_finish(getattr(book_obj, 'finish_date', None)):
                return False

            raw_status = getattr(book_obj, 'reading_status', None)
            if isinstance(raw_status, ReadingStatus):
                status_val = raw_status.value
            else:
                status_val = str(raw_status or '')
            normalized_status = status_val.strip().lower()

            meta_status = ''
            if isinstance(meta_status_raw, ReadingStatus):
                meta_status = meta_status_raw.value
            elif meta_status_raw is not None:
                meta_status = str(meta_status_raw)
            meta_status_norm = meta_status.strip().lower()

            terminal_statuses = {
                'read', 'finished', 'completed', 'did_not_finish', 'did-not-finish', 'dnf', 'abandoned', 'not_interested',
                'finished_reading', 'complete', 'completed_reading'
            }
            in_progress_statuses = {'reading', 'currently_reading', 'in_progress', 'reading_now'}

            if normalized_status in terminal_statuses or meta_status_norm in terminal_statuses:
                return False

            if normalized_status in in_progress_statuses or meta_status_norm in in_progress_statuses:
                return True

            if normalized_status or meta_status_norm:
                # Status provided but not in recognized in-progress set; don't auto-prefill
                return False

            if latest_log_raw:
                latest_log_date: Optional[date] = None
                if isinstance(latest_log_raw, datetime):
                    latest_log_date = latest_log_raw.date()
                elif isinstance(latest_log_raw, date):
                    latest_log_date = latest_log_raw
                elif isinstance(latest_log_raw, str):
                    candidate = latest_log_raw.strip()
                    if candidate:
                        candidate = candidate.replace('Z', '+00:00')
                        try:
                            latest_log_date = datetime.fromisoformat(candidate).date()
                        except ValueError:
                            try:
                                latest_log_date = date.fromisoformat(candidate.split('T')[0])
                            except ValueError:
                                latest_log_date = None

                if latest_log_date:
                    today = datetime.now(timezone.utc).date()
                    if latest_log_date <= today and (today - latest_log_date) <= timedelta(days=14):
                        return True

            return False

        if query:
            matched_books = book_service.search_books_sync(query, current_user.id, limit=limit)
            results = [make_payload(book) for book in matched_books if book]
            return jsonify(results)

        # 1) Recently read (prioritized)
        recently_read = reading_log_service.get_recently_read_books_sync(current_user.id, limit=5)

        books: list[dict] = []
        seen: set = set()
        suggested_payload: Optional[dict] = None

        for bdict in recently_read or []:
            bid = bdict.get('id')
            if not bid or bid in seen:
                continue
            b = book_service.get_book_by_id_for_user_sync(bid, current_user.id)
            if not b:
                b = book_service.get_book_by_id_sync(bid)
            if not b:
                continue
            metadata_snapshot: Dict[str, Any] = {}
            try:
                metadata_snapshot = personal_metadata_service.get_personal_metadata(current_user.id, bid) or {}
            except Exception as meta_exc:
                logger.debug(f"Unable to load personal metadata for book {bid}: {meta_exc}")

            if metadata_snapshot.get('reading_status') and not getattr(b, 'reading_status', None):
                try:
                    setattr(b, 'reading_status', metadata_snapshot.get('reading_status'))
                except Exception:
                    pass
            if metadata_snapshot.get('finish_date') and not getattr(b, 'finish_date', None):
                try:
                    setattr(b, 'finish_date', metadata_snapshot.get('finish_date'))
                except Exception:
                    pass

            meta_context: Dict[str, Any] = {}
            if isinstance(bdict, dict):
                meta_context.update(bdict)
            if metadata_snapshot:
                meta_context.update(metadata_snapshot)

            payload = make_payload(b)
            if suggested_payload is None and qualifies_for_in_progress_prefill(b, meta_context):
                suggested_payload = dict(payload)
            books.append(payload)
            seen.add(b.id)

        # 2) Include most recently added books in the global library
        if len(books) < limit:
            recently_added = book_service.get_recently_added_books_sync(limit=min(5, limit - len(books)))
            for b in recently_added:
                if len(books) >= limit:
                    break
                if not getattr(b, 'id', None) or b.id in seen:
                    continue
                books.append(make_payload(b))
                seen.add(b.id)

        # 3) Fill with additional books to reach the requested limit
        if len(books) < limit:
            remaining = limit - len(books)
            more = book_service.get_books_for_user(current_user.id, limit=remaining + 50)
            for b in more:
                if len(books) >= limit:
                    break
                if not getattr(b, 'id', None) or b.id in seen:
                    continue
                books.append(make_payload(b))
                seen.add(b.id)

        response_body: Dict[str, Any] = {'books': books}
        if suggested_payload:
            response_body['suggested_book'] = suggested_payload

        return jsonify(response_body)

    except Exception as e:
        logger.error(f"Error getting user books for API: {e}")
        return jsonify({'status': 'error', 'message': 'Failed to load books'}), 500


@reading_logs.route('/stats')
@login_required
def reading_stats():
    """View reading statistics and analytics."""
    try:
        # Get reading statistics
        stats = reading_log_service.get_user_reading_stats_sync(current_user.id)
        
        return render_template('reading_logs/stats.html',
                             stats=stats,
                             title='Reading Statistics')
                             
    except Exception as e:
        logger.error(f"Error retrieving reading stats: {e}")
        flash('Error retrieving reading statistics', 'error')
        return redirect(url_for('main.library'))


@reading_logs.route('/delete/<log_id>', methods=['POST'])
@login_required
def delete_reading_log(log_id):
    """Delete a reading log entry."""
    try:
        success = reading_log_service.delete_reading_log_sync(log_id, current_user.id)
        
        if success:
            flash('Reading log entry deleted successfully', 'success')
        else:
            flash('Failed to delete reading log entry', 'error')
            
        return redirect(url_for('reading_logs.my_reading_logs'))
        
    except Exception as e:
        logger.error(f"Error deleting reading log {log_id}: {e}")
        flash('Error deleting reading log entry', 'error')
        return redirect(url_for('reading_logs.my_reading_logs'))


@reading_logs.route('/edit/<log_id>', methods=['GET', 'POST'])
@login_required
def edit_reading_log(log_id):
    """Edit an existing reading log entry."""
    try:
        # Get the existing reading log
        existing_log = reading_log_service.get_reading_log_by_id_sync(log_id, current_user.id)
        
        if not existing_log:
            flash('Reading log not found', 'error')
            return redirect(url_for('reading_logs.my_reading_logs'))
        
        if request.method == 'GET':
            # Show edit form
            return render_template('reading_logs/edit_log.html', 
                                 log=existing_log)
        
        # Handle POST - update the log
        pages_read = request.form.get('pages_read', 0)
        minutes_read = request.form.get('minutes_read', 0)
        notes = request.form.get('notes', '').strip()
        # Ensure we always have a string to parse; fall back to existing log date or today
        log_date_str = request.form.get('date') or str(existing_log.get('date') or date.today().isoformat())
        
        # Handle book association changes
        new_book_id = request.form.get('new_book_id', '').strip()
        new_book_title = request.form.get('new_book_title', '').strip()
        current_book_id = request.form.get('book_id', '').strip()
        current_book_title = request.form.get('book_title', '').strip()
        
        # Determine final book association
        final_book_id = None
        final_book_title = None
        
        if new_book_id:
            # User selected a new book
            final_book_id = new_book_id
            final_book_title = None  # Clear book_title when linked to a book
        elif new_book_title:
            # User entered a custom book title (bookless mode)
            final_book_id = None
            final_book_title = new_book_title
        else:
            # No changes made, keep current values
            final_book_id = current_book_id if current_book_id else None
            final_book_title = current_book_title if current_book_title else None
        
        # Parse date
        try:
            log_date = datetime.strptime(log_date_str, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            flash('Invalid date format. Please use YYYY-MM-DD', 'error')
            return render_template('reading_logs/edit_log.html', log=existing_log)
        
        # Convert numeric fields
        try:
            pages_read = int(pages_read) if pages_read else 0
            minutes_read = int(minutes_read) if minutes_read else 0
        except ValueError:
            flash('Pages and minutes must be valid numbers', 'error')
            return render_template('reading_logs/edit_log.html', log=existing_log)
        
        # Validate that we have either pages or minutes
        if pages_read <= 0 and minutes_read <= 0:
            flash('Must specify either pages read or minutes read', 'error')
            return render_template('reading_logs/edit_log.html', log=existing_log)
        
        # Create updated reading log object
        updated_log = ReadingLog(
            user_id=current_user.id,
            book_id=final_book_id,
            date=log_date,
            pages_read=pages_read,
            minutes_read=minutes_read,
            notes=notes or None,
            created_at=datetime.fromisoformat(existing_log['created_at']) if existing_log.get('created_at') else datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
        )
        
        # Update the log
        result = reading_log_service.update_reading_log_sync(log_id, updated_log)
        
        if result:
            flash('Reading log updated successfully', 'success')
            return redirect(url_for('reading_logs.my_reading_logs'))
        else:
            flash('Failed to update reading log', 'error')
            return render_template('reading_logs/edit_log.html', log=existing_log)
        
    except Exception as e:
        logger.error(f"Error editing reading log {log_id}: {e}")
        flash('Error updating reading log', 'error')
        return redirect(url_for('reading_logs.my_reading_logs'))


@reading_logs.route('/quick-add', methods=['POST'])
@login_required
def quick_add_bookless_log():
    """Quick add a reading log without a specific book."""
    try:
        pages_read = request.form.get('pages_read', 0)
        minutes_read = request.form.get('minutes_read', 0)
        notes = request.form.get('notes', '').strip()
        book_title = request.form.get('book_title', '').strip()
        log_date_str = request.form.get('date', date.today().isoformat())
        
        # Parse date
        try:
            log_date = datetime.strptime(log_date_str, '%Y-%m-%d').date()
        except (ValueError, TypeError):
            return jsonify({
                'status': 'error',
                'message': 'Invalid date format. Please use YYYY-MM-DD'
            }), 400
        
        # Convert numeric fields
        try:
            pages_read = int(pages_read) if pages_read else 0
            minutes_read = int(minutes_read) if minutes_read else 0
        except ValueError:
            return jsonify({
                'status': 'error',
                'message': 'Pages and minutes must be valid numbers'
            }), 400
        
        # Apply defaults if needed
        if pages_read <= 0 and minutes_read <= 0:
            dp, dm = get_effective_reading_defaults(getattr(current_user, 'id', None))
            if (dp or 0) > 0:
                pages_read = int(dp)  # type: ignore[arg-type]
            if (dm or 0) > 0:
                minutes_read = int(dm)  # type: ignore[arg-type]
        # Validate that we have either pages or minutes
        if pages_read <= 0 and minutes_read <= 0:
            return jsonify({
                'status': 'error',
                'message': 'Must specify either pages read or minutes read'
            }), 400
        
        # Create reading log
        reading_log = ReadingLog(
            user_id=current_user.id,
            book_id=None,  # Bookless log
            date=log_date,
            pages_read=pages_read,
            minutes_read=minutes_read,
            notes=notes or None
        )
        
        result = reading_log_service.create_reading_log_sync(reading_log)
        
        if result:
            return jsonify({
                'status': 'success',
                'message': 'Reading log added successfully',
                'log': result
            })
        else:
            return jsonify({
                'status': 'error',
                'message': 'Failed to create reading log'
            }), 500
        
    except Exception as e:
        logger.error(f"Error creating quick reading log: {e}")
        return jsonify({
            'status': 'error',
            'message': 'Error creating reading log'
        }), 500
