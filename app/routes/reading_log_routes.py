"""
Reading Log Routes

Handles reading log functionality including creating, viewing, and managing reading sessions.
"""

from flask import Blueprint, request, jsonify, render_template, flash, redirect, url_for
from flask_login import login_required, current_user
from datetime import datetime, date
import logging

from app.forms import ReadingLogEntryForm
from app.services import reading_log_service, book_service
from app.domain.models import ReadingLog

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
        
        # Ensure at least one metric is provided
        if pages_read_int <= 0 and minutes_read_int <= 0:
            return jsonify({
                'status': 'error',
                'message': 'Please provide either pages read or minutes read'
            }), 400
        
        # Verify user owns the book
        user_book = book_service.get_book_by_id_for_user_sync(book_id, current_user.id)
        if not user_book:
            return jsonify({
                'status': 'error',
                'message': 'Book not found in your library'
            }), 404
        
        # Create reading log entry
        reading_log = ReadingLog(
            user_id=current_user.id,
            book_id=book_id,
            date=date.today(),
            pages_read=pages_read_int,
            minutes_read=minutes_read_int,
            notes=notes or None
        )
        
        # Use the reading log service to create the entry
        created_log = reading_log_service.create_reading_log_sync(reading_log)
        
        if created_log:
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
    """API endpoint to get user's books for the reading log modal, prioritizing recent reads."""
    try:
        # First, try to get recently read books (last 5 books with reading logs)
        recently_read = reading_log_service.get_recently_read_books_sync(current_user.id, limit=5)
        
        book_data = []
        book_ids_added = set()
        
        # Add recently read books first
        for book_dict in recently_read:
            book_id = book_dict.get('id')
            if book_id and book_id not in book_ids_added:
                # Get the full book details
                book = book_service.get_book_by_id_sync(book_id)
                if book:
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
                    book_ids_added.add(book.id)
        
        # If we don't have enough books (less than 5), fill with user's regular books
        if len(book_data) < 5:
            all_books = book_service.get_books_for_user(current_user.id, limit=50)
            
            for book in all_books:
                if len(book_data) >= 5:
                    break
                    
                if book.id not in book_ids_added:
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
                    book_ids_added.add(book.id)
        
        # If still not enough books, fall back to any user books
        if len(book_data) < 5:
            all_books = book_service.get_books_for_user(current_user.id, limit=50)
            
            for book in all_books:
                if len(book_data) >= 5:
                    break
                    
                if book.id not in book_ids_added:
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
                    book_ids_added.add(book.id)
        
        # Get all remaining books for search functionality
        all_remaining_books = book_service.get_books_for_user(current_user.id, limit=1000)
        for book in all_remaining_books:
            if book.id not in book_ids_added:
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
