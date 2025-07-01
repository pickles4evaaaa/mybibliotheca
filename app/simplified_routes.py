"""
Simplified book creation route using the new decoupled architecture.
This replaces the complex add_book_manual route with a cleaner approach.
"""

from flask import request, flash, redirect, url_for, current_app
from flask_login import login_required, current_user
from datetime import datetime

from .simplified_book_service import SimplifiedBook, create_simplified_book_service
from .services import normalize_isbn_upc, get_google_books_cover, fetch_book_data


@login_required 
def add_book_manual_simplified():
    """
    Simplified book addition route using decoupled architecture.
    """
    try:
        # Validate required fields
        title = request.form['title'].strip()
        if not title:
            flash('Error: Title is required to add a book.', 'danger')
            return redirect(url_for('main.library'))

        # Extract form fields
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
        genres = request.form.get('genres', '').strip()
        reading_status = request.form.get('reading_status', '').strip() or 'plan_to_read'
        ownership_status = request.form.get('ownership_status', '').strip() or 'owned'
        media_type = request.form.get('media_type', '').strip() or 'physical'
        location_id = request.form.get('location_id', '').strip()
        user_rating_str = request.form.get('user_rating', '').strip()
        personal_notes = request.form.get('personal_notes', '').strip()
        description = request.form.get('description', '').strip()

        # Convert numeric fields safely
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

        # Normalize ISBN
        normalized_isbn = None
        isbn13 = None
        isbn10 = None
        if isbn:
            normalized_isbn = normalize_isbn_upc(isbn)
            if normalized_isbn:
                if len(normalized_isbn) == 13:
                    isbn13 = normalized_isbn
                elif len(normalized_isbn) == 10:
                    isbn10 = normalized_isbn
                print(f"📚 [SIMPLIFIED] Normalized ISBN: {isbn} -> {normalized_isbn}")

        # Process categories
        categories = []
        if genres:
            categories = [cat.strip() for cat in genres.split(',') if cat.strip()]

        # Fetch additional metadata if ISBN is provided
        if normalized_isbn and not (description and cover_url):
            try:
                # Try Google Books first
                google_data = get_google_books_cover(normalized_isbn, fetch_title_author=True)
                if google_data:
                    if not description and google_data.get('description'):
                        description = google_data.get('description')
                    if not cover_url and google_data.get('cover'):
                        cover_url = google_data.get('cover')
                    if not publisher_name and google_data.get('publisher'):
                        publisher_name = google_data.get('publisher')
                    if not page_count and google_data.get('page_count'):
                        page_count = google_data.get('page_count')
                    if not published_date_str and google_data.get('published_date'):
                        published_date_str = google_data.get('published_date')
                    if not categories and google_data.get('categories'):
                        api_categories = google_data.get('categories')
                        if isinstance(api_categories, list):
                            categories.extend(api_categories)
                        elif isinstance(api_categories, str):
                            categories.append(api_categories)
                    print(f"📚 [SIMPLIFIED] Enhanced with Google Books data")
                else:
                    # Fallback to OpenLibrary
                    ol_data = fetch_book_data(normalized_isbn)
                    if ol_data:
                        if not description and ol_data.get('description'):
                            description = ol_data.get('description')
                        if not cover_url and ol_data.get('cover'):
                            cover_url = ol_data.get('cover')
                        if not publisher_name and ol_data.get('publisher'):
                            publisher_name = ol_data.get('publisher')
                        if not page_count and ol_data.get('page_count'):
                            page_count = ol_data.get('page_count')
                        if not published_date_str and ol_data.get('published_date'):
                            published_date_str = ol_data.get('published_date')
                        print(f"📚 [SIMPLIFIED] Enhanced with OpenLibrary data")
            except Exception as e:
                print(f"⚠️ [SIMPLIFIED] API fetch failed, using manual data: {e}")

        # Process custom metadata
        custom_metadata = {}
        for key, value in request.form.items():
            if key.startswith('custom_field_') and value and value.strip():
                field_name = key.replace('custom_field_', '')
                custom_metadata[field_name] = value.strip()
                print(f"📋 [SIMPLIFIED] Custom field: {field_name} = {value.strip()}")

        # Create simplified book data
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
            series_order=series_order,
            categories=categories if categories else None
        )

        # Create simplified book service
        book_service = create_simplified_book_service()
        
        # Add book to user library using simplified service
        success = book_service.add_book_to_user_library(
            book_data=book_data,
            user_id=str(current_user.id),
            reading_status=reading_status,
            ownership_status=ownership_status,
            media_type=media_type,
            user_rating=user_rating,
            personal_notes=personal_notes,
            location_id=location_id if location_id else None,
            custom_metadata=custom_metadata if custom_metadata else None
        )

        if success:
            if custom_metadata:
                flash(f'Book "{title}" added successfully with {len(custom_metadata)} custom fields.', 'success')
            else:
                flash(f'Book "{title}" added successfully to your library.', 'success')
        else:
            flash('Failed to add book to your library. Please try again.', 'danger')

    except Exception as e:
        current_app.logger.error(f"Error in simplified book addition: {e}")
        flash('An error occurred while adding the book. Please try again.', 'danger')

    return redirect(url_for('main.library'))
