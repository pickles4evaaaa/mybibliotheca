"""Library page route implementation.

The public blueprint registration remains in book_routes.  This module holds
the existing library-page handler and its local presentation helpers without
changing retrieval, filtering, ordering, or persistence behavior.
"""

from __future__ import annotations

from flask import current_app, jsonify, make_response, render_template, request
from flask_login import current_user, login_required

from app.domain.models import MediaType, ReadingStatus
from app.services import book_service, user_service
from app.utils.author_sorting import author_first_sort_key_for_book, author_last_sort_key_for_book
from app.utils.library_search import library_book_matches_query
from app.utils.simple_cache import cache_get, cache_set, get_user_library_version
from app.utils.user_settings import get_library_view_defaults
from .book_route_helpers import _humanize_status


@login_required
def library():
    # Determine per-user defaults for status/sort fallbacks
    try:
        default_status, default_sort = get_library_view_defaults(str(current_user.id))
    except Exception:
        default_status, default_sort = ('all', 'title_asc')

    # Get filter parameters from URL, falling back to per-user defaults
    raw_status = request.args.get('status_filter')
    status_filter = (raw_status.strip().lower() if isinstance(raw_status, str) else '') or default_status
    category_filter = request.args.get('category', '')
    publisher_filter = request.args.get('publisher', '')
    language_filter = request.args.get('language', '')
    location_filter = request.args.get('location', '')
    media_type_filter_raw = request.args.get('media_type', '')
    media_type_filter = media_type_filter_raw.lower() if media_type_filter_raw else ''
    finished_after_raw = request.args.get('finished_after', '')
    finished_before_raw = request.args.get('finished_before', '')
    raw_search_query = request.args.get('search', '')
    search_query = raw_search_query.strip() if isinstance(raw_search_query, str) else ''
    raw_sort_option = request.args.get('sort')
    sort_option = (raw_sort_option.strip().lower() if isinstance(raw_sort_option, str) else '') or default_sort

    # Custom QC filter: show only books flagged as Needs Review (typically missing ISBN at import time)
    needs_review_raw = request.args.get('needs_review', '')
    needs_review_only = False
    if isinstance(needs_review_raw, str):
        needs_review_only = needs_review_raw.strip().lower() in ('1', 'true', 'yes', 'on')
    elif needs_review_raw is not None:
        needs_review_only = bool(needs_review_raw)

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
        from app.utils.simple_cache import cache_get, cache_set, get_user_library_version
        _ver = get_user_library_version(str(current_user.id))
        _tc_key = f"total_count:{current_user.id}:v{_ver}"
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

    # Decide data retrieval strategy: if any filter is active OR non-default sort is used, pull all then filter/sort across full set
    offset = (page - 1) * per_page
    has_filter = any([
        status_filter and status_filter != 'all',
        bool(search_query),
        bool(category_filter.strip()) if isinstance(category_filter, str) else False,
        bool(publisher_filter.strip()) if isinstance(publisher_filter, str) else False,
        bool(language_filter.strip()) if isinstance(language_filter, str) else False,
        bool(location_filter.strip()) if isinstance(location_filter, str) else False,
        bool(media_type_filter.strip()) if isinstance(media_type_filter, str) else False,
        bool(finished_after_raw.strip()) if isinstance(finished_after_raw, str) else False,
        bool(finished_before_raw.strip()) if isinstance(finished_before_raw, str) else False,
        bool(needs_review_only),
        sort_option != 'title_asc',  # Treat non-default sort as requiring full fetch for proper ordering
    ])
    if has_filter:
        try:
            # Use short-lived cache for expensive all-books call
            from app.utils.simple_cache import cache_get, cache_set, get_user_library_version
            version = get_user_library_version(str(current_user.id))
            cache_key = f"all_books_overlay_flat:{current_user.id}:v{version}:v2"
            user_books = cache_get(cache_key)
            if user_books is None:
                user_books = book_service.get_all_books_with_user_overlay_flat_sync(str(current_user.id))
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
    from datetime import datetime
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
    
    # We'll compute stats after applying non-status filters.
    stats: Dict[str, Any] = {}

    # Start from the working set
    filtered_books = user_books

    def _resolve_media_type_value(book_obj):
        value = book_obj.get('media_type') if isinstance(book_obj, dict) else getattr(book_obj, 'media_type', None)
        if value is None:
            return None
        if hasattr(value, 'value'):
            value = value.value
        try:
            raw = str(value).strip().lower()
        except Exception:
            return None
        if not raw:
            return None

        simplified = ' '.join(raw.replace('-', ' ').replace('_', ' ').split())
        alias_map = {
            'physical': 'physical',
            'physical book': 'physical',
            'physicalbook': 'physical',
            'print': 'physical',
            'print book': 'physical',
            'printbook': 'physical',
            'paperback': 'physical',
            'hardcover': 'physical',
            'hard cover': 'physical',
            'ebook': 'ebook',
            'e book': 'ebook',
            'e-book': 'ebook',
            'digital': 'ebook',
            'digital book': 'ebook',
            'kindle': 'kindle',
            'audio book': 'audiobook',
            'audio-book': 'audiobook',
            'audiobook': 'audiobook',
            'audible': 'audiobook',
        }
        if simplified in alias_map:
            return alias_map[simplified]

        collapsed = simplified.replace(' ', '')
        if collapsed in alias_map:
            return alias_map[collapsed]

        return raw

    # Apply other filters (excluding status_filter so stat tiles remain meaningful)
    def _parse_finish_date(val):
        if not val:
            return None
        try:
            from datetime import date, datetime as _dt
            if isinstance(val, date) and not isinstance(val, _dt):
                return val
            if isinstance(val, _dt):
                return val.date()
            if isinstance(val, str):
                # Accept ISO strings like 2024-01-02 or full datetime
                try:
                    return _dt.fromisoformat(val.replace('Z', '')).date()
                except Exception:
                    pass
            return None
        except Exception:
            return None

    finished_after = _parse_finish_date(finished_after_raw)
    finished_before = _parse_finish_date(finished_before_raw)

    if search_query:
        from app.utils.library_search import library_book_matches_query

        def _book_as_dict(book_obj):
            if isinstance(book_obj, dict):
                return book_obj
            if hasattr(book_obj, '__dict__'):
                try:
                    return book_obj.__dict__
                except Exception:
                    return {}
            return {}

        filtered_books = [
            book for book in filtered_books
            if library_book_matches_query(_book_as_dict(book), search_query)
        ]
    
    if publisher_filter:
        filtered_books = [
            book for book in filtered_books 
            if (book.get('publisher') if isinstance(book, dict) else getattr(book, 'publisher', None)) and 
               publisher_filter.lower() in ((book.get('publisher', '') if isinstance(book, dict) else getattr(book, 'publisher', '')) or '').lower()
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

    if media_type_filter:
        filtered_books = [
            book for book in filtered_books
            if (_resolve_media_type_value(book) or '') == media_type_filter
        ]

    if finished_after or finished_before:
        def _book_finish_date(book_obj):
            val = book_obj.get('finish_date') if isinstance(book_obj, dict) else getattr(book_obj, 'finish_date', None)
            return _parse_finish_date(val)
        filtered_books = [
            book for book in filtered_books
            if (
                (fd := _book_finish_date(book)) is not None and
                (finished_after is None or fd >= finished_after) and
                (finished_before is None or fd <= finished_before)
            )
        ]

    if needs_review_only:
        def _book_needs_review(book_obj) -> bool:
            val = book_obj.get('needs_review') if isinstance(book_obj, dict) else getattr(book_obj, 'needs_review', None)
            if isinstance(val, bool):
                return val
            if isinstance(val, str):
                return val.strip().lower() in ('needs review', 'true', '1', 'yes', 'on')
            return bool(val)

        filtered_books = [book for book in filtered_books if _book_needs_review(book)]

    # Compute statistics for filter buttons.
    # If we're already in "full fetch" mode (has_filter), these reflect the current
    # filter context (excluding status_filter). Otherwise, fall back to cached global counts.
    if has_filter:
        computed_counts: Dict[str, int] = {
            'read': 0,
            'currently_reading': 0,
            'on_hold': 0,
            'plan_to_read': 0,
            'wishlist': 0,
        }
        for book in filtered_books or []:
            rs = get_reading_status(book)
            owner = get_ownership_status(book)

            if rs == 'read':
                computed_counts['read'] += 1
            elif rs in ('reading', 'currently_reading'):
                computed_counts['currently_reading'] += 1
            elif rs == 'on_hold':
                computed_counts['on_hold'] += 1
            elif rs == 'plan_to_read':
                computed_counts['plan_to_read'] += 1

            if isinstance(owner, str) and owner.strip().lower() == 'wishlist':
                computed_counts['wishlist'] += 1

        stats = {
            'total_books': int(len(filtered_books) if filtered_books is not None else 0),
            'books_read': int(computed_counts.get('read', 0)),
            'currently_reading': int(computed_counts.get('currently_reading', 0)),
            'want_to_read': int(computed_counts.get('plan_to_read', 0)),
            'on_hold': int(computed_counts.get('on_hold', 0)),
            'wishlist': int(computed_counts.get('wishlist', 0)),
            # Add location stats (page sample)
            'books_with_locations': books_with_locations,
            'books_without_locations': books_without_locations,
            'location_counts': location_counts
        }
    else:
        try:
            from app.utils.simple_cache import cache_get, cache_set, get_user_library_version
            _ver = get_user_library_version(str(current_user.id))
            # Include a small schema/version suffix to invalidate older cached shapes
            # (prevents lingering broken counts after upgrades).
            _sc_key = f"status_counts:{current_user.id}:v{_ver}:v2"
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

    # Apply status filter after other filters.
    if status_filter and status_filter != 'all':
        if status_filter == 'wishlist':
            filtered_books = [book for book in filtered_books if get_ownership_status(book) == 'wishlist']
        elif status_filter == 'reading':
            # Handle both 'reading' and 'currently_reading' for backwards compatibility
            filtered_books = [book for book in filtered_books if get_reading_status(book) in ['reading', 'currently_reading']]
        else:
            filtered_books = [book for book in filtered_books if get_reading_status(book) == status_filter]

    # Apply sorting
    def get_author_name(book):
        """Helper function to get author name safely"""
        if isinstance(book, dict):
            # Prefer Person table data via contributors
            contributors = book.get('contributors')
            if isinstance(contributors, list) and contributors:
                for contrib in contributors:
                    try:
                        ctype = contrib.get('contribution_type') if isinstance(contrib, dict) else getattr(contrib, 'contribution_type', None)
                        if hasattr(ctype, 'value'):
                            ctype = ctype.value
                        ctype = (str(ctype or '').strip().lower())
                        if ctype in ('authored', 'co_authored'):
                            person = contrib.get('person') if isinstance(contrib, dict) else getattr(contrib, 'person', None)
                            if isinstance(person, dict):
                                name = person.get('name')
                                if name:
                                    return str(name)
                            elif person is not None:
                                name = getattr(person, 'name', None)
                                if name:
                                    return str(name)
                    except Exception:
                        continue
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
            elif hasattr(book, 'contributors') and getattr(book, 'contributors', None):
                try:
                    for contrib in getattr(book, 'contributors') or []:
                        ctype = getattr(contrib, 'contribution_type', None)
                        if hasattr(ctype, 'value'):
                            ctype = ctype.value
                        ctype = (str(ctype or '').strip().lower())
                        if ctype in ('authored', 'co_authored'):
                            person = getattr(contrib, 'person', None)
                            if person is not None:
                                name = getattr(person, 'name', None)
                                if name:
                                    return str(name)
                except Exception:
                    pass
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
    
    def get_date_added_sort_key(book):
        """Helper function to get date added with title as secondary sort key.
        
        Returns tuple of (date, title) for stable sorting when dates are identical (bulk imports).
        """
        date_value = (
            book.get('added_at') or book.get('created_at') or '' 
            if isinstance(book, dict) 
            else getattr(book, 'added_at', None) or getattr(book, 'created_at', None) or ''
        )
        title_value = (book.get('title', '') if isinstance(book, dict) else getattr(book, 'title', '')).lower()
        return (date_value, title_value)
    
    if sort_option == 'title_asc':
        filtered_books.sort(key=lambda x: (x.get('title', '') if isinstance(x, dict) else getattr(x, 'title', '')).lower())
    elif sort_option == 'title_desc':
        filtered_books.sort(key=lambda x: (x.get('title', '') if isinstance(x, dict) else getattr(x, 'title', '')).lower(), reverse=True)
    elif sort_option == 'author_first_asc':
        filtered_books.sort(key=author_first_sort_key_for_book)
    elif sort_option == 'author_first_desc':
        filtered_books.sort(key=author_first_sort_key_for_book, reverse=True)
    elif sort_option == 'author_last_asc':
        filtered_books.sort(key=author_last_sort_key_for_book)
    elif sort_option == 'author_last_desc':
        filtered_books.sort(key=author_last_sort_key_for_book, reverse=True)
    elif sort_option == 'date_added_desc':
        # Sort by date added (newest first) - use added_at or created_at
        # Use title as secondary key for stable sorting when timestamps are identical (bulk imports)
        filtered_books.sort(key=get_date_added_sort_key, reverse=True)
    elif sort_option == 'date_added_asc':
        # Sort by date added (oldest first)
        # Use title as secondary key for stable sorting when timestamps are identical (bulk imports)
        filtered_books.sort(key=get_date_added_sort_key)
    elif sort_option == 'publication_date_desc':
        # Sort by publication date (newest first) - handle various date formats
        def get_pub_date(book):
            pub_date = book.get('published_date') if isinstance(book, dict) else getattr(book, 'published_date', None)
            if not pub_date:
                return ''
            # Convert to string for sorting (ISO format works well)
            if hasattr(pub_date, 'isoformat'):
                return pub_date.isoformat()
            return str(pub_date)
        filtered_books.sort(key=get_pub_date, reverse=True)
    elif sort_option == 'publication_date_asc':
        # Sort by publication date (oldest first)
        def get_pub_date(book):
            pub_date = book.get('published_date') if isinstance(book, dict) else getattr(book, 'published_date', None)
            if not pub_date:
                return ''
            if hasattr(pub_date, 'isoformat'):
                return pub_date.isoformat()
            return str(pub_date)
        filtered_books.sort(key=get_pub_date)
    elif sort_option == 'finish_date_desc':
        # Sort by finish date (most recently finished first; unfinished last)
        def get_finish_sort_desc(book):
            fd = _parse_finish_date(book.get('finish_date') if isinstance(book, dict) else getattr(book, 'finish_date', None))
            return (fd is not None, fd or datetime.min.date())
        filtered_books.sort(key=get_finish_sort_desc, reverse=True)
    elif sort_option == 'finish_date_asc':
        # Sort by finish date (oldest finished first; unfinished last)
        def get_finish_sort_asc(book):
            fd = _parse_finish_date(book.get('finish_date') if isinstance(book, dict) else getattr(book, 'finish_date', None))
            return (fd is None, fd or datetime.max.date())
        filtered_books.sort(key=get_finish_sort_asc)
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
    media_types = set()
    filter_record_set: set[tuple] = set()

    for book in all_books:
        # Handle categories
        book_categories = book.get('categories', []) if isinstance(book, dict) else getattr(book, 'categories', [])
        category_values: List[str] = []
        if book_categories:
            # book.categories is a list of Category objects, not a string
            for cat in book_categories:
                if isinstance(cat, dict):
                    name_val = cat.get('name', '')
                    categories.add(name_val)
                    category_values.append(name_val)
                elif hasattr(cat, 'name'):
                    categories.add(cat.name)
                    category_values.append(cat.name)
                else:
                    string_val = str(cat)
                    categories.add(string_val)
                    category_values.append(string_val)
        if not category_values:
            category_values.append('')
        
        # Handle publisher
        book_publisher = book.get('publisher') if isinstance(book, dict) else getattr(book, 'publisher', None)
        publisher_name = ''
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
        language_value = book_language or ''
        if book_language:
            languages.add(book_language)
        
        # Handle locations - they are now returned as strings (location names) from KuzuIntegrationService
        book_locations = book.get('locations', []) if isinstance(book, dict) else getattr(book, 'locations', [])
        location_values: List[str] = []
        if book_locations:
            for loc in book_locations:
                if isinstance(loc, str) and loc:
                    # Location is already a string (location name) and not empty
                    locations.add(loc)
                    location_values.append(loc)
                elif isinstance(loc, dict) and loc.get('name'):
                    locations.add(loc.get('name'))
                    location_values.append(loc.get('name'))
                elif hasattr(loc, 'name') and getattr(loc, 'name', None):
                    extracted = getattr(loc, 'name')
                    locations.add(extracted)
                    location_values.append(extracted)
                else:
                    string_val = str(loc)
                    locations.add(string_val)
                    location_values.append(string_val)
        if not location_values:
            location_values.append('')

        mt_value = _resolve_media_type_value(book)
        if mt_value:
            media_types.add(mt_value)
        else:
            mt_value = ''

        # Build a lightweight co-occurrence index so dropdown options can cascade on the client.
        for category_value in category_values:
            for location_value in location_values:
                filter_record_set.add((
                    category_value or '',
                    publisher_name or '',
                    language_value or '',
                    location_value or '',
                    mt_value or ''
                ))

    declared_media_types = {mt.value.lower() for mt in MediaType}
    all_media_type_values = sorted(
        declared_media_types.union(media_types),
        key=lambda val: val.replace('_', ' ') if isinstance(val, str) else ''
    )

    friendly_media_labels = {
        'physical': 'Physical Book',
        'ebook': 'E-book',
        'audiobook': 'Audiobook',
        'kindle': 'Kindle'
    }

    def _format_media_type_label(val: str) -> str:
        normalized = (val or '').strip().lower()
        if normalized in friendly_media_labels:
            return friendly_media_labels[normalized]
        base = normalized.replace('_', ' ')
        return base.title() if base else ''

    media_type_options = [
        {
            'value': val,
            'label': _format_media_type_label(val)
        }
        for val in all_media_type_values
    ]
    media_type_labels = {opt['value']: opt['label'] for opt in media_type_options}

    filter_records = [
        {
            'category': entry[0],
            'publisher': entry[1],
            'language': entry[2],
            'location': entry[3],
            'media_type': entry[4]
        }
        for entry in sorted(filter_record_set)
    ]

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

    # Bulk action option lists
    reading_status_options = [
        {
            'value': status.value,
            'label': _humanize_status(status.value)
        }
        for status in ReadingStatus
    ]

    location_options: List[Dict[str, Any]] = []
    try:
        from app.location_service import LocationService

        location_service = LocationService()
        all_locations = location_service.get_all_locations(active_only=True)
        for location in all_locations or []:
            if isinstance(location, dict):
                loc_id = (location.get('id') or '').strip()
                loc_name = (location.get('name') or '').strip()
                is_default = bool(location.get('is_default'))
            else:
                loc_id = (getattr(location, 'id', '') or '').strip()
                loc_name = (getattr(location, 'name', '') or '').strip()
                is_default = bool(getattr(location, 'is_default', False))

            if not loc_id or not loc_name:
                continue

            location_options.append({
                'id': loc_id,
                'name': loc_name,
                'is_default': is_default
            })

        if location_options:
            location_options.sort(key=lambda entry: (not entry.get('is_default', False), entry.get('name', '').lower()))
    except Exception as exc:  # pragma: no cover - defensive fallback
        current_app.logger.warning(f"Failed to load locations for bulk actions: {exc}")
        location_options = []

    category_options: List[Dict[str, str]] = []
    category_lookup: Dict[str, Dict[str, str]] = {}

    try:
        all_categories = book_service.list_all_categories_sync()
    except Exception as exc:  # pragma: no cover - defensive logging
        current_app.logger.warning(f"Failed to load category list for bulk actions: {exc}")
        all_categories = []

    for category in (all_categories or []):
        if isinstance(category, dict):
            raw_name = (category.get('name') or category.get('normalized_name') or '').strip()
            display_label = (category.get('label') or category.get('display_name') or raw_name)
        else:
            raw_name = (getattr(category, 'name', '') or '').strip()
            display_label = raw_name

        if not raw_name:
            continue

        key = raw_name.lower()
        if key in category_lookup:
            continue

        category_lookup[key] = {
            'name': raw_name,
            'label': (display_label.strip() if isinstance(display_label, str) and display_label.strip() else raw_name)
        }

    for existing_category in categories:
        name = (existing_category or '').strip()
        if not name:
            continue
        key = name.lower()
        if key not in category_lookup:
            category_lookup[key] = {
                'name': name,
                'label': name
            }

    if category_lookup:
        category_options = sorted(category_lookup.values(), key=lambda entry: entry.get('label', '').lower())

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
                'needs_review': getattr(b, 'needs_review', None) if not isinstance(b, dict) else b.get('needs_review'),
            }
            payload.append(bd)
        # ETag based on user, page/filter/sort, and version
        from app.utils.simple_cache import get_user_library_version
        version = get_user_library_version(str(current_user.id))
        etag = f"W/\"lib:{current_user.id}:{page}:{rows}:{cols}:{per_page}:{status_filter}:{category_filter}:{publisher_filter}:{language_filter}:{location_filter}:{media_type_filter}:{finished_after_raw}:{finished_before_raw}:{search_query}:{sort_option}:{int(needs_review_only)}:v{version}\""
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
        resp.headers['Cache-Control'] = 'private, max-age=0, must-revalidate'
        return resp

    # ETag for HTML response too
    from app.utils.simple_cache import get_user_library_version
    _version = get_user_library_version(str(current_user.id))
    _html_etag = f"W/\"libhtml:{current_user.id}:{page}:{rows}:{cols}:{per_page}:{status_filter}:{category_filter}:{publisher_filter}:{language_filter}:{location_filter}:{media_type_filter}:{finished_after_raw}:{finished_before_raw}:{search_query}:{sort_option}:{int(needs_review_only)}:v{_version}\""
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
        media_types=media_type_options,
        current_status_filter=status_filter,
        current_category=category_filter,
        current_publisher=publisher_filter,
        current_language=language_filter,
        current_location=location_filter,
        current_media_type=media_type_filter,
        current_finished_after=finished_after_raw,
        current_finished_before=finished_before_raw,
        current_search=search_query,
        current_sort=sort_option,
        current_needs_review=needs_review_only,
        media_type_labels=media_type_labels,
        users=users,
        reading_status_options=reading_status_options,
        location_options=location_options,
        category_options=category_options,
        filter_records=filter_records
    ))
    resp.headers['ETag'] = _html_etag
    resp.headers['Cache-Control'] = 'private, max-age=0, must-revalidate'
    # Hint the browser to warm the next page JSON in the background
    try:
        if page < total_pages:
            from urllib.parse import urlencode
            # Preserve existing args but override page and add format=json
            # request.args may be a MultiDict; use flat values for cleanliness
            flat_params = {k: request.args.get(k) for k in request.args.keys()}
            if raw_search_query is not None:
                if search_query:
                    flat_params['search'] = search_query
                else:
                    flat_params.pop('search', None)
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

