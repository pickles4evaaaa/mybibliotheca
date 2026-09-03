"""Book title/author search route implementation.

book_routes keeps the public route registration and delegates to this module.
The handler body is unchanged so response and provider behavior remain stable.
"""

from __future__ import annotations

from flask import current_app, jsonify, request
from flask_login import login_required


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
        from app.utils.book_utils import iter_title_search_variants
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

        title_variants = iter_title_search_variants(title) if title else ['']

        import concurrent.futures
        import requests as _req
        from app.utils.google_books import google_books_url

        def _fetch_openlibrary():
            """Fetch search results from OpenLibrary with fast-path strategy.
            
            Strategy: Only make the initial search request; skip nested edition/book
            detail calls which cause cascading timeouts. The search endpoint already
            returns ISBN, author, and title info which is sufficient for selection UI.
            """
            try:
                for t_variant in title_variants:
                    ol_query = ' '.join([q for q in [t_variant or author, author if (t_variant and author) else None] if q])
                    if not ol_query:
                        continue
                    url = f"https://openlibrary.org/search.json?q={ol_query}&limit=8"
                    r = _req.get(url, timeout=(2.5, 4.5))  # (connect, read) tuple for faster fail
                    r.raise_for_status()
                    data = r.json()
                    docs = data.get('docs', [])[:8]
                    if not docs:
                        continue
                    out = []
                    for doc in docs:
                        doc_title = doc.get('title','')
                        doc_authors = doc.get('author_name', []) if isinstance(doc.get('author_name'), list) else [doc.get('author_name','')]
                        # Use ISBNs directly from search results - skip nested API calls
                        doc_isbn = doc.get('isbn', []) or []
                        edition_keys = doc.get('edition_key') if isinstance(doc.get('edition_key'), list) else ([doc.get('edition_key')] if doc.get('edition_key') else [])
                        # FAST PATH: Skip nested edition/book lookups to avoid timeout cascades.
                        # The search result already contains ISBN lists in most cases.
                        best_isbn = next((i for i in doc_isbn if isinstance(i, str) and len(i)==13), (doc_isbn[0] if doc_isbn else None))
                        cleaned_isbn_list = []
                        for candidate in doc_isbn:
                            if not candidate:
                                continue
                            digits = re.sub(r"[^0-9Xx]", "", str(candidate))
                            if digits and digits not in cleaned_isbn_list:
                                cleaned_isbn_list.append(digits)
                        isbn13_candidate = next((digits for digits in cleaned_isbn_list if len(digits) == 13), None)
                        isbn10_candidate = next((digits for digits in cleaned_isbn_list if len(digits) == 10), None)
                        subjects_facet = doc.get('subject_facet') if isinstance(doc.get('subject_facet'), list) else []
                        subjects = doc.get('subject') if isinstance(doc.get('subject'), list) else []
                        combined_subjects = [s for s in (subjects_facet or subjects) if isinstance(s, str)]
                        raw_category_paths = [s for s in (subjects if subjects else subjects_facet) if isinstance(s, str)]
                        ol_description = doc.get('first_sentence')
                        if isinstance(ol_description, dict):
                            ol_description = ol_description.get('value')
                        if isinstance(ol_description, list):
                            ol_description = ' '.join([str(item) for item in ol_description])
                        # Use publisher from search results directly
                        publisher_list = doc.get('publisher', [])
                        publisher_str = ', '.join(publisher_list) if isinstance(publisher_list, list) and publisher_list else (str(publisher_list) if isinstance(publisher_list, str) else '')
                        published_date = str(doc.get('first_publish_year','')) if doc.get('first_publish_year') else ''
                        language_code = None
                        if doc.get('language'):
                            lang_list = doc.get('language')
                            language_code = lang_list[0] if isinstance(lang_list, list) and lang_list else (str(lang_list) if lang_list else None)
                        res = {
                            'title': doc_title,
                            'subtitle': doc.get('subtitle') or '',
                            'authors': ', '.join(doc_authors) if doc_authors else '',
                            'authors_list': doc_authors,
                            'isbn': best_isbn,
                            'isbn_list': cleaned_isbn_list or doc_isbn,
                            'isbn13': isbn13_candidate,
                            'isbn10': isbn10_candidate,
                            'publisher': publisher_str,
                            'published_date': published_date,
                            'page_count': doc.get('number_of_pages_median'),
                            'language': language_code or 'en',
                            'openlibrary_id': doc.get('key','').replace('/works/','') if doc.get('key') else None,
                            'cover_id': doc.get('cover_i'),
                            'description': ol_description if isinstance(ol_description, str) else '',
                            'categories': combined_subjects,
                            'raw_category_paths': raw_category_paths,
                            'edition_key': edition_keys[0] if edition_keys else None,
                            'source': 'OpenLibrary'
                        }
                        # Generate cover URL from cover_id if available (fast, no API call)
                        if res.get('cover_id'):
                            res['cover_url'] = f"https://covers.openlibrary.org/b/id/{res['cover_id']}-M.jpg"
                            res['cover_source'] = 'openlibrary'
                        out.append(res)
                    if out:
                        return out
                return []
            except Exception as e:
                current_app.logger.debug(f"[SEARCH] OpenLibrary failed: {e}")
                return []

        def _fetch_google():
            try:
                for t_variant in title_variants:
                    gb_parts = []
                    if t_variant:
                        gb_parts.append(f'intitle:"{t_variant}"')
                    if author:
                        gb_parts.append(f'inauthor:"{author}"')
                    gb_query = ' '.join(gb_parts)
                    if not gb_query:
                        continue
                    r = _req.get(google_books_url(q=gb_query, maxResults=8), timeout=(2.5, 3.5))  # (connect, read) tuple for faster fail
                    r.raise_for_status()
                    data = r.json()
                    items = data.get('items', [])[:8]
                    if not items:
                        continue
                    out = []
                    for item in items:
                        info = item.get('volumeInfo', {})
                        gb_title = info.get('title','')
                        gb_authors = info.get('authors', []) or []
                        identifiers = info.get('industryIdentifiers', []) or []
                        gb_isbn = None
                        isbn_candidates = []
                        for ident in identifiers:
                            t = ident.get('type')
                            if t == 'ISBN_13':
                                gb_isbn = ident.get('identifier'); break
                            if t == 'ISBN_10' and not gb_isbn:
                                gb_isbn = ident.get('identifier')
                            if ident.get('identifier'):
                                isbn_candidates.append(ident.get('identifier'))
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
                            'isbn_list': isbn_candidates,
                            'publisher': info.get('publisher',''),
                            'published_date': info.get('publishedDate',''),
                            'description': info.get('description',''),
                            'page_count': info.get('pageCount'),
                            'language': info.get('language','en'),
                            'average_rating': info.get('averageRating'),
                            'rating_count': info.get('ratingsCount'),
                            'categories': info.get('categories', []),
                            'raw_category_paths': info.get('categories', []),
                            'google_books_id': item.get('id'),
                            'cover_url': gb_cover,
                            'source': 'Google Books'
                        })
                    if out:
                        return out
                return []
            except Exception as e:
                current_app.logger.debug(f"[SEARCH] Google Books failed: {e}")
                return []

        provider_timeout = float(current_app.config.get('BOOK_SEARCH_PROVIDER_TIMEOUT', 7.0)) if current_app else 7.0
        timed_out_providers: list[str] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
            fut_ol = ex.submit(_fetch_openlibrary)
            fut_gb = ex.submit(_fetch_google)
            try:
                ol_results = fut_ol.result(timeout=provider_timeout)
            except concurrent.futures.TimeoutError:
                timed_out_providers.append('openlibrary')
                current_app.logger.warning(f"[SEARCH] OpenLibrary timed out after {provider_timeout:.1f}s")
                fut_ol.cancel()
                ol_results = []
            except Exception as e:
                current_app.logger.debug(f"[SEARCH] OpenLibrary exception: {e}")
                ol_results = []
            results.extend(ol_results)
            # Always fetch Google Books results too for better coverage
            gb_results = []
            try:
                gb_results = fut_gb.result(timeout=provider_timeout)
            except concurrent.futures.TimeoutError:
                timed_out_providers.append('google')
                current_app.logger.warning(f"[SEARCH] Google Books timed out after {provider_timeout:.1f}s")
                fut_gb.cancel()
            except Exception as e:
                current_app.logger.debug(f"[SEARCH] Google Books exception: {e}")
                gb_results = []
            existing_keys = {(r.get('title','').lower(), r.get('authors','').lower()) for r in results}
            for r in gb_results:
                key = (r.get('title','').lower(), r.get('authors','').lower())
                if key not in existing_keys:
                    results.append(r)
                    existing_keys.add(key)

        # Sort results by similarity score (highest first)
        from difflib import SequenceMatcher
        
        def _normalize(s: str) -> str:
            """Normalize string for comparison: lowercase, strip punctuation, collapse spaces."""
            import re
            s = (s or '').lower().strip()
            s = re.sub(r'[^\w\s]', ' ', s)  # Replace punctuation with spaces
            return ' '.join(s.split())  # Collapse multiple spaces
        
        def _similarity_score(r: dict) -> float:
            """Calculate similarity between result and search criteria. Higher = better match."""
            r_title = r.get('title') or ''
            r_authors = r.get('authors') or ''
            
            search_title_norm = _normalize(title) if title else ''
            search_author_norm = _normalize(author) if author else ''
            r_title_norm = _normalize(r_title)
            r_authors_norm = _normalize(r_authors)
            
            score = 0.0
            
            if search_title_norm:
                # Title similarity (weighted heavily)
                title_ratio = SequenceMatcher(None, search_title_norm, r_title_norm).ratio()
                score += title_ratio * 0.7
                
                # Bonus for exact normalized match
                if search_title_norm == r_title_norm:
                    score += 0.15
                # Bonus if result title starts with search title
                elif r_title_norm.startswith(search_title_norm):
                    score += 0.1
            
            if search_author_norm:
                # Author similarity
                author_ratio = SequenceMatcher(None, search_author_norm, r_authors_norm).ratio()
                score += author_ratio * 0.3
                
                # Bonus for exact author match
                if search_author_norm == r_authors_norm or search_author_norm in r_authors_norm:
                    score += 0.1
            
            return score
        
        # Sort by similarity score descending (negate for descending order)
        results.sort(key=lambda r: -_similarity_score(r))

        def _format_provider_label(raw: str) -> str:
            mapping = {'openlibrary': 'OpenLibrary', 'google': 'Google Books', 'googlebooks': 'Google Books'}
            return mapping.get(raw.lower(), raw.title())

        message = f"Found {len(results)} options. Select one to apply." if results else 'No books found matching your search criteria. Try a different title or check spelling.'
        if results and timed_out_providers:
            formatted = ', '.join(_format_provider_label(name) for name in timed_out_providers)
            message += f" (partial results: {formatted} timed out)"

        payload = {
            'success': bool(results),
            'results': results,
            'timed_out_providers': timed_out_providers,
            'message': message
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

