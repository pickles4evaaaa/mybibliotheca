"""Legacy title and ISBN search helpers.

These functions retain the original provider behavior while keeping the
compatibility module app.utils.book_utils focused on metadata and cover
helpers.
"""

from __future__ import annotations

import os
import requests

from .book_utils import (
    _google_title_cache_get,
    _google_title_cache_set,
    _openlibrary_cover_exists,
    fetch_book_data,
    iter_title_search_variants,
    normalize_cover_url,
    select_highest_google_image,
    upgrade_google_cover_url,
)


def normalize_goodreads_value(value, field_type='text'):
    """
    Normalize values from Goodreads CSV exports that use Excel text formatting.
    Goodreads exports often have values like ="123456789" or ="" to force text formatting.
    """
    if not value or not isinstance(value, str):
        return value.strip() if value else ''
    
    # Remove Excel text formatting: ="value" -> value
    if value.startswith('="') and value.endswith('"'):
        value = value[2:-1]  # Remove =" prefix and " suffix
    elif value.startswith('=') and value.endswith('"'):
        value = value[1:-1]  # Remove = prefix and " suffix  
    elif value == '=""':
        value = ''  # Empty quoted value
    
    # Handle standard quoted values for backwards compatibility
    elif value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    
    # Special handling for ISBN fields
    if field_type == 'isbn' and value:
        # Remove any remaining quotes or formatting for ISBNs
        value = value.replace('"', '').replace("'", "").replace('-', '').replace(' ', '')
        # Only return if it looks like a valid ISBN (10 or 13 digits)
        if value.isdigit() and len(value) in [10, 13]:
            return value
        elif len(value) >= 10:  # Be more lenient for partial matches
            return value
    
    return value.strip() if value else ''

def search_multiple_books_by_title_author(title, author=None, limit=10):
    """Search for multiple books from both OpenLibrary and Google Books APIs by title and optionally author."""
    if not title:
        print(f"[MULTI_API] No title provided for book search")
        return []
    
    all_results = []
    
    # Google Books is preferred; OpenLibrary (slower, rate limited) only fills in when Google
    # is paused/over quota or returned nothing.
    print(f"[MULTI_API] Searching Google Books for: '{title}' by '{author}'")
    try:
        gb_results = search_google_books_by_title_author(title, author, limit)
        if gb_results:
            all_results.extend(gb_results)
            print(f"[MULTI_API] Google Books returned {len(gb_results)} results")
    except Exception as e:
        print(f"[MULTI_API] Google Books search failed: {e}")

    if not all_results:
        print(f"[MULTI_API] Falling back to OpenLibrary for: '{title}' by '{author}'")
        try:
            ol_results = _search_openlibrary_multiple(title, author, limit)
            if ol_results:
                all_results.extend(ol_results)
                print(f"[MULTI_API] OpenLibrary returned {len(ol_results)} results")
        except Exception as e:
            print(f"[MULTI_API] OpenLibrary search failed: {e}")

    # Deduplicate results by title similarity
    unique_results = []
    seen_titles = set()
    
    for result in all_results:
        result_title = result.get('title', '').lower().strip()
        
        # Skip if we've seen a very similar title
        is_duplicate = False
        for seen_title in seen_titles:
            # Consider titles duplicates if they're very similar
            if (result_title in seen_title or seen_title in result_title) and len(result_title) > 3:
                is_duplicate = True
                break
        
        if not is_duplicate:
            unique_results.append(result)
            seen_titles.add(result_title)
    
    # Sort results by relevance (prefer exact matches, then partial matches)
    def calculate_relevance_score(result):
        score = 0
        result_title = result.get('title', '').lower().strip()
        result_authors = result.get('authors_list', [])
        
        # Exact title match
        if result_title == title.lower().strip():
            score += 100
        # Partial title match
        elif title.lower() in result_title or result_title in title.lower():
            score += 50
        
        # Author match
        if author and result_authors:
            for result_author in result_authors:
                if author.lower() in result_author.lower():
                    score += 30
        
        # Prefer results with ISBNs
        if result.get('isbn13') or result.get('isbn10'):
            score += 10
        
        # Slight preference for Google Books (usually more complete metadata)
        if result.get('source') == 'Google Books':
            score += 5
        
        return score
    
    unique_results.sort(key=calculate_relevance_score, reverse=True)
    
    # Limit to requested number of results
    final_results = unique_results[:limit]
    
    print(f"[MULTI_API] Returning {len(final_results)} unique results from {len(all_results)} total results")
    return final_results


def _search_openlibrary_multiple(title, author=None, limit=10):
    """Internal function to search OpenLibrary (extracted from original function)."""
    title_str = '' if title is None else str(title).strip()
    title_variants = iter_title_search_variants(title_str)
    if not title_variants:
        print(f"[OPENLIBRARY] No title provided for book search")
        return []

    for t_variant in title_variants:
        if not t_variant:
            continue

        query_parts = [t_variant]
        if author:
            query_parts.append(author)

        query = ' '.join(query_parts)
        url = f"https://openlibrary.org/search.json?q={query}&limit={limit}"

        print(f"[OPENLIBRARY] Searching for multiple books: title='{t_variant}', author='{author}' at {url}")

        try:
            response = requests.get(url, timeout=15)
            print(f"[OPENLIBRARY] Multiple book search response status: {response.status_code}")

            if response.status_code == 404:
                print(f"[OPENLIBRARY] OpenLibrary API returned 404 for query: {query}")
                continue
            elif response.status_code != 200:
                print(f"[OPENLIBRARY] OpenLibrary API returned {response.status_code}")
                continue

            data = response.json()

            docs = data.get('docs', [])
            if not docs:
                continue

            print(f"[OPENLIBRARY] Found {len(docs)} book search results")

            scored_matches = []

            for i, doc in enumerate(docs):
                doc_title = doc.get('title', '')
                doc_authors = doc.get('author_name', []) if isinstance(doc.get('author_name'), list) else [doc.get('author_name', '')]
                doc_isbn = doc.get('isbn', [])

                best_isbn = None
                if doc_isbn:
                    for isbn in doc_isbn:
                        if len(isbn) == 13:
                            best_isbn = isbn
                            break
                    if not best_isbn and doc_isbn:
                        best_isbn = doc_isbn[0]

                print(f"[OPENLIBRARY] Result {i}: title='{doc_title}', authors={doc_authors}, isbn={best_isbn}")

                score = 0

                if t_variant.lower() in doc_title.lower() or doc_title.lower() in t_variant.lower():
                    score += 50
                if t_variant.lower().strip() == doc_title.lower().strip():
                    score += 50

                if author:
                    for doc_author in doc_authors:
                        if doc_author and author.lower() in doc_author.lower():
                            score += 30
                        if doc_author and author.lower().strip() == doc_author.lower().strip():
                            score += 20

                if best_isbn:
                    score += 10

                if doc.get('first_publish_year'):
                    try:
                        year = int(doc.get('first_publish_year'))
                        if year > 1950:
                            score += min((year - 1950) // 10, 5)
                    except Exception:
                        pass

                result = {
                    'title': doc_title,
                    'author': ', '.join(doc_authors) if doc_authors else '',
                    'authors_list': doc_authors,
                    'isbn': best_isbn,
                    'isbn13': best_isbn if best_isbn and len(best_isbn) == 13 else '',
                    'isbn10': best_isbn if best_isbn and len(best_isbn) == 10 else '',
                    'publisher': ', '.join(doc.get('publisher', [])) if isinstance(doc.get('publisher'), list) else doc.get('publisher', ''),
                    'published_date': str(doc.get('first_publish_year', '')) if doc.get('first_publish_year') else '',
                    'page_count': doc.get('number_of_pages_median'),
                    'cover': None,
                    'cover_url': None,
                    'description': '',
                    'language': doc.get('language', [''])[0] if doc.get('language') else 'en',
                    'openlibrary_id': doc.get('key', '').replace('/works/', '') if doc.get('key') else None,
                    'categories': [],
                    'score': score
                }

                cover_id = doc.get('cover_i')
                if cover_id:
                    # Verified in parallel below; sequential checks cost ~0.2-0.6s each.
                    result['_pending_cover'] = normalize_cover_url(f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg")

                scored_matches.append(result)

            pending = [r for r in scored_matches if r.get('_pending_cover')]
            if pending:
                from concurrent.futures import ThreadPoolExecutor

                def _check(r):
                    try:
                        return bool(_openlibrary_cover_exists(r['_pending_cover']))
                    except Exception:
                        return False

                with ThreadPoolExecutor(max_workers=min(8, len(pending))) as pool:
                    verdicts = list(pool.map(_check, pending))
                for r, ok in zip(pending, verdicts):
                    if ok:
                        r['cover'] = r['cover_url'] = r['_pending_cover']
            for r in scored_matches:
                r.pop('_pending_cover', None)

            scored_matches.sort(key=lambda x: x['score'], reverse=True)
            print(f"[OPENLIBRARY] Returning {len(scored_matches)} book results")
            return scored_matches

        except Exception as e:
            print(f"[OPENLIBRARY] Failed to search for multiple books '{t_variant}' by '{author}': {e}")
            continue

    return []


def _openlibrary_match_sort_key(match, title_variant):
    """Stable ordering for scored OpenLibrary matches.

    Higher score first, then exact title match, then more editions, then the work
    key. The key guarantees the same query always yields the same order even when
    OpenLibrary returns tied results in a different order between requests.
    """
    doc = match.get('doc') or {}
    exact_title = str(match.get('title') or '').lower().strip() == str(title_variant or '').lower().strip()
    try:
        editions = int(doc.get('edition_count') or 0)
    except Exception:
        editions = 0
    return (-match.get('score', 0), 0 if exact_title else 1, -editions, str(doc.get('key') or ''))


def search_openlibrary_cover_candidates(title, author=None, limit=6):
    """Return up to ``limit`` distinct OpenLibrary candidates that have a cover.

    Uses a single search request (the results are already in the response) and a
    deterministic order, so repeated searches give the same list.
    """
    title_str = '' if title is None else str(title).strip()
    title_variants = iter_title_search_variants(title_str)
    results = []
    seen_cover_ids = set()
    for t_variant in title_variants:
        if not t_variant:
            continue
        query = ' '.join([t_variant] + ([author] if author else []))
        try:
            response = requests.get(
                "https://openlibrary.org/search.json",
                params={'q': query, 'limit': 20},
                timeout=15,
            )
            if response.status_code != 200:
                continue
            docs = response.json().get('docs', []) or []
        except Exception as e:
            print(f"[OPENLIBRARY] Cover candidate search failed for '{t_variant}': {e}")
            continue

        scored = []
        for doc in docs:
            if not doc.get('cover_i'):
                continue
            doc_title = doc.get('title', '') or ''
            names = doc.get('author_name')
            doc_authors = names if isinstance(names, list) else ([names] if names else [])
            score = 0
            tl, dl = t_variant.lower(), doc_title.lower()
            if tl in dl or dl in tl:
                score += 50
            if tl.strip() == dl.strip():
                score += 50
            if author:
                for a in doc_authors:
                    if a and author.lower() in a.lower():
                        score += 30
                    if a and author.lower().strip() == a.lower().strip():
                        score += 20
            scored.append({'doc': doc, 'score': score, 'title': doc_title, 'authors': doc_authors})

        scored.sort(key=lambda m: _openlibrary_match_sort_key(m, t_variant))
        for m in scored:
            doc = m['doc']
            cover_id = doc.get('cover_i')
            if cover_id in seen_cover_ids:
                continue
            seen_cover_ids.add(cover_id)
            url = normalize_cover_url(f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg")
            if not url:
                continue
            results.append({
                'title': m['title'],
                'authors_list': m['authors'],
                'published_date': str(doc.get('first_publish_year', '')) if doc.get('first_publish_year') else '',
                'cover_url': url,
            })
            if len(results) >= limit:
                return results
        if results:
            break
    return results


def search_book_by_title_author(title, author=None):
    """Search for books on OpenLibrary by title and optionally author, return the best match."""
    title_str = '' if title is None else str(title).strip()
    title_variants = iter_title_search_variants(title_str)
    if not title_variants:
        print(f"[OPENLIBRARY] No title provided for book search")
        return None

    best_fallback_result = None
    best_fallback_score = None

    for t_variant in title_variants:
        if not t_variant:
            continue

        query_parts = [t_variant]
        if author:
            query_parts.append(author)

        query = ' '.join(query_parts)
        url = f"https://openlibrary.org/search.json?q={query}&limit=10"

        print(f"[OPENLIBRARY] Searching for book: title='{t_variant}', author='{author}' at {url}")

        try:
            response = requests.get(url, timeout=15)
            print(f"[OPENLIBRARY] Book search response status: {response.status_code}")

            if response.status_code == 404:
                print(f"[OPENLIBRARY] OpenLibrary API returned 404 for query: {query}")
                continue
            elif response.status_code != 200:
                print(f"[OPENLIBRARY] OpenLibrary API returned {response.status_code}")
                continue

            data = response.json()

            docs = data.get('docs', [])
            if not docs:
                continue

            print(f"[OPENLIBRARY] Found {len(docs)} book search results")

            scored_matches = []

            for i, doc in enumerate(docs):
                doc_title = doc.get('title', '')
                doc_authors = doc.get('author_name', []) if isinstance(doc.get('author_name'), list) else [doc.get('author_name', '')]
                doc_isbn = doc.get('isbn', [])

                best_isbn = None
                if doc_isbn:
                    for isbn in doc_isbn:
                        if len(isbn) == 13:
                            best_isbn = isbn
                            break
                    if not best_isbn and doc_isbn:
                        best_isbn = doc_isbn[0]

                print(f"[OPENLIBRARY] Result {i}: title='{doc_title}', authors={doc_authors}, isbn={best_isbn}")

                score = 0

                if t_variant.lower() in doc_title.lower() or doc_title.lower() in t_variant.lower():
                    score += 50
                if t_variant.lower().strip() == doc_title.lower().strip():
                    score += 50

                if author:
                    for doc_author in doc_authors:
                        if doc_author and author.lower() in doc_author.lower():
                            score += 30
                        if doc_author and author.lower().strip() == doc_author.lower().strip():
                            score += 20

                if best_isbn:
                    score += 10

                if doc.get('first_publish_year'):
                    try:
                        year = int(doc.get('first_publish_year'))
                        if year > 1950:
                            score += min((year - 1950) // 10, 5)
                    except Exception:
                        pass

                scored_matches.append({
                    'doc': doc,
                    'score': score,
                    'title': doc_title,
                    'authors': doc_authors,
                    'isbn': best_isbn
                })

            scored_matches.sort(key=lambda x: _openlibrary_match_sort_key(x, t_variant))

            if scored_matches:
                # Prefer candidates that can yield full metadata (ISBN) or at least a cover.
                for candidate in scored_matches[:3]:
                    print(f"[OPENLIBRARY] Best match: '{candidate['title']}' by {candidate['authors']} (score: {candidate['score']}, ISBN: {candidate['isbn']})")

                    if candidate['isbn']:
                        print(f"[OPENLIBRARY] Fetching full data using ISBN: {candidate['isbn']}")
                        full_data = fetch_book_data(candidate['isbn'])
                        if full_data:
                            return full_data

                    doc = candidate['doc']
                    cover_id = doc.get('cover_i')
                    if cover_id:
                        result = {
                            'title': candidate['title'],
                            'author': ', '.join(candidate['authors']) if candidate['authors'] else '',
                            'authors_list': candidate['authors'],
                            'isbn': candidate['isbn'],
                            'publisher': ', '.join(doc.get('publisher', [])) if isinstance(doc.get('publisher'), list) else doc.get('publisher', ''),
                            'published_date': str(doc.get('first_publish_year', '')) if doc.get('first_publish_year') else '',
                            'page_count': doc.get('number_of_pages_median'),
                            'cover': None,
                            'description': '',
                            'language': doc.get('language', [''])[0] if doc.get('language') else 'en',
                            'openlibrary_id': doc.get('key', '').replace('/works/', '') if doc.get('key') else None
                        }
                        cover_url = normalize_cover_url(f"https://covers.openlibrary.org/b/id/{cover_id}-L.jpg")
                        try:
                            if cover_url and _openlibrary_cover_exists(cover_url):
                                result['cover'] = cover_url
                                result['cover_url'] = cover_url
                                print(f"[OPENLIBRARY] Returning book data: {result}")
                                return result
                        except Exception:
                            pass

                # Keep the best result as a fallback, but continue to next title variant.
                best_match = scored_matches[0]
                if best_fallback_result is None or (best_fallback_score is None) or (best_match.get('score', 0) > best_fallback_score):
                    doc = best_match['doc']
                    best_fallback_score = best_match.get('score', 0)
                    best_fallback_result = {
                        'title': best_match['title'],
                        'author': ', '.join(best_match['authors']) if best_match['authors'] else '',
                        'authors_list': best_match['authors'],
                        'isbn': best_match['isbn'],
                        'publisher': ', '.join(doc.get('publisher', [])) if isinstance(doc.get('publisher'), list) else doc.get('publisher', ''),
                        'published_date': str(doc.get('first_publish_year', '')) if doc.get('first_publish_year') else '',
                        'page_count': doc.get('number_of_pages_median'),
                        'cover': None,
                        'description': '',
                        'language': doc.get('language', [''])[0] if doc.get('language') else 'en',
                        'openlibrary_id': doc.get('key', '').replace('/works/', '') if doc.get('key') else None
                    }

        except Exception as e:
            print(f"[OPENLIBRARY] Failed to search for book '{t_variant}' by '{author}': {e}")
            continue

    return best_fallback_result


# Google Books quota handling lives in app.utils.api_limits (shared across all gunicorn
# workers and persisted): it counts daily requests, honours GOOGLE_BOOKS_DAILY_LIMIT, and
# pauses Google calls until the quota resets after a quota 429. Retrying such a 429 with
# sleeps only makes lookups slower, so we never do that.
def _google_quota_blocked() -> bool:
    try:
        from app.utils.api_limits import is_blocked, GOOGLE
        return is_blocked(GOOGLE)
    except Exception:
        return False


def _google_note_rate_limited(response) -> None:
    # The transport hook in api_limits already recorded the 429 and set the pause.
    return None


def search_google_books_by_title_author(title, author=None, limit=10):
    """Search Google Books API by title and optionally author."""
    import os as _os
    _VERBOSE = (
        (_os.getenv('VERBOSE') or 'false').lower() == 'true'
        or (_os.getenv('IMPORT_VERBOSE') or 'false').lower() == 'true'
        or (_os.getenv('COVER_VERBOSE') or 'false').lower() == 'true'
    )
    title_str = '' if title is None else str(title).strip()
    title_variants = iter_title_search_variants(title_str)
    if not title_variants:
        if _VERBOSE:
            print(f"[GOOGLE_BOOKS] No title provided for book search")
        return []

    if _google_quota_blocked():
        return []

    cached = _google_title_cache_get(title_variants[0], author)
    if cached is not None:
        return cached[:limit]

    # Always fetch more than the desired limit because Google can return "stub" items
    # (e.g. empty titles) early in the list; we filter and then truncate.
    try:
        fetch_n = min(max(int(limit) * 5, 10), 40)
    except Exception:
        fetch_n = 10

    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; MyBibliotheca/1.0)',
        'Accept': 'application/json',
    }

    _upgrade = upgrade_google_cover_url
    for t_variant in title_variants:
        safe_title = str(t_variant).replace('"', '')
        query_parts = [f'intitle:"{safe_title}"']
        if author:
            safe_author = str(author).replace('"', '')
            query_parts.append(f'inauthor:"{safe_author}"')
        query = '+'.join(query_parts)
        url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults={fetch_n}"

        if _VERBOSE:
            print(f"[GOOGLE_BOOKS] Searching for multiple books: title='{t_variant}', author='{author}' at {url}")

        try:
            response = None
            for attempt in range(2):
                try:
                    response = requests.get(url, timeout=10, headers=headers)
                except Exception:
                    response = None
                code = getattr(response, 'status_code', 0) if response is not None else 0
                if code == 429:
                    # Retrying a rate limit/quota response never helps; back off instead.
                    _google_note_rate_limited(response)
                    return []
                if code in (500, 502, 503, 504) or code == 0:
                    try:
                        import time as _t
                        _t.sleep(0.5 * (attempt + 1))
                    except Exception:
                        pass
                    continue
                break

            if response is None:
                continue
            if response.status_code != 200:
                if _VERBOSE:
                    print(f"[GOOGLE_BOOKS] Google Books API returned {response.status_code}")
                continue

            data = response.json()
            items = data.get('items', [])
            if not items:
                continue

            results = []
            for i, item in enumerate(items):
                try:
                    volume_info = item.get('volumeInfo', {})

                    book_title = volume_info.get('title', '')
                    book_authors = volume_info.get('authors', [])
                    book_description = volume_info.get('description', '')
                    book_publisher = volume_info.get('publisher', '')
                    book_published_date = volume_info.get('publishedDate', '')
                    book_page_count = volume_info.get('pageCount', 0)
                    book_language = volume_info.get('language', 'en')
                    book_categories = volume_info.get('categories', [])

                    isbn13 = None
                    isbn10 = None
                    industry_identifiers = volume_info.get('industryIdentifiers', [])
                    for identifier in industry_identifiers:
                        if identifier.get('type') == 'ISBN_13':
                            isbn13 = identifier.get('identifier')
                        elif identifier.get('type') == 'ISBN_10':
                            isbn10 = identifier.get('identifier')

                    image_links = volume_info.get('imageLinks', {}) or {}
                    raw_cover = select_highest_google_image(image_links)
                    cover_url = _upgrade(raw_cover) if raw_cover else None

                    result = {
                        'title': book_title,
                        'author': ', '.join(book_authors) if book_authors else '',
                        'authors_list': book_authors,
                        'description': book_description,
                        'publisher': book_publisher,
                        'published_date': book_published_date,
                        'page_count': book_page_count,
                        'isbn13': isbn13,
                        'isbn10': isbn10,
                        'isbn': isbn13 or isbn10,
                        'cover_url': cover_url,
                        'cover': cover_url,
                        'language': book_language,
                        'categories': book_categories,
                        'google_books_id': item.get('id'),
                        'source': 'Google Books'
                    }

                    if result['title']:
                        results.append(result)
                    if len(results) >= limit:
                        break
                except Exception as item_error:
                    if _VERBOSE:
                        print(f"[GOOGLE_BOOKS] Error processing item {i}: {item_error}")
                    continue

            if results:
                _google_title_cache_set(title_variants[0], author, results)
                return results[:limit]

        except Exception as e:
            if _VERBOSE:
                print(f"[GOOGLE_BOOKS] Failed variant search for '{t_variant}' by '{author}': {e}")
            continue

    return []


# Compatibility aliases remain available from app.utils.book_utils.
from .legacy_book_search import (  # noqa: E402,F401
    _search_openlibrary_multiple,
    normalize_goodreads_value,
    search_book_by_title_author,
    search_google_books_by_title_author,
    search_multiple_books_by_title_author,
)

