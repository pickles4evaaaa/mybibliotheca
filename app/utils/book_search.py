"""
Enhanced book search functionality that queries multiple APIs and ranks results.

This module provides title-based search across Google Books API and OpenLibrary,
with intelligent ranking and deduplication of results.
"""

import requests
from difflib import SequenceMatcher
from typing import List, Dict, Optional, Any
import re
from urllib.parse import quote_plus
import time
import os as _os_for_verbose
from collections import OrderedDict
import threading
import copy

# Quiet logging by default; enable with VERBOSE=true or IMPORT_VERBOSE=true
_IMPORT_VERBOSE = (
    (_os_for_verbose.getenv('VERBOSE') or 'false').lower() == 'true'
    or (_os_for_verbose.getenv('IMPORT_VERBOSE') or 'false').lower() == 'true'
)

def _dprint(*args, **kwargs):
    if _IMPORT_VERBOSE:
        __builtins__.print(*args, **kwargs)

# Shadow print in this module to respect verbosity toggle
print = _dprint


_SEARCH_CACHE_TTL = int((_os_for_verbose.getenv('BOOK_SEARCH_CACHE_TTL') or '300'))
_SEARCH_CACHE_MAX = int((_os_for_verbose.getenv('BOOK_SEARCH_CACHE_MAX') or '128'))
_SEARCH_CACHE: "OrderedDict[tuple, tuple[float, List[Dict[str, Any]]]]" = OrderedDict()
_SEARCH_CACHE_LOCK = threading.RLock()

_GOOGLE_CONNECT_TIMEOUT = float((_os_for_verbose.getenv('BOOK_SEARCH_GOOGLE_CONNECT_TIMEOUT') or '2.5'))
_GOOGLE_READ_TIMEOUT = float((_os_for_verbose.getenv('BOOK_SEARCH_GOOGLE_READ_TIMEOUT') or '3.5'))
_OPENLIBRARY_CONNECT_TIMEOUT = float((_os_for_verbose.getenv('BOOK_SEARCH_OPENLIBRARY_CONNECT_TIMEOUT') or '2.5'))
_OPENLIBRARY_READ_TIMEOUT = float((_os_for_verbose.getenv('BOOK_SEARCH_OPENLIBRARY_READ_TIMEOUT') or '4.5'))
_GOOGLE_RESULT_TIMEOUT = float((_os_for_verbose.getenv('BOOK_SEARCH_GOOGLE_RESULT_TIMEOUT') or '3.8'))
_OPENLIBRARY_RESULT_TIMEOUT = float((_os_for_verbose.getenv('BOOK_SEARCH_OPENLIBRARY_RESULT_TIMEOUT') or '5.0'))
_BOOK_SEARCH_GLOBAL_TIMEOUT = float((_os_for_verbose.getenv('BOOK_SEARCH_GLOBAL_TIMEOUT') or '5.5'))



def _purge_search_cache_locked() -> None:
    if not _SEARCH_CACHE:
        return
    now = time.time()
    if _SEARCH_CACHE_TTL > 0:
        expired = [cache_key for cache_key, (ts, _) in _SEARCH_CACHE.items() if now - ts > _SEARCH_CACHE_TTL]
        for cache_key in expired:
            _SEARCH_CACHE.pop(cache_key, None)
    while _SEARCH_CACHE_MAX > 0 and len(_SEARCH_CACHE) > _SEARCH_CACHE_MAX:
        _SEARCH_CACHE.popitem(last=False)


def _search_cache_get(key: tuple) -> Optional[List[Dict[str, Any]]]:
    if _SEARCH_CACHE_MAX <= 0 or _SEARCH_CACHE_TTL <= 0:
        return None
    with _SEARCH_CACHE_LOCK:
        entry = _SEARCH_CACHE.get(key)
        if not entry:
            return None
        ts, payload = entry
        if time.time() - ts > _SEARCH_CACHE_TTL:
            _SEARCH_CACHE.pop(key, None)
            return None
        try:
            _SEARCH_CACHE.move_to_end(key)
        except Exception:
            pass
        return copy.deepcopy(payload)


def _search_cache_set(key: tuple, results: List[Dict[str, Any]]) -> None:
    if _SEARCH_CACHE_MAX <= 0 or _SEARCH_CACHE_TTL <= 0:
        return
    with _SEARCH_CACHE_LOCK:
        _SEARCH_CACHE[key] = (time.time(), copy.deepcopy(results))
        _purge_search_cache_locked()


def validate_asin(asin: str) -> bool:
    """
    Validate ASIN format.
    ASIN is typically 10 characters, alphanumeric, starts with B for most products.
    """
    if not asin or not isinstance(asin, str):
        return False
    
    # Remove whitespace and convert to uppercase
    asin = asin.strip().upper()
    
    # ASIN should be exactly 10 characters
    if len(asin) != 10:
        return False
    
    # ASIN should be alphanumeric
    if not asin.isalnum():
        return False
    
    return True


def normalize_title(title: str) -> str:
    """Normalize title for comparison by removing articles, punctuation, and extra spaces."""
    if not title:
        return ""
    
    # Convert to lowercase
    title = title.lower().strip()
    
    # Remove common articles at the beginning
    articles = ['the', 'a', 'an']
    words = title.split()
    if words and words[0] in articles:
        words = words[1:]
        title = ' '.join(words)
    
    # Remove punctuation and normalize spaces
    title = re.sub(r'[^\w\s]', ' ', title)
    title = re.sub(r'\s+', ' ', title).strip()
    
    return title


def calculate_title_similarity(search_title: str, result_title: str) -> float:
    """Calculate similarity score between search title and result title."""
    if not search_title or not result_title:
        return 0.0
    
    # Normalize both titles
    norm_search = normalize_title(search_title)
    norm_result = normalize_title(result_title)
    
    if not norm_search or not norm_result:
        return 0.0
    
    # Use SequenceMatcher for similarity
    similarity = SequenceMatcher(None, norm_search, norm_result).ratio()
    
    # Boost exact matches
    if norm_search == norm_result:
        similarity = 1.0
    
    # Boost if search title is contained in result title or vice versa
    elif norm_search in norm_result or norm_result in norm_search:
        similarity = max(similarity, 0.85)
    
    return similarity


def _dedupe_candidates(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    deduped: List[Dict[str, Any]] = []
    for cand in candidates:
        url = cand.get('url')
        if not url or url in seen:
            continue
        deduped.append(cand)
        seen.add(url)
    return deduped


def _build_google_cover_candidates(volume_info: Dict[str, Any]) -> List[Dict[str, Any]]:
    image_links = volume_info.get('imageLinks') or {}
    if not isinstance(image_links, dict) or not image_links:
        return []
    try:
        from app.utils.book_utils import upgrade_google_cover_url
    except Exception:
        def upgrade_google_cover_url(raw_url: Optional[str], *, allow_probe: bool = True) -> Optional[str]:  # type: ignore
            return raw_url
    order = ['extraLarge', 'large', 'medium', 'small', 'thumbnail', 'smallThumbnail']
    candidates: List[Dict[str, Any]] = []
    for label in order:
        raw = image_links.get(label)
        if not raw or not isinstance(raw, str):
            continue
        normalized = upgrade_google_cover_url(raw, allow_probe=False)
        if not normalized:
            continue
        candidates.append({'provider': 'google', 'size': label, 'url': normalized})
    return _dedupe_candidates(candidates)


def _build_openlibrary_cover_candidates(cover_i: Optional[int]) -> List[Dict[str, Any]]:
    if not cover_i:
        return []
    base = f"https://covers.openlibrary.org/b/id/{cover_i}"
    sizes = (
        ('L', 'large'),
        ('M', 'medium'),
        ('S', 'small'),
    )
    candidates = []
    for suffix, label in sizes:
        candidates.append({'provider': 'openlibrary', 'size': label, 'url': f"{base}-{suffix}.jpg?default=false"})
    return candidates


def select_best_publication_date(date1: str, date2: str, year1: Optional[int], year2: Optional[int]) -> tuple[str, Optional[int]]:
    """
    Select the best publication date from two sources.
    Prefers full dates over just years when they're from the same year.
    
    Returns:
        tuple: (best_published_date, best_publication_year)
    """
    # If both dates are empty, return what we have
    if not date1 and not date2:
        return date1 or date2 or '', year1 or year2
    
    # If only one has a date, use it
    if date1 and not date2:
        return date1, year1 or year2
    if date2 and not date1:
        return date2, year2 or year1
    
    # Both have dates - determine which is better
    # Check if either looks like a full date (has month/day info)
    date1_is_full = bool(re.search(r'\d{1,2}[-/]\d{1,2}[-/]\d{4}|\d{4}-\d{1,2}-\d{1,2}|[A-Za-z]{3,}', date1))
    date2_is_full = bool(re.search(r'\d{1,2}[-/]\d{1,2}[-/]\d{4}|\d{4}-\d{1,2}-\d{1,2}|[A-Za-z]{3,}', date2))
    
    # If years are the same or very close, prefer the full date
    if year1 and year2 and abs(year1 - year2) <= 1:
        if date1_is_full and not date2_is_full:
            return date1, year1
        elif date2_is_full and not date1_is_full:
            return date2, year2
    
    # If one is clearly a year and the other has more info, prefer the detailed one
    if date1_is_full and not date2_is_full:
        return date1, year1 or year2
    elif date2_is_full and not date1_is_full:
        return date2, year2 or year1
    
    # Default to the first one
    return date1, year1 or year2


def search_google_books(title: str, max_results: int = 20, author: Optional[str] = None) -> List[Dict[str, Any]]:
    """Search Google Books API for books by title (and optional author)."""
    print(f"📚 [GOOGLE_BOOKS_SEARCH] Searching for: '{title}'" + (f" by '{author}'" if author else ""))
    
    if not title:
        return []
    
    # Prepare search query
    q_title = quote_plus(title)
    if author and isinstance(author, str) and author.strip():
        q_author = quote_plus(author.strip())
        q = f"intitle:{q_title}+inauthor:{q_author}"
    else:
        q = f"intitle:{q_title}"
    url = f"https://www.googleapis.com/books/v1/volumes?q={q}&maxResults={max_results}"
    
    try:
        response = requests.get(url, timeout=(_GOOGLE_CONNECT_TIMEOUT, _GOOGLE_READ_TIMEOUT))
        response.raise_for_status()
        data = response.json()
        
        results = []
        items = data.get('items', [])
        print(f"📚 [GOOGLE_BOOKS_SEARCH] Found {len(items)} items")
        
        for item in items:
            try:
                volume_info = item.get('volumeInfo', {})
                
                # Extract basic information
                book_title = volume_info.get('title', '')
                subtitle = volume_info.get('subtitle', '')
                authors = volume_info.get('authors', [])
                publisher = volume_info.get('publisher', '')
                published_date = volume_info.get('publishedDate', '')
                page_count = volume_info.get('pageCount')
                language = volume_info.get('language', 'en')
                description = volume_info.get('description', '')
                categories = volume_info.get('categories', [])
                average_rating = volume_info.get('averageRating')
                rating_count = volume_info.get('ratingsCount')
                
                # Extract ISBNs and ASIN
                isbn_10 = None
                isbn_13 = None
                asin = None
                industry_identifiers = volume_info.get('industryIdentifiers', [])
                for identifier in industry_identifiers:
                    identifier_type = identifier.get('type', '')
                    identifier_value = identifier.get('identifier', '')
                    if identifier_type == 'ISBN_10':
                        isbn_10 = identifier_value
                    elif identifier_type == 'ISBN_13':
                        isbn_13 = identifier_value
                    elif identifier_type == 'ASIN':
                        if validate_asin(identifier_value):
                            asin = identifier_value.strip().upper()
                    elif identifier_type == 'OTHER' and 'ASIN' in str(identifier_value):
                        # Fallback for incorrectly categorized ASINs
                        # Extract ASIN from the identifier value
                        asin_match = re.search(r'[A-Z0-9]{10}', str(identifier_value).upper())
                        if asin_match and validate_asin(asin_match.group()):
                            asin = asin_match.group()
                
                # Extract and normalize Google cover image (no immediate download for speed)
                cover_candidates = _build_google_cover_candidates(volume_info)
                cover_url = None
                try:
                    from app.utils.book_utils import select_highest_google_image, upgrade_google_cover_url
                    raw_cover = select_highest_google_image(volume_info.get('imageLinks', {}))
                    if raw_cover:
                        cover_url = upgrade_google_cover_url(raw_cover, allow_probe=False)
                except Exception as ce:
                    print(f"[COVER_NORMALIZE] Google cover normalization failed: {ce}")
                if not cover_url and cover_candidates:
                    cover_url = cover_candidates[0].get('url')
                
                # Extract publication year
                publication_year = None
                if published_date:
                    year_match = re.search(r'(\d{4})', published_date)
                    if year_match:
                        publication_year = int(year_match.group(1))
                
                # Calculate similarity score
                similarity_score = calculate_title_similarity(title, book_title)

                # Optional author bonus
                author_bonus = 0.0
                if author and authors:
                    try:
                        def _norm_name(n: str) -> str:
                            n = (n or '').lower()
                            n = re.sub(r"[^a-z\s]", " ", n)
                            n = re.sub(r"\s+", " ", n).strip()
                            return n
                        sa = _norm_name(author)
                        result_names = [_norm_name(a) for a in authors]
                        # Exact normalized match or contains
                        if sa and any(sa == rn for rn in result_names):
                            author_bonus = 0.2
                        elif sa and any(sa in rn or rn in sa for rn in result_names):
                            author_bonus = 0.12
                        else:
                            # Last-name overlap provides small boost
                            last = sa.split(" ")[-1] if sa else ''
                            if last and any(last in rn.split(" ")[-1] for rn in result_names):
                                author_bonus = 0.08
                    except Exception:
                        author_bonus = 0.0
                similarity_score = min(1.0, similarity_score + author_bonus)
                
                result = {
                    'title': book_title,
                    'subtitle': subtitle,
                    'authors': authors,
                    'author': ', '.join(authors) if authors else '',
                    'publication_year': publication_year,
                    'published_date': published_date,
                    'page_count': page_count,
                    'cover_url': cover_url,
                    'isbn_10': isbn_10,
                    'isbn_13': isbn_13,
                    'asin': asin,
                    'publisher': publisher,
                    'language': language,
                    'description': description,
                    'categories': categories,
                    'raw_category_paths': list(categories) if categories else [],
                    'average_rating': average_rating,
                    'rating_count': rating_count,
                    'google_books_id': item.get('id'),
                    'openlibrary_id': None,  # Will be filled if found in OpenLibrary
                    'source': 'Google Books',
                    'similarity_score': similarity_score,
                    'cover_candidates': cover_candidates
                }
                
                results.append(result)
                
            except Exception as e:
                print(f"❌ [GOOGLE_BOOKS_SEARCH] Error processing item: {e}")
                continue
        
        print(f"✅ [GOOGLE_BOOKS_SEARCH] Processed {len(results)} valid results")
        return results
        
    except requests.exceptions.RequestException as e:
        print(f"❌ [GOOGLE_BOOKS_SEARCH] Request error: {e}")
        return []
    except Exception as e:
        print(f"❌ [GOOGLE_BOOKS_SEARCH] Unexpected error: {e}")
        return []


def search_openlibrary(title: str, max_results: int = 20, author: Optional[str] = None) -> List[Dict[str, Any]]:
    """Search OpenLibrary API for books by title (and optional author)."""
    print(f"📖 [OPENLIBRARY_SEARCH] Searching for: '{title}'" + (f" by '{author}'" if author else ""))
    
    if not title:
        return []
    
    # Prepare search query
    q_title = quote_plus(title)
    if author and isinstance(author, str) and author.strip():
        q_author = quote_plus(author.strip())
        url = f"https://openlibrary.org/search.json?title={q_title}&author={q_author}&limit={max_results}"
    else:
        url = f"https://openlibrary.org/search.json?title={q_title}&limit={max_results}"
    
    try:
        response = requests.get(url, timeout=(_OPENLIBRARY_CONNECT_TIMEOUT, _OPENLIBRARY_READ_TIMEOUT))
        response.raise_for_status()
        data = response.json()
        
        results = []
        docs = data.get('docs', [])
        print(f"📖 [OPENLIBRARY_SEARCH] Found {len(docs)} items")
        
        for doc in docs:
            try:
                # Extract basic information
                book_title = doc.get('title', '')
                subtitle = doc.get('subtitle', '')
                authors = doc.get('author_name', [])
                publisher = doc.get('publisher', [])
                publisher_str = publisher[0] if publisher else ''
                
                # Extract publication year
                first_publish_year = doc.get('first_publish_year')
                publish_year = doc.get('publish_year', [])
                publication_year = first_publish_year or (publish_year[0] if publish_year else None)
                
                # Extract ISBNs
                isbn_list = doc.get('isbn', [])
                isbn_10 = None
                isbn_13 = None
                
                for isbn in isbn_list:
                    if len(isbn) == 10:
                        isbn_10 = isbn
                    elif len(isbn) == 13:
                        isbn_13 = isbn
                    # Take the first of each type we find
                    if isbn_10 and isbn_13:
                        break
                
                # Extract other metadata
                language = doc.get('language', [])
                language_str = language[0] if language else 'en'
                
                subject = doc.get('subject', [])
                categories = subject[:5] if subject else []  # Limit to first 5 subjects
                
                # Extract cover image (centralized via CoverService)
                cover_i = doc.get('cover_i')
                cover_candidates = _build_openlibrary_cover_candidates(cover_i)
                cover_url = cover_candidates[0].get('url') if cover_candidates else None
                
                # Extract OpenLibrary ID
                openlibrary_id = None
                key = doc.get('key')
                if key:
                    # Key format: /works/OL123456W
                    match = re.search(r'/works/(.+)', key)
                    if match:
                        openlibrary_id = match.group(1)
                
                # Calculate similarity score
                similarity_score = calculate_title_similarity(title, book_title)
                # Optional author bonus
                author_bonus = 0.0
                if author and authors:
                    try:
                        def _norm_name(n: str) -> str:
                            n = (n or '').lower()
                            n = re.sub(r"[^a-z\s]", " ", n)
                            n = re.sub(r"\s+", " ", n).strip()
                            return n
                        sa = _norm_name(author)
                        result_names = [_norm_name(a) for a in authors]
                        if sa and any(sa == rn for rn in result_names):
                            author_bonus = 0.2
                        elif sa and any(sa in rn or rn in sa for rn in result_names):
                            author_bonus = 0.12
                        else:
                            last = sa.split(" ")[-1] if sa else ''
                            if last and any(last in rn.split(" ")[-1] for rn in result_names):
                                author_bonus = 0.08
                    except Exception:
                        author_bonus = 0.0
                similarity_score = min(1.0, similarity_score + author_bonus)
                
                result = {
                    'title': book_title,
                    'subtitle': subtitle,
                    'authors': authors,
                    'author': ', '.join(authors) if authors else '',
                    'publication_year': publication_year,
                    'published_date': str(publication_year) if publication_year else '',
                    'page_count': None,  # OpenLibrary search doesn't return page count
                    'cover_url': cover_url,
                    'isbn_10': isbn_10,
                    'isbn_13': isbn_13,
                    'publisher': publisher_str,
                    'language': language_str,
                    'description': None,  # Not available in search results
                    'categories': categories,
                    'average_rating': None,
                    'rating_count': None,
                    'google_books_id': None,  # Will be filled if found in Google Books
                    'openlibrary_id': openlibrary_id,
                    'source': 'OpenLibrary',
                    'similarity_score': similarity_score,
                    'cover_candidates': cover_candidates
                }
                
                results.append(result)
                
            except Exception as e:
                print(f"❌ [OPENLIBRARY_SEARCH] Error processing item: {e}")
                continue
        
        print(f"✅ [OPENLIBRARY_SEARCH] Processed {len(results)} valid results")
        return results
        
    except requests.exceptions.RequestException as e:
        print(f"❌ [OPENLIBRARY_SEARCH] Request error: {e}")
        return []
    except Exception as e:
        print(f"❌ [OPENLIBRARY_SEARCH] Unexpected error: {e}")
        return []


def merge_and_rank_results(search_title: str, google_results: List[Dict], 
                          openlibrary_results: List[Dict], max_results: int = 10) -> List[Dict]:
    """
    Merge results from both APIs, deduplicate by ISBN, and rank by similarity.
    
    When the same book is found in both APIs (matching ISBN), merge the data
    and keep both source IDs.
    """
    print(f"🔀 [MERGE_RANK] Merging {len(google_results)} Google + {len(openlibrary_results)} OpenLibrary results")
    
    merged_results = {}
    
    # Process Google Books results first
    for result in google_results:
        isbn_key = result.get('isbn_13') or result.get('isbn_10') or f"google_{result.get('google_books_id')}"
        if isbn_key:
            record = result.copy()
            if result.get('cover_candidates'):
                record['cover_candidates'] = [c.copy() for c in result['cover_candidates'] if isinstance(c, dict)]
            merged_results[isbn_key] = record
    
    # Process OpenLibrary results and merge with Google Books where ISBN matches
    for result in openlibrary_results:
        isbn_key = result.get('isbn_13') or result.get('isbn_10') or f"openlibrary_{result.get('openlibrary_id')}"
        
        if isbn_key in merged_results:
            # Found matching ISBN - merge data
            existing = merged_results[isbn_key]
            
            # Merge OpenLibrary ID
            existing['openlibrary_id'] = result.get('openlibrary_id')
            
            # Smart merge publication date information
            best_date, best_year = select_best_publication_date(
                existing.get('published_date', ''),
                result.get('published_date', ''),
                existing.get('publication_year'),
                result.get('publication_year')
            )
            existing['published_date'] = best_date
            existing['publication_year'] = best_year
            
            # Fill in missing data from OpenLibrary (except dates which we handled above)
            for key, value in result.items():
                if key not in ['published_date', 'publication_year'] and (key not in existing or not existing[key]):
                    existing[key] = value

            incoming_candidates = result.get('cover_candidates') or []
            if incoming_candidates:
                existing_candidates = list(existing.get('cover_candidates') or [])
                seen_urls = {c.get('url') for c in existing_candidates if isinstance(c, dict) and c.get('url')}
                for cand in incoming_candidates:
                    url = cand.get('url') if isinstance(cand, dict) else None
                    if not url or url in seen_urls:
                        continue
                    existing_candidates.append(cand.copy())
                    seen_urls.add(url)
                if existing_candidates:
                    existing['cover_candidates'] = existing_candidates
            
            # Update source to indicate both
            existing['source'] = 'Google Books + OpenLibrary'
            
            # Use the higher similarity score
            existing['similarity_score'] = max(
                existing.get('similarity_score', 0),
                result.get('similarity_score', 0)
            )
            
            print(f"🔗 [MERGE_RANK] Merged book with ISBN {isbn_key[:20]}...")
            
        else:
            # New book from OpenLibrary
            record = result.copy()
            if result.get('cover_candidates'):
                record['cover_candidates'] = [c.copy() for c in result['cover_candidates'] if isinstance(c, dict)]
            merged_results[isbn_key] = record
    
    # Convert to list and sort by similarity score
    final_results = list(merged_results.values())
    final_results.sort(key=lambda x: x.get('similarity_score', 0), reverse=True)
    
    # Limit to max_results
    final_results = final_results[:max_results]
    
    print(f"✅ [MERGE_RANK] Final results: {len(final_results)} books")
    for i, result in enumerate(final_results[:5]):  # Log top 5
        print(f"  {i+1}. '{result.get('title', '')}' (score: {result.get('similarity_score', 0):.3f}, source: {result.get('source', '')})")
    
    return final_results


def search_books_by_title(title: str, max_results: int = 10, author: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Search for books by title across Google Books and OpenLibrary APIs.
    
    Args:
        title: The book title to search for
        max_results: Maximum number of results to return (default: 10)
    
    Returns:
        List of book dictionaries sorted by title similarity, with the following fields:
        - title, subtitle, authors, author (comma-separated)
        - publication_year, published_date, page_count
        - cover_url, isbn_10, isbn_13, publisher, language
        - description, categories, average_rating, rating_count
        - google_books_id, openlibrary_id, source, similarity_score
    """
    print(f"🔍 [BOOK_SEARCH] Starting search for: '{title}'" + (f" by '{author}'" if author else ""))
    
    if not title or not title.strip():
        print(f"❌ [BOOK_SEARCH] Empty title provided")
        return []
    
    title = title.strip()
    author_arg = author.strip() if isinstance(author, str) else None

    cache_key = (title.lower(), (author_arg or '').lower(), int(max_results))
    cached = _search_cache_get(cache_key)
    if cached is not None:
        print(f"💾 [BOOK_SEARCH] Cache hit for '{title}'")
        return cached

    start_time = time.perf_counter()
    google_results: List[Dict[str, Any]] = []
    try:
        google_results = search_google_books(title, max_results * 2, author_arg)
    except Exception as exc:
        print(f"❌ [BOOK_SEARCH] Google search failed: {exc}")

    openlibrary_results: List[Dict[str, Any]] = []
    elapsed = time.perf_counter() - start_time
    remaining_budget = max(0.0, _BOOK_SEARCH_GLOBAL_TIMEOUT - elapsed)
    if remaining_budget <= 0:
        print("⏱️ [BOOK_SEARCH] Skipping OpenLibrary results due to global timeout budget")
    else:
        try:
            openlibrary_results = search_openlibrary(title, max_results * 2, author_arg)
        except Exception as exc:
            print(f"❌ [BOOK_SEARCH] OpenLibrary search failed: {exc}")
    
    # Merge, deduplicate, and rank results
    final_results = merge_and_rank_results(title, google_results, openlibrary_results, max_results)
    _search_cache_set(cache_key, final_results)
    
    print(f"🎯 [BOOK_SEARCH] Search complete. Returning {len(final_results)} results for '{title}'" + (f" by '{author}'" if author else ""))
    
    return copy.deepcopy(final_results)


def search_books_with_display_fields(title: str, max_results: int = 10, isbn_required: bool = False, author: Optional[str] = None) -> Dict[str, Any]:
    """
    Search for books and return results formatted for display.
    
    Args:
        title: The book title to search for
        max_results: Maximum number of results to return
        isbn_required: If True, only return books that have ISBN-10 or ISBN-13
        author: Optional author name to filter results
    
    Returns:
        Dict with 'results' containing the book list and 'metadata' with search info
    """
    results = search_books_by_title(title, max_results, author)  # Pass author to search_books_by_title
    
    # Filter for ISBN if required
    if isbn_required:
        results = [book for book in results if book.get('isbn_10') or book.get('isbn_13')]
    
    # Format results for display - keep only essential fields visible, store full data
    display_results = []
    for result in results:
        # Non-blocking cover strategy: select candidate quickly; let UI optionally schedule async processing
        cover_candidates = result.get('cover_candidates') or []
        if cover_candidates and not result.get('cover_candidate'):
            result['cover_candidate'] = cover_candidates[0]
        cover_url = result.get('cover_url') or (cover_candidates[0].get('url') if cover_candidates else None)
        if not cover_url:
            try:
                from app.services.cover_service import cover_service
                cand = cover_service.select_candidate(
                    isbn=result.get('isbn_13') or result.get('isbn_10'),
                    title=result.get('title'),
                    author=(result.get('authors') or [None])[0] if isinstance(result.get('authors'), list) else result.get('author')
                )
                if cand:
                    cover_url = cand.get('url')
                    result['cover_candidate'] = cand
                    existing_candidates = [c.copy() for c in cover_candidates if isinstance(c, dict)]
                    if isinstance(cand, dict):
                        if not any(c.get('url') == cand.get('url') for c in existing_candidates):
                            existing_candidates.append(cand.copy())
                    if existing_candidates:
                        result['cover_candidates'] = existing_candidates
                    result['cover_url'] = cover_url
            except Exception:
                pass
        display_item = {
            'title': result.get('title', ''),
            'author': result.get('author', ''),
            'publication_year': result.get('publication_year'),
            'page_count': result.get('page_count'),
            'similarity_score': result.get('similarity_score', 0),
            'source': result.get('source', ''),
            'cover_url': cover_url,
            'cover_candidate': result.get('cover_candidate'),
            'cover_candidates': result.get('cover_candidates'),
            'full_data': result
        }
        display_results.append(display_item)
    
    return {
        'results': display_results,
        'metadata': {
            'search_title': title,
            'total_results': len(display_results),
            'max_results': max_results,
            'search_timestamp': time.time(),
            'isbn_required': isbn_required
        }
    }
