from collections import OrderedDict
import time as _time
import os as _os_cover_debug
from typing import Dict, List, Optional, Tuple

_PROVIDER_META_CACHE: Dict[str, Tuple[dict, float]] = {}
_CACHE_TTL_SECONDS = 300  # 5 minutes; cover metadata rarely changes

_BEST_CACHE_TTL_SECONDS = int((_os_cover_debug.getenv('BEST_COVER_CACHE_TTL') or '21600'))
_BEST_CACHE_MAX_ENTRIES = int((_os_cover_debug.getenv('BEST_COVER_CACHE_MAX') or '512'))
_CANDIDATE_CACHE_TTL_SECONDS = int((_os_cover_debug.getenv('COVER_CANDIDATE_CACHE_TTL') or '900'))
_CANDIDATE_CACHE_MAX_ENTRIES = int((_os_cover_debug.getenv('COVER_CANDIDATE_CACHE_MAX') or '256'))

_BEST_COVER_CACHE: OrderedDict[str, Tuple[float, dict]] = OrderedDict()
_COVER_CANDIDATE_CACHE: OrderedDict[str, Tuple[float, List[dict]]] = OrderedDict()

# Verbose flag for cover & search debug (ENV: VERBOSE, IMPORT_VERBOSE, COVER_VERBOSE)
_COVER_VERBOSE = (
    (_os_cover_debug.getenv('VERBOSE') or 'false').lower() == 'true'
    or (_os_cover_debug.getenv('IMPORT_VERBOSE') or 'false').lower() == 'true'
    or (_os_cover_debug.getenv('COVER_VERBOSE') or 'false').lower() == 'true'
)

def _cache_get(key):
    rec = _PROVIDER_META_CACHE.get(key)
    if not rec:
        return None
    data, ts = rec
    import time as _t
    if _t.time() - ts > _CACHE_TTL_SECONDS:
        try:
            del _PROVIDER_META_CACHE[key]
        except Exception:
            pass
        return None
    return data

def _cache_set(key, data):
    import time as _t
    _PROVIDER_META_CACHE[key] = (data, _t.time())


def _purge_ordered_dict(store: OrderedDict, ttl: int, max_entries: int) -> None:
    now = _time.time()
    if ttl > 0:
        for key, (ts, _) in list(store.items()):
            if now - ts > ttl:
                store.pop(key, None)
            else:
                break
    if max_entries > 0:
        while len(store) > max_entries:
            store.popitem(last=False)


def _normalized_cover_key(isbn: Optional[str], title: Optional[str], author: Optional[str]) -> str:
    return "|".join([
        (isbn or '').strip().lower(),
        (title or '').strip().lower(),
        (author or '').strip().lower(),
    ])


def _best_cache_get(cache_key: str) -> Optional[dict]:
    entry = _BEST_COVER_CACHE.get(cache_key)
    if not entry:
        return None
    ts, payload = entry
    if _time.time() - ts > _BEST_CACHE_TTL_SECONDS:
        _BEST_COVER_CACHE.pop(cache_key, None)
        return None
    try:
        _BEST_COVER_CACHE.move_to_end(cache_key)
    except Exception:
        pass
    return payload


def _best_cache_set(cache_key: str, value: dict) -> None:
    if _BEST_CACHE_MAX_ENTRIES <= 0:
        return
    _BEST_COVER_CACHE[cache_key] = (_time.time(), value)
    _purge_ordered_dict(_BEST_COVER_CACHE, _BEST_CACHE_TTL_SECONDS, _BEST_CACHE_MAX_ENTRIES)


def _candidate_cache_key(isbn: Optional[str], title: Optional[str], author: Optional[str]) -> str:
    return _normalized_cover_key(isbn, title, author)


def _candidate_cache_get(isbn: Optional[str], title: Optional[str], author: Optional[str]) -> Optional[List[dict]]:
    if _CANDIDATE_CACHE_MAX_ENTRIES <= 0:
        return None
    key = _candidate_cache_key(isbn, title, author)
    entry = _COVER_CANDIDATE_CACHE.get(key)
    if not entry:
        return None
    ts, payload = entry
    if _time.time() - ts > _CANDIDATE_CACHE_TTL_SECONDS:
        _COVER_CANDIDATE_CACHE.pop(key, None)
        return None
    try:
        _COVER_CANDIDATE_CACHE.move_to_end(key)
    except Exception:
        pass
    return [candidate.copy() for candidate in payload]


def _candidate_cache_set(isbn: Optional[str], title: Optional[str], author: Optional[str], candidates: List[dict]) -> None:
    if _CANDIDATE_CACHE_MAX_ENTRIES <= 0:
        return
    key = _candidate_cache_key(isbn, title, author)
    _COVER_CANDIDATE_CACHE[key] = (_time.time(), [candidate.copy() for candidate in candidates])
    _purge_ordered_dict(_COVER_CANDIDATE_CACHE, _CANDIDATE_CACHE_TTL_SECONDS, _CANDIDATE_CACHE_MAX_ENTRIES)

# --- Google Cover Utilities ---
_GOOGLE_SIZE_ORDER = ['extraLarge','large','medium','small','thumbnail','smallThumbnail']

def select_highest_google_image(image_links: dict | None) -> str | None:
    if not image_links or not isinstance(image_links, dict):
        return None
    for size in _GOOGLE_SIZE_ORDER:
        url = image_links.get(size)
        if url:
            if isinstance(url, str) and url.startswith('http:'):
                url = url.replace('http:','https:')
            return url
    # fallback: any value
    for v in image_links.values():
        if isinstance(v,str):
            return v
    return None

def upgrade_google_cover_url(raw_url: str | None, *, allow_probe: bool = True) -> str | None:
    """Normalize and opportunistically upgrade Google Books cover URL.

    Strategy:
    1. Ensure base params (printsec=frontcover, img=1).
    2. If no zoom, default zoom=1.
    3. Probe a zoom=0 variant (largest) ONLY if original isn't already explicit extraLarge/large and we don't already have zoom=0.
       - Use a 1s HEAD request; if Content-Length improves ( > original or above threshold ) adopt it.
    Safe + fast: short timeout, swallow errors.
    """
    if not raw_url or 'books.google' not in raw_url:
        return raw_url
    import re as _re
    from flask import current_app
    url = raw_url
    # Normalize protocol
    if url.startswith('http:'):
        url = url.replace('http:', 'https:')
    # Base params
    if 'printsec=' not in url:
        sep = '&' if '?' in url else '?'
        url = f"{url}{sep}printsec=frontcover"
    if 'img=1' not in url:
        url += '&img=1'
    # Ensure some zoom if none specified
    has_zoom = 'zoom=' in url
    if not has_zoom:
        sep = '&' if '?' in url else '?'
        url = f"{url}{sep}zoom=1"

    # Decide whether to attempt zoom=0 upgrade
    attempt_zoom0 = ('zoom=0' not in url) and ('extraLarge' not in url) and ('large' not in url)
    chosen = url
    if attempt_zoom0 and allow_probe:
        z0 = _re.sub(r'zoom=\d', 'zoom=0', url) if has_zoom else url + '&zoom=0'
        try:
            import requests
            h1 = None
            h2 = None
            # Probe current URL
            try:
                h1 = requests.head(chosen, timeout=1, allow_redirects=True)
            except Exception:
                pass
            try:
                h2 = requests.head(z0, timeout=1, allow_redirects=True)
            except Exception:
                h2 = None
            def _len(resp):
                if not resp: return 0
                try:
                    return int(resp.headers.get('Content-Length','0') or 0)
                except Exception:
                    return 0
            size_curr = _len(h1)
            size_z0 = _len(h2)
            # Adopt zoom=0 if clearly larger or current size tiny
            if size_z0 > size_curr * 1.15 or (size_curr < 35_000 and size_z0 > size_curr):
                chosen = z0
                if _COVER_VERBOSE:
                    try:
                        current_app.logger.info(f"[COVER][UPGRADE] zoom0 adopted curr={size_curr} z0={size_z0} url={chosen}")
                    except Exception:
                        pass
            else:
                if _COVER_VERBOSE:
                    try:
                        current_app.logger.debug(f"[COVER][UPGRADE] zoom0 skipped curr={size_curr} z0={size_z0}")
                    except Exception:
                        pass
        except Exception:
            pass
    return chosen
def normalize_cover_url(url: str | None) -> str | None:
    """Universal normalization entrypoint for any discovered cover URL.

    - Google Books: ensure https, printsec=frontcover, img=1, zoom parameter (adaptive via upgrade_google_cover_url)
    - OpenLibrary: prefer large (-L.jpg), upgrade M/S to L when pattern fits
    - Others: unchanged
    """
    if not url:
        return url
    try:
        if 'books.google' in url:
            return upgrade_google_cover_url(url)
        if 'covers.openlibrary.org' in url:
            import re as _re
            return _re.sub(r'-(S|M)\.(jpg|png)$', r'-L.\2', url)
    except Exception:
        return url
    return url

def get_best_cover_for_book(isbn=None, title=None, author=None):
    """
    Universal cover selection: favor Google Books, fall back to OpenLibrary, select largest/clearest image.
    Args:
        isbn (str): ISBN-13 or ISBN-10
        title (str): Book title
        author (str): Book author(s)
    Returns:
        dict: {'cover_url': ..., 'source': ..., 'quality': ...}
    """
    # Simple in-memory cache (module-level) to cut repeated latency
    global _BEST_COVER_CACHE
    cache_key = _normalized_cover_key(isbn, title, author)
    cached = _best_cache_get(cache_key)
    if cached is not None:
        return cached

    import time
    t0 = time.perf_counter()
    phase_logs = []
    # Try Google Books first
    gb_data = None
    if isbn:
        t_gb_start = time.perf_counter()
        gb_data = get_google_books_cover(isbn, fetch_title_author=True)
        phase_logs.append(f"google_isbn={time.perf_counter()-t_gb_start:.3f}s has={'yes' if gb_data else 'no'}")
    if not gb_data and title:
        t_gb_title = time.perf_counter()
        gb_results = search_google_books_by_title_author(title, author, limit=1)
        phase_logs.append(f"google_title={time.perf_counter()-t_gb_title:.3f}s res={len(gb_results) if gb_results else 0}")
        gb_data = gb_results[0] if gb_results else None
    cover_url = None
    quality = 'none'
    if gb_data and gb_data.get('cover_url'):
        cover_url = normalize_cover_url(gb_data['cover_url'])
        # Prefer provided explicit largest variants; avoid forcing zoom=0 which can degrade quality
        if cover_url and 'books.google' in cover_url:
            # If no explicit size or zoom parameter, add zoom=1 (empirically high quality)
            if 'zoom=' not in cover_url and not any(x in cover_url for x in ('extraLarge', 'large', 'medium', 'small', 'thumbnail')):
                sep = '&' if '?' in cover_url else '?'
                cover_url = f"{cover_url}{sep}zoom=1"
            # Ensure printsec frontcover for consistency
            if 'printsec=' not in cover_url:
                sep = '&' if '?' in cover_url else '?'
                cover_url = f"{cover_url}{sep}printsec=frontcover"
        # Determine quality tag based on known larger indicators
        if cover_url and ('extraLarge' in cover_url or 'zoom=1' in cover_url or 'large' in cover_url):
            quality = 'high'
        else:
            quality = 'medium'
    # Fallback to OpenLibrary if Google Books failed or image is missing/poor
    if not cover_url or quality == 'none':
        ol_data = None
        if isbn:
            t_ol_start = time.perf_counter()
            try:
                ol_data = fetch_book_data(isbn)
            except Exception:
                ol_data = None
            phase_logs.append(f"openlibrary_isbn={time.perf_counter()-t_ol_start:.3f}s has={'yes' if ol_data else 'no'}")
        if not ol_data and title:
            t_ol_title = time.perf_counter()
            ol_results = search_book_by_title_author(title, author)
            # ol_results may be list or dict
            count = (len(ol_results) if isinstance(ol_results, list) else (1 if ol_results else 0))
            phase_logs.append(f"openlibrary_title={time.perf_counter()-t_ol_title:.3f}s res={count}")
            ol_data = ol_results if isinstance(ol_results, dict) else (ol_results[0] if ol_results else None)
        if ol_data and ol_data.get('cover_url'):
            cover_url = normalize_cover_url(ol_data['cover_url'])
            quality = 'medium' if 'L.jpg' in str(cover_url) else 'low'
            source = 'OpenLibrary'
        else:
            source = 'none'
    else:
        source = 'Google Books'
    # Final fallback: None
    result_obj = {
        'cover_url': cover_url,
        'source': source,
        'quality': quality
    }
    _best_cache_set(cache_key, result_obj)
    total = time.perf_counter() - t0
    try:
        from flask import current_app
        if _COVER_VERBOSE:
            current_app.logger.info(f"[COVER][SELECT] total={total:.3f}s isbn={isbn} title='{(title or '')[:40]}' source={result_obj['source']} quality={result_obj['quality']} steps={' | '.join(phase_logs)}")
    except Exception:
        pass
    return result_obj

def get_cover_candidates(isbn=None, title=None, author=None):
    """Return ordered list of candidate cover URLs with metadata without downloading.

    Order preference:
    1. Google: extraLarge, large, medium, small, thumbnail
    2. OpenLibrary: large (L), medium (M), small (S)
    Returns list of dicts: {'provider': 'google'|'openlibrary', 'size': label, 'url': url}
    """
    cached_candidates = _candidate_cache_get(isbn, title, author)
    if cached_candidates is not None:
        return cached_candidates

    candidates: List[dict] = []
    def _expand_google_variants(url: str):
        """Generate ordered google image variants (largest first) heuristically.

        Google Books image URLs often look like:
          https://books.google.com/books/content?id=XXXX&printsec=frontcover&img=1&zoom=1&edge=curl&source=gbs_api

        Empirically: smaller zoom number => higher resolution (zoom=0 or 1 larger than zoom=3/4/5). Some items only have zoom=1 & zoom=5.
        We create variants removing decorative edge=curl and trying zoom values 0..5.
        We do NOT fetch here; selection layer may optionally HEAD request to confirm size later.
        """
        import re
        base = url
        # Normalize protocol
        if base.startswith('http:'):
            base = base.replace('http:', 'https:')
        # Remove existing zoom; we'll re-add
        base_no_zoom = re.sub(r'([&?])zoom=\d', r'\1', base)
        # Remove any duplicated && or ?& patterns
        base_no_zoom = base_no_zoom.replace('?&', '?').rstrip('&')
        # Remove edge=curl for raw variant
        base_no_edge = re.sub(r'(&|\?)edge=curl', r'\1', base_no_zoom)
        zoom_levels = [0,1,2,3,4,5]
        variants = []
        seen = set()
        for z in zoom_levels:
            sep = '&' if ('?' in base_no_edge and not base_no_edge.endswith('?')) else '?'
            variant = f"{base_no_edge}{sep}zoom={z}"
            # Ensure printsec=frontcover present for consistency
            if 'printsec=' not in variant:
                variant += '&printsec=frontcover'
            if 'img=1' not in variant:
                variant += '&img=1'
            # Basic cleanup of duplicate ampersands
            while '&&' in variant:
                variant = variant.replace('&&', '&')
            variant = variant.replace('?&', '?')
            if variant not in seen:
                variants.append({'provider': 'google', 'size': f'zoom{z}', 'url': variant})
                seen.add(variant)
        # Add original last if not already
        if url not in seen:
            variants.append({'provider': 'google', 'size': 'original', 'url': url})
        return variants
    try:
        gb_data = None
        if isbn:
            try:
                gb_data = get_google_books_cover(isbn, fetch_title_author=True)
            except Exception:
                gb_data = None
        if not gb_data and title:
            try:
                gb_results = search_google_books_by_title_author(title, author, limit=1)
                gb_data = gb_results[0] if gb_results else None
            except Exception:
                gb_data = None
        if gb_data:
            # Include each provided size first (ordered by presumed largest to smallest)
            order = ['extraLarge','large','medium','small','thumbnail','smallThumbnail']
            links_all = gb_data.get('image_links_all') or {}
            added_urls=set()
            for key in order:
                if key in links_all and links_all[key]:
                    u = links_all[key]
                    if u not in added_urls:
                        candidates.append({'provider':'google','size':key,'url':u})
                        added_urls.add(u)
            # Expand zoom variants from the largest available base
            base = gb_data.get('cover_url')
            if base:
                for variant in _expand_google_variants(base):
                    if variant['url'] not in added_urls:
                        candidates.append(variant)
                        added_urls.add(variant['url'])
        # OpenLibrary candidates
        ol_data = None
        if isbn:
            try:
                ol_data = fetch_book_data(isbn)
            except Exception:
                ol_data = None
        if not ol_data and title:
            try:
                ol_data = search_book_by_title_author(title, author)
            except Exception:
                ol_data = None
        if isinstance(ol_data, dict) and ol_data.get('cover_url'):
            candidates.append({'provider': 'openlibrary', 'size': 'L', 'url': ol_data['cover_url']})
    except Exception:
        pass
    _candidate_cache_set(isbn, title, author, candidates)
    return candidates

# USAGE INSTRUCTIONS:
# For all entry points (quick ISBN lookup, OCR, imports, title/author search, migration, reading log import),
# replace any direct cover selection logic with:
#   best_cover = get_best_cover_for_book(isbn=..., title=..., author=...)
#   cover_url = best_cover['cover_url']
from datetime import date, timedelta, datetime
import pytz
import calendar
from PIL import Image, ImageDraw, ImageFont
from io import BytesIO
import requests
import os
from flask import current_app

# Quiet logging by default; enable with VERBOSE=true or IMPORT_VERBOSE=true
_IMPORT_VERBOSE = (
    (os.getenv('VERBOSE') or 'false').lower() == 'true'
    or (os.getenv('IMPORT_VERBOSE') or 'false').lower() == 'true'
)

def _dprint(*args, **kwargs):
    if _IMPORT_VERBOSE:
        __builtins__.print(*args, **kwargs)

print = _dprint

def merge_book_metadata(original: dict, new: dict) -> dict:
    """Merge new metadata into original without degrading existing cover quality.

    Rules:
    - Skip empty incoming values.
    - For textual fields, keep longer (more informative) value.
    - For authors list, keep longer list.
    - cover_url: never replace with empty; only replace if heuristic says higher quality.
    """
    if not original:
        original = {}
    if not new:
        return original
    result = original.copy()
    for k, v in new.items():
        if v in (None, '', [], {}):
            continue
        ov = result.get(k)
        if ov in (None, '', [], {}):
            result[k] = v
            continue
        if k in ('title','subtitle','description'):
            if isinstance(v,str) and isinstance(ov,str) and len(v) > len(ov):
                result[k] = v
        elif k == 'authors':
            if isinstance(v, (list, tuple)) and isinstance(ov, (list, tuple)) and len(v) > len(ov):
                result[k] = v
        elif k == 'cover_url':
            if isinstance(v,str) and isinstance(ov,str):
                better_markers = ['extraLarge','zoom=1','zoom=0','large','800','1000','original']
                worse_markers = ['smallThumbnail','thumbnail','small','zoom=4','zoom=5','200']
                def _score(u:str):
                    s=0
                    for i,m in enumerate(better_markers):
                        if m in u: s += 100 - i
                    for m in worse_markers:
                        if m in u: s -= 10
                    s += len(u)//50  # slight preference for longer parameterized URLs (often higher res hints)
                    return s
                if _score(v) > _score(ov):
                    result[k] = v
        else:
            if isinstance(v,str) and isinstance(ov,str) and len(v) > len(ov)+10:
                result[k] = v
    return result

def search_author_by_name(author_name):
    """Search for authors on OpenLibrary by name and return the best match with most comprehensive data."""
    if not author_name:
        return None
    
    # OpenLibrary search API endpoint for authors
    url = f"https://openlibrary.org/search/authors.json?q={author_name}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        docs = data.get('docs', [])
        if not docs:
            return None
        
        # Find exact matches first and score them by comprehensiveness
        exact_matches = []
        close_matches = []
        
        for i, doc in enumerate(docs):
            name = doc.get('name', '')
            key = doc.get('key', '')
            author_id = key.replace('/authors/', '') if key.startswith('/authors/') else key
            # Calculate a scoring metric for matching
            score = 0
            
            # Calculate comprehensiveness score for this search result
            score = 0
            if doc.get('birth_date'):
                score += 10
            if doc.get('death_date'):
                score += 5
            if doc.get('alternate_names'):
                score += 8
            if doc.get('top_subjects'):
                score += 3
            work_count = doc.get('work_count', 0)
            if work_count > 0:
                score += min(work_count // 5, 10)  # Up to 10 points for work count
            
            result_data = {
                'name': name,
                'author_id': author_id,
                'doc': doc,
                'score': score
            }
            
            # Check for exact name match (case insensitive)
            if name.lower().strip() == author_name.lower().strip():
                exact_matches.append(result_data)
            elif author_name.lower() in name.lower() or name.lower() in author_name.lower():
                close_matches.append(result_data)
        
        # Sort exact matches by comprehensiveness score (highest first)
        exact_matches.sort(key=lambda x: x['score'], reverse=True)
        
        # Choose the best match from exact matches, or fall back to close matches
        candidates = exact_matches if exact_matches else close_matches
        if not candidates:
            # Fall back to first result if no good matches
            candidates = [{'name': docs[0].get('name', ''), 
                          'author_id': docs[0].get('key', '').replace('/authors/', ''),
                          'doc': docs[0], 'score': 0}]
        
        # Log scoring results
        if exact_matches:
            for match in exact_matches:
                print(f"  - {match['name']} ({match['author_id']}): {match['score']} points")
        
        # Try to get detailed data for the best candidate
        best_match = candidates[0]
        author_id = best_match['author_id']
        
        # Fetch detailed data for this author
        detailed_data = fetch_author_data(author_id)
        if detailed_data:
            detailed_data['openlibrary_id'] = author_id
            detailed_data['name'] = best_match['name']
            return detailed_data
        else:
            # Return enhanced basic info from search results if detailed fetch fails
            doc = best_match['doc']
            return {
                'openlibrary_id': author_id,
                'name': best_match['name'],
                'birth_date': doc.get('birth_date', ''),
                'death_date': doc.get('death_date', ''),
                'bio': None,
                'photo_url': None,
                'alternate_names': doc.get('alternate_names', []),
                'top_subjects': doc.get('top_subjects', [])
            }
        
        # If no exact match, return the first result as best match
        first_match = docs[0]
        name = first_match.get('name', '')
        key = first_match.get('key', '')
        author_id = key.replace('/authors/', '') if key.startswith('/authors/') else key
        
        detailed_data = fetch_author_data(author_id)
        if detailed_data:
            detailed_data['openlibrary_id'] = author_id
            detailed_data['name'] = name
            return detailed_data
        else:
            return {
                'openlibrary_id': author_id,
                'name': name,
                'birth_date': first_match.get('birth_date'),
                'death_date': first_match.get('death_date'),
                'bio': None,
                'photo_url': None
            }
            
    except Exception as e:
        return None

def fetch_book_data(isbn):
    """Enhanced OpenLibrary API lookup with comprehensive field mapping and timeout handling."""
    
    if not isbn:
        return None
        
    url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data"
    
    try:
        response = requests.get(url, timeout=15)  # Increased timeout
        print(f"📖 [OPENLIBRARY] Response status: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        
        print(f"📖 [OPENLIBRARY] Response data keys: {list(data.keys())}")
        
        book_key = f"ISBN:{isbn}"
        if book_key in data:
            print(f"📖 [OPENLIBRARY] Found book data for {book_key}")
            book = data[book_key]
            
            # Extract OpenLibrary ID from the key field
            openlibrary_id = None
            if 'key' in book:
                key = book['key']
                # Key format is typically "/books/OL12345M" - extract the ID part
                if key.startswith('/books/'):
                    openlibrary_id = key.replace('/books/', '')
            
            title = book.get('title', '')
            subtitle = book.get('subtitle', '')
            
            # Extract individual authors with IDs for better Person entity creation
            authors_list = []
            author_ids = []
            for author in book.get('authors', []):
                name = ''
                ol_id = ''
                if isinstance(author, dict):
                    name = author.get('name', '')
                    key = author.get('key', '')
                    if key:
                        ol_id = key.split('/')[-1]
                elif isinstance(author, str):
                    name = author
                if name:
                    authors_list.append(name)
                    author_ids.append(ol_id)
            
            # Keep backward compatibility with joined authors string
            authors = ', '.join(authors_list) if authors_list else ''
            
            # Enhanced cover image handling - get best quality available
            cover_data = book.get('cover', {})
            cover_url = None
            for size in ['large', 'medium', 'small']:
                if size in cover_data:
                    cover_url = cover_data[size]
                    break
            
            # Enhanced description handling
            description = ''
            
            # Try multiple sources for description
            desc_sources = ['description', 'notes', 'summary', 'excerpt']
            for source in desc_sources:
                if source in book:
                    desc_data = book.get(source)
                    print(f"📖 [OPENLIBRARY] Found {source}: {type(desc_data)} - {str(desc_data)[:100] if desc_data else 'None'}...")
                    
                    if isinstance(desc_data, dict):
                        if 'value' in desc_data:
                            description = desc_data['value']
                            print(f"📖 [OPENLIBRARY] Using description from {source}.value")
                            break
                        elif 'text' in desc_data:
                            description = desc_data['text']
                            print(f"📖 [OPENLIBRARY] Using description from {source}.text")
                            break
                    elif isinstance(desc_data, str) and desc_data.strip():
                        description = desc_data.strip()
                        print(f"📖 [OPENLIBRARY] Using description from {source}")
                        break
            
            if not description:
                print(f"📖 [OPENLIBRARY] No description found in any source")
            else:
                print(f"📖 [OPENLIBRARY] Final description: {description[:100]}...")
            
            # Publication info
            published_date = book.get('publish_date', '')
            page_count = book.get('number_of_pages')
            
            # Enhanced subjects/categories processing with better filtering
            subjects = book.get('subjects', [])
            categories = []
            # Filter out overly generic or unhelpful categories
            exclude_categories = {
                'accessible book', 'protected daisy', 'lending library',
                'in library', 'fiction', 'non-fiction', 'literature',
                'reading level-adult', 'adult', 'juvenile', 'young adult',
                'large type books', 'large print books'
            }
            
            for subject in subjects[:15]:  # Limit to 15 most relevant categories
                category_name = ''
                if isinstance(subject, dict):
                    category_name = subject.get('name', '')
                else:
                    category_name = str(subject)
                
                # Clean and filter categories
                category_name = category_name.strip().lower()
                if (category_name and 
                    len(category_name) > 2 and 
                    category_name not in exclude_categories and
                    not category_name.startswith('places--') and
                    not category_name.startswith('people--')):
                    # Capitalize properly
                    categories.append(category_name.title())
            
            # Remove duplicates while preserving order
            seen = set()
            unique_categories = []
            for cat in categories:
                if cat.lower() not in seen:
                    unique_categories.append(cat)
                    seen.add(cat.lower())
            
            # Language extraction
            languages = book.get('languages', [])
            language = ''
            if languages and len(languages) > 0:
                lang_item = languages[0]
                if isinstance(lang_item, dict):
                    language = lang_item.get('key', '').split('/')[-1]
                else:
                    language = str(lang_item)
            
            # ISBN extraction from identifiers
            identifiers = book.get('identifiers', {})
            isbn_10 = identifiers.get('isbn_10', [])
            isbn_13 = identifiers.get('isbn_13', [])
            
            # Get the first available ISBN
            primary_isbn = isbn
            if isbn_13 and len(isbn_13) > 0:
                primary_isbn = isbn_13[0]
            elif isbn_10 and len(isbn_10) > 0:
                primary_isbn = isbn_10[0]
            
            # Publisher information
            publishers = book.get('publishers', [])
            publisher = ''
            if publishers and len(publishers) > 0:
                pub = publishers[0]
                if isinstance(pub, dict):
                    publisher = pub.get('name', '')
                else:
                    publisher = str(pub)
            
            result = {
                'title': title,
                'subtitle': subtitle,
                'authors': authors,  # Backward compatibility
                'authors_list': authors_list,  # New field for individual authors
                'author_ids': author_ids,  # OpenLibrary IDs for authors
                'description': description,
                'published_date': published_date,
                'page_count': page_count,
                'categories': unique_categories,
                'cover_url': cover_url,
                'language': language,
                'isbn': primary_isbn,
                'publisher': publisher,
                'openlibrary_id': openlibrary_id,
                'source': 'OpenLibrary'
            }
            
            print(f"✅ [OPENLIBRARY] Successfully retrieved data for ISBN {isbn}:")
            print(f"    openlibrary_id='{openlibrary_id}'")
            print(f"    title='{title}'")
            print(f"    authors={len(authors_list)} items")
            print(f"    description='{description[:100] if description else None}...'")
            print(f"    cover_url='{cover_url}'")
            print(f"    categories={len(unique_categories)} items")
            return result
        else:
            print(f"❌ [OPENLIBRARY] No book data found for ISBN {isbn}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"❌ [OPENLIBRARY] Request error for ISBN {isbn}: {e}")
        return None
    except Exception as e:
        print(f"❌ [OPENLIBRARY] Unexpected error for ISBN {isbn}: {e}")
        return None

def fetch_author_data(author_id):
    """Fetch detailed author information from OpenLibrary API using author ID."""
    if not author_id:
        print(f"[OPENLIBRARY] No author ID provided")
        return None
        
    url = f"https://openlibrary.org/authors/{author_id}.json"
    print(f"[OPENLIBRARY] Fetching author data from: {url}")
    try:
        response = requests.get(url, timeout=10)
        print(f"[OPENLIBRARY] Response status: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        print(f"[OPENLIBRARY] Raw response data: {data}")
        
        # Extract author information
        name = data.get('name', '')
        birth_date = data.get('birth_date', '')
        death_date = data.get('death_date', '')
        bio = data.get('bio', '')
        
        # Handle bio if it's a dict
        if isinstance(bio, dict):
            bio = bio.get('value', '')
        
        # Extract photo URL
        photos = data.get('photos', [])
        photo_url = None
        if photos and len(photos) > 0:
            photo_id = photos[0]
            photo_url = f"https://covers.openlibrary.org/a/id/{photo_id}-L.jpg"
        
        # Extract alternate names
        alternate_names = data.get('alternate_names', [])
        
        # Extract Wikipedia link
        links = data.get('links', [])
        wikipedia_url = None
        for link in links:
            if isinstance(link, dict):
                url_val = link.get('url', '')
                if 'wikipedia.org' in url_val:
                    wikipedia_url = url_val
                    break
        
        result = {
            'name': name,
            'birth_date': birth_date,
            'death_date': death_date,
            'bio': bio,
            'photo_url': photo_url,
            'alternate_names': alternate_names,
            'wikipedia_url': wikipedia_url,
            'openlibrary_id': author_id,
            'source': 'OpenLibrary'
        }
        print(f"[OPENLIBRARY] Processed author data: {result}")
        return result
        
    except requests.exceptions.RequestException as e:
        print(f"[OPENLIBRARY] Error fetching author data from OpenLibrary for ID {author_id}: {e}")
        return None
    except Exception as e:
        print(f"[OPENLIBRARY] Unexpected error processing OpenLibrary author data for ID {author_id}: {e}")
        return None

def get_google_books_cover(isbn, fetch_title_author=False):
    """
    Fetch cover image from Google Books API using ISBN.
    
    Args:
        isbn (str): The ISBN to search for
        fetch_title_author (bool): If True, also return title and author info
    
    Returns:
        dict: Contains cover_url and optionally title/author info
    """
    print(f"📚 [GOOGLE_BOOKS] Fetching data for ISBN: {isbn}")
    
    if not isbn:
        print(f"❌ [GOOGLE_BOOKS] No ISBN provided")
        return None
        
    url = f"https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}"
    print(f"📚 [GOOGLE_BOOKS] Request URL: {url}")
    
    # Cached metadata short‑circuit
    cache_key = ('google_isbn', isbn, fetch_title_author)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        response = requests.get(url, timeout=3.5)
        print(f"📚 [GOOGLE_BOOKS] Response status: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        
        print(f"📚 [GOOGLE_BOOKS] Response data keys: {list(data.keys())}")
        
        if 'items' in data and len(data['items']) > 0:
            print(f"📚 [GOOGLE_BOOKS] Found {len(data['items'])} items")
            book_item = data['items'][0]
            book_info = book_item['volumeInfo']
            google_books_id = book_item.get('id', '')
            
            result = {}
            
            # Store Google Books ID
            result['google_books_id'] = google_books_id
            
            # Get cover image
            image_links = book_info.get('imageLinks', {})
            cover_url = None
            
            # Prefer higher quality images
            for size in ['extraLarge', 'large', 'medium', 'small', 'thumbnail', 'smallThumbnail']:
                if size in image_links:
                    cover_url = image_links[size]
                    # Convert to HTTPS if needed
                    if cover_url and cover_url.startswith('http:'):
                        cover_url = cover_url.replace('http:', 'https:')
                    break
            
            result['cover_url'] = cover_url
            # Legacy zoom/printsec/img param forcing removed. Central normalization now handled
            # by normalize_cover_url and upgrade_google_cover_url elsewhere in the pipeline.
            # Keep all image links for variant expansion later
            if image_links:
                # Normalize all to https
                normalized_links = {}
                for k,v in image_links.items():
                    if isinstance(v,str) and v.startswith('http:'):
                        normalized_links[k] = v.replace('http:','https:')
                    else:
                        normalized_links[k] = v
                result['image_links_all'] = normalized_links
            
            if fetch_title_author:
                # Get title and author information
                title = book_info.get('title', '')
                subtitle = book_info.get('subtitle', '')
                authors = book_info.get('authors', [])
                
                # Get description
                description = book_info.get('description', '')
                
                # Get categories/genres
                categories = book_info.get('categories', [])
                
                # Get additional contributors
                contributors = []
                
                # Google Books sometimes has contributors in different fields
                if 'authors' in book_info and book_info['authors']:
                    for author in book_info['authors']:
                        contributors.append({'name': author, 'role': 'author'})
                
                # Check for other contributor types that might be in the data
                if 'editors' in book_info and book_info['editors']:
                    for editor in book_info['editors']:
                        contributors.append({'name': editor, 'role': 'editor'})
                
                if 'translators' in book_info and book_info['translators']:
                    for translator in book_info['translators']:
                        contributors.append({'name': translator, 'role': 'translator'})
                
                # Get publisher and publication date
                publisher = book_info.get('publisher', '')
                published_date = book_info.get('publishedDate', '')
                
                # Get page count
                page_count = book_info.get('pageCount')
                
                # Get language
                language = book_info.get('language', 'en')
                
                # Get average rating and rating count
                average_rating = book_info.get('averageRating')
                rating_count = book_info.get('ratingsCount')
                
                # Get ISBN data from industryIdentifiers
                isbn_10 = None
                isbn_13 = None
                if 'industryIdentifiers' in book_info:
                    for identifier in book_info['industryIdentifiers']:
                        if identifier.get('type') == 'ISBN_10':
                            isbn_10 = identifier.get('identifier')
                        elif identifier.get('type') == 'ISBN_13':
                            isbn_13 = identifier.get('identifier')
                
                result.update({
                    'title': title,
                    'subtitle': subtitle,
                    'authors': ', '.join(authors) if authors else '',
                    'authors_list': authors,
                    'description': description,
                    'categories': categories,
                    'contributors': contributors,
                    'publisher': publisher,
                    'published_date': published_date,
                    'page_count': page_count,
                    'language': language,
                    'average_rating': average_rating,
                    'rating_count': rating_count,
                    'isbn_10': isbn_10,
                    'isbn_13': isbn_13,
                    'source': 'Google Books'
                })
                
                print(f"✅ [GOOGLE_BOOKS] Enhanced data for ISBN {isbn}:")
                print(f"    google_books_id='{google_books_id}'")
                print(f"    title='{title}'")
                print(f"    authors={len(authors)} items")
                print(f"    description='{description[:100] if description else None}...'")
                print(f"    categories={len(categories)} items")
                print(f"    contributors={len(contributors)} items")
                print(f"    publisher='{publisher}'")
            
            print(f"✅ [GOOGLE_BOOKS] Successfully retrieved data for ISBN {isbn}: {list(result.keys())}")
            _cache_set(cache_key, result)
            return result
        else:
            print(f"❌ [GOOGLE_BOOKS] No items found for ISBN {isbn}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ [GOOGLE_BOOKS] Request error for ISBN {isbn}: {e}")
        return None
    except Exception as e:
        print(f"❌ [GOOGLE_BOOKS] Unexpected error for ISBN {isbn}: {e}")
        return None

def generate_month_review_image(books, month, year):
    """
    Generate a monthly reading review image showing books read in the given month.
    """
    if not books:
        return None
    
    # Image dimensions
    width = 1200
    height = 800
    bg_color = (245, 245, 245)  # Light gray background
    
    # Create image
    img = Image.new('RGB', (width, height), bg_color)
    draw = ImageDraw.Draw(img)
    
    # Try to load a font
    try:
        title_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 36)
        book_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 20)
        stat_font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 24)
    except (OSError, IOError):
        # Fallback to default font if custom fonts aren't available
        title_font = ImageFont.load_default()
        book_font = ImageFont.load_default()
        stat_font = ImageFont.load_default()
    
    # Colors
    title_color = (50, 50, 50)
    text_color = (80, 80, 80)
    accent_color = (100, 150, 200)
    
    # Title
    month_name = calendar.month_name[month]
    title = f"Reading Review - {month_name} {year}"
    
    # Get title dimensions for centering
    title_bbox = draw.textbbox((0, 0), title, font=title_font)
    title_width = title_bbox[2] - title_bbox[0]
    title_x = (width - title_width) // 2
    
    draw.text((title_x, 40), title, fill=title_color, font=title_font)
    
    # Statistics
    total_books = len(books)
    total_pages = sum(book.get('page_count', 0) for book in books if book.get('page_count'))
    
    stats_y = 120
    stats_text = f"Books Read: {total_books}    Total Pages: {total_pages:,}"
    stats_bbox = draw.textbbox((0, 0), stats_text, font=stat_font)
    stats_width = stats_bbox[2] - stats_bbox[0]
    stats_x = (width - stats_width) // 2
    
    draw.text((stats_x, stats_y), stats_text, fill=accent_color, font=stat_font)
    
    # Book list
    book_list_y = 200
    max_books_to_show = 15  # Limit to prevent overcrowding
    books_to_show = books[:max_books_to_show]
    
    for i, book in enumerate(books_to_show):
        y_pos = book_list_y + (i * 35)
        
        if y_pos > height - 100:  # Leave space at bottom
            remaining = len(books) - i
            if remaining > 0:
                more_text = f"... and {remaining} more books"
                draw.text((50, y_pos), more_text, fill=text_color, font=book_font)
            break
        
        # Book title and author
        title = book.get('title', 'Unknown Title')
        authors = book.get('authors', 'Unknown Author')
        page_count = book.get('page_count', 0)
        
        # Truncate long titles
        if len(title) > 50:
            title = title[:47] + "..."
        
        book_text = f"• {title}"
        if authors and authors != 'Unknown Author':
            if len(authors) > 30:
                authors = authors[:27] + "..."
            book_text += f" by {authors}"
        
        if page_count > 0:
            book_text += f" ({page_count} pages)"
        
        draw.text((50, y_pos), book_text, fill=text_color, font=book_font)
    
    # Convert to bytes for return
    img_buffer = BytesIO()
    img.save(img_buffer, format='PNG', quality=95)
    img_buffer.seek(0)
    
    return img_buffer


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
    
    # Search OpenLibrary first
    print(f"[MULTI_API] Searching OpenLibrary for: '{title}' by '{author}'")
    try:
        ol_results = _search_openlibrary_multiple(title, author, limit//2)
        if ol_results:
            all_results.extend(ol_results)
            print(f"[MULTI_API] OpenLibrary returned {len(ol_results)} results")
    except Exception as e:
        print(f"[MULTI_API] OpenLibrary search failed: {e}")
    
    # Search Google Books
    print(f"[MULTI_API] Searching Google Books for: '{title}' by '{author}'")
    try:
        gb_results = search_google_books_by_title_author(title, author, limit//2)
        if gb_results:
            all_results.extend(gb_results)
            print(f"[MULTI_API] Google Books returned {len(gb_results)} results")
    except Exception as e:
        print(f"[MULTI_API] Google Books search failed: {e}")
    
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
    if not title:
        print(f"[OPENLIBRARY] No title provided for book search")
        return []
    
    # Build search query
    query_parts = [title]
    if author:
        query_parts.append(author)
    
    query = ' '.join(query_parts)
    url = f"https://openlibrary.org/search.json?q={query}&limit={limit}"
    
    print(f"[OPENLIBRARY] Searching for multiple books: title='{title}', author='{author}' at {url}")
    
    try:
        response = requests.get(url, timeout=15)
        print(f"[OPENLIBRARY] Multiple book search response status: {response.status_code}")
        
        # Handle different response codes more gracefully
        if response.status_code == 404:
            print(f"[OPENLIBRARY] OpenLibrary API returned 404 for query: {query}")
            return []
        elif response.status_code != 200:
            print(f"[OPENLIBRARY] OpenLibrary API returned {response.status_code}")
            return []
            
        data = response.json()
        
        docs = data.get('docs', [])
        if not docs:
            print(f"[OPENLIBRARY] No search results found for '{title}' by '{author}'")
            return []
            
        print(f"[OPENLIBRARY] Found {len(docs)} book search results")
        
        # Score matches based on title similarity and author match
        scored_matches = []
        
        for i, doc in enumerate(docs):
            doc_title = doc.get('title', '')
            doc_authors = doc.get('author_name', []) if isinstance(doc.get('author_name'), list) else [doc.get('author_name', '')]
            doc_isbn = doc.get('isbn', [])
            
            # Get best ISBN (prefer 13-digit)
            best_isbn = None
            if doc_isbn:
                for isbn in doc_isbn:
                    if len(isbn) == 13:
                        best_isbn = isbn
                        break
                if not best_isbn and doc_isbn:
                    best_isbn = doc_isbn[0]
            
            print(f"[OPENLIBRARY] Result {i}: title='{doc_title}', authors={doc_authors}, isbn={best_isbn}")
            
            # Calculate match score
            score = 0
            
            # Title similarity (basic)
            if title.lower() in doc_title.lower() or doc_title.lower() in title.lower():
                score += 50
            if title.lower().strip() == doc_title.lower().strip():
                score += 50  # Exact title match bonus
            
            # Author similarity
            if author:
                for doc_author in doc_authors:
                    if doc_author and author.lower() in doc_author.lower():
                        score += 30
                    if doc_author and author.lower().strip() == doc_author.lower().strip():
                        score += 20  # Exact author match bonus
            
            # Prefer results with ISBN
            if best_isbn:
                score += 10
            
            # Prefer more recent publications (if available)
            if doc.get('first_publish_year'):
                try:
                    year = int(doc.get('first_publish_year'))
                    if year > 1950:  # Reasonable cutoff
                        score += min((year - 1950) // 10, 5)  # Up to 5 bonus points for newer books
                except:
                    pass
            
            # Build book data from search result
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
                'description': '',  # Search results don't include descriptions
                'language': doc.get('language', [''])[0] if doc.get('language') else 'en',
                'openlibrary_id': doc.get('key', '').replace('/works/', '') if doc.get('key') else None,
                'categories': [],
                'score': score
            }
            
            # Try to get cover image if we have an OpenLibrary work ID
            if result.get('openlibrary_id'):
                cover_url = f"https://covers.openlibrary.org/w/id/{result['openlibrary_id']}-L.jpg"
                try:
                    cover_response = requests.head(cover_url, timeout=5)
                    if cover_response.status_code == 200:
                        result['cover'] = cover_url
                        result['cover_url'] = cover_url
                except:
                    pass
            
            scored_matches.append(result)
        
        # Sort by score (highest first)
        scored_matches.sort(key=lambda x: x['score'], reverse=True)
        
        print(f"[OPENLIBRARY] Returning {len(scored_matches)} book results")
        return scored_matches
        
    except Exception as e:
        print(f"[OPENLIBRARY] Failed to search for multiple books '{title}' by '{author}': {e}")
        return []


def search_book_by_title_author(title, author=None):
    """Search for books on OpenLibrary by title and optionally author, return the best match."""
    if not title:
        print(f"[OPENLIBRARY] No title provided for book search")
        return None
    
    # Build search query
    query_parts = [title]
    if author:
        query_parts.append(author)
    
    query = ' '.join(query_parts)
    url = f"https://openlibrary.org/search.json?q={query}&limit=10"
    
    print(f"[OPENLIBRARY] Searching for book: title='{title}', author='{author}' at {url}")
    
    try:
        response = requests.get(url, timeout=15)
        print(f"[OPENLIBRARY] Book search response status: {response.status_code}")
        
        # Handle different response codes more gracefully
        if response.status_code == 404:
            print(f"[OPENLIBRARY] OpenLibrary API returned 404 for query: {query}")
            return None
        elif response.status_code != 200:
            print(f"[OPENLIBRARY] OpenLibrary API returned {response.status_code}")
            return None
            
        data = response.json()
        
        docs = data.get('docs', [])
        if not docs:
            print(f"[OPENLIBRARY] No search results found for '{title}' by '{author}'")
            return None
            
        print(f"[OPENLIBRARY] Found {len(docs)} book search results")
        
        # Score matches based on title similarity and author match
        scored_matches = []
        
        for i, doc in enumerate(docs):
            doc_title = doc.get('title', '')
            doc_authors = doc.get('author_name', []) if isinstance(doc.get('author_name'), list) else [doc.get('author_name', '')]
            doc_isbn = doc.get('isbn', [])
            
            # Get best ISBN (prefer 13-digit)
            best_isbn = None
            if doc_isbn:
                for isbn in doc_isbn:
                    if len(isbn) == 13:
                        best_isbn = isbn
                        break
                if not best_isbn and doc_isbn:
                    best_isbn = doc_isbn[0]
            
            print(f"[OPENLIBRARY] Result {i}: title='{doc_title}', authors={doc_authors}, isbn={best_isbn}")
            
            # Calculate match score
            score = 0
            
            # Title similarity (basic)
            if title.lower() in doc_title.lower() or doc_title.lower() in title.lower():
                score += 50
            if title.lower().strip() == doc_title.lower().strip():
                score += 50  # Exact title match bonus
            
            # Author similarity
            if author:
                for doc_author in doc_authors:
                    if doc_author and author.lower() in doc_author.lower():
                        score += 30
                    if doc_author and author.lower().strip() == doc_author.lower().strip():
                        score += 20  # Exact author match bonus
            
            # Prefer results with ISBN
            if best_isbn:
                score += 10
            
            # Prefer more recent publications (if available)
            if doc.get('first_publish_year'):
                try:
                    year = int(doc.get('first_publish_year'))
                    if year > 1950:  # Reasonable cutoff
                        score += min((year - 1950) // 10, 5)  # Up to 5 bonus points for newer books
                except:
                    pass
            
            scored_matches.append({
                'doc': doc,
                'score': score,
                'title': doc_title,
                'authors': doc_authors,
                'isbn': best_isbn
            })
        
        # Sort by score (highest first)
        scored_matches.sort(key=lambda x: x['score'], reverse=True)
        
        if scored_matches:
            best_match = scored_matches[0]
            print(f"[OPENLIBRARY] Best match: '{best_match['title']}' by {best_match['authors']} (score: {best_match['score']}, ISBN: {best_match['isbn']})")
            
            # If we have an ISBN, fetch full book data using existing function
            if best_match['isbn']:
                print(f"[OPENLIBRARY] Fetching full data using ISBN: {best_match['isbn']}")
                full_data = fetch_book_data(best_match['isbn'])
                if full_data:
                    return full_data
            
            # Otherwise, build book data from search result
            doc = best_match['doc']
            result = {
                'title': best_match['title'],
                'author': ', '.join(best_match['authors']) if best_match['authors'] else '',
                'authors_list': best_match['authors'],
                'isbn': best_match['isbn'],
                'publisher': ', '.join(doc.get('publisher', [])) if isinstance(doc.get('publisher'), list) else doc.get('publisher', ''),
                'published_date': str(doc.get('first_publish_year', '')) if doc.get('first_publish_year') else '',
                'page_count': doc.get('number_of_pages_median'),
                'cover': None,  # Search results don't include cover URLs
                'description': '',  # Search results don't include descriptions
                'language': doc.get('language', [''])[0] if doc.get('language') else 'en',
                'openlibrary_id': doc.get('key', '').replace('/works/', '') if doc.get('key') else None
            }
            
            # Try to get cover image if we have an OpenLibrary work ID
            if result.get('openlibrary_id'):
                cover_url = f"https://covers.openlibrary.org/w/id/{result['openlibrary_id']}-L.jpg"
                try:
                    cover_response = requests.head(cover_url, timeout=5)
                    if cover_response.status_code == 200:
                        result['cover'] = cover_url
                        result['cover_url'] = cover_url
                except:
                    pass
            
            print(f"[OPENLIBRARY] Returning book data: {result}")
            return result
        
    except Exception as e:
        print(f"[OPENLIBRARY] Failed to search for book '{title}' by '{author}': {e}")
        return None
    
    return None


def search_google_books_by_title_author(title, author=None, limit=10):
    """Search Google Books API by title and optionally author."""
    import os as _os
    _VERBOSE = (
        (_os.getenv('VERBOSE') or 'false').lower() == 'true'
        or (_os.getenv('IMPORT_VERBOSE') or 'false').lower() == 'true'
        or (_os.getenv('COVER_VERBOSE') or 'false').lower() == 'true'
    )
    if not title:
        if _VERBOSE:
            print(f"[GOOGLE_BOOKS] No title provided for book search")
        return []
    
    # Build search query for Google Books
    query_parts = [f'intitle:"{title}"']
    if author:
        query_parts.append(f'inauthor:"{author}"')
    
    query = '+'.join(query_parts)
    url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults={min(limit, 40)}"
    
    if _VERBOSE:
        print(f"[GOOGLE_BOOKS] Searching for multiple books: title='{title}', author='{author}' at {url}")
    
    try:
        response = requests.get(url, timeout=10)
        if _VERBOSE:
            print(f"[GOOGLE_BOOKS] Multiple book search response status: {response.status_code}")
        
        if response.status_code != 200:
            if _VERBOSE:
                print(f"[GOOGLE_BOOKS] Google Books API returned {response.status_code}")
            return []
            
        data = response.json()
        
        items = data.get('items', [])
        if not items:
            if _VERBOSE:
                print(f"[GOOGLE_BOOKS] No search results found for '{title}' by '{author}'")
            return []
            
        if _VERBOSE:
            print(f"[GOOGLE_BOOKS] Found {len(items)} book search results")

        
        from .book_utils import select_highest_google_image, upgrade_google_cover_url as _upgrade
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
                
                # Get ISBNs
                isbn13 = None
                isbn10 = None
                industry_identifiers = volume_info.get('industryIdentifiers', [])
                for identifier in industry_identifiers:
                    if identifier.get('type') == 'ISBN_13':
                        isbn13 = identifier.get('identifier')
                    elif identifier.get('type') == 'ISBN_10':
                        isbn10 = identifier.get('identifier')
                
                # Get cover image
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
                    'isbn': isbn13 or isbn10,  # Prefer ISBN13
                    'cover_url': cover_url,
                    'cover': cover_url,
                    'language': book_language,
                    'categories': book_categories,
                    'google_books_id': item.get('id'),
                    'source': 'Google Books'
                }
                
                if _VERBOSE:
                    print(f"[GOOGLE_BOOKS] Result {i}: title='{book_title}', authors={book_authors}, isbn={isbn13 or isbn10}")
                
                # Only add if we have at least a title
                if result['title']:
                    results.append(result)
                
                # Limit results
                if len(results) >= limit:
                    break
                    
            except Exception as item_error:
                if _VERBOSE:
                    print(f"[GOOGLE_BOOKS] Error processing item {i}: {item_error}")
                continue

        if _VERBOSE:
            print(f"[GOOGLE_BOOKS] Returning {len(results)} valid results")
        return results

    except Exception as e:
        # Ensure _VERBOSE defined
        if _VERBOSE:
            print(f"[GOOGLE_BOOKS] Failed to search for book '{title}' by '{author}': {e}")
        return []
