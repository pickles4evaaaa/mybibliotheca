"""
Audiobookshelf Import Service

Orchestrates a small import from ABS to MyBibliotheca using existing book services.
Implements a test sync (N items) to validate mapping and cover caching.
"""
from __future__ import annotations

from typing import Dict, Any, List, Optional
import uuid
import threading
import traceback
from datetime import datetime, timezone

from app.services.audiobookshelf_service import AudiobookShelfClient
from app.simplified_book_service import SimplifiedBookService, SimplifiedBook, BookAlreadyExistsError
from app.services.kuzu_book_service import KuzuBookService
from app.utils.safe_import_manager import safe_create_import_job, safe_update_import_job, safe_get_import_job
from pathlib import Path
import os
import requests  # type: ignore


class AudiobookshelfImportService:
    """Import audiobooks from ABS libraries into Books as media_type='audiobook'."""

    def __init__(self, user_id: str, client: AudiobookShelfClient):
        self.user_id = user_id
        self.client = client
        self.simple_books = SimplifiedBookService()
        self.kuzu_book_service = KuzuBookService(user_id=user_id)

    def _map_abs_item_to_simplified(self, item: Dict[str, Any]) -> SimplifiedBook:
        """Map ABS item JSON to SimplifiedBook structure (robust fields)."""
        media = item.get('media') or {}
        md = media.get('metadata') or {}

        # Authors and narrators can be arrays in md
        author = ''
        addl: List[str] = []
        narrators: List[str] = []
        if isinstance(md.get('authors'), list) and md['authors']:
            first = md['authors'][0]
            if isinstance(first, dict):
                author = str(first.get('name') or first.get('title') or '').strip()
            else:
                author = str(first).strip()
            for a in md['authors'][1:]:
                if isinstance(a, dict):
                    name = a.get('name') or a.get('title')
                    if name:
                        addl.append(str(name).strip())
                elif a:
                    addl.append(str(a).strip())
        elif isinstance(md.get('author'), str):
            author = md.get('author') or ''

        # Narrators
        if isinstance(md.get('narrators'), list):
            for n in md['narrators']:
                if not n:
                    continue
                if isinstance(n, dict):
                    nname = n.get('name') or n.get('title')
                    if nname:
                        narrators.append(str(nname).strip())
                else:
                    narrators.append(str(n).strip())
        elif isinstance(md.get('narrator'), str) and md['narrator']:
            narrators = [md['narrator']]

        title = md.get('title') or item.get('title') or 'Untitled'
        subtitle = md.get('subtitle') or None
        description = md.get('description') or ''
        language = (md.get('language') or 'en')

        # Series: Only pull from ABS series object; no subtitle or heuristic parsing
        series: Optional[str] = None
        series_order: Optional[int] = None
        series_volume: Optional[str] = None

        def _to_int(val) -> Optional[int]:
            try:
                if isinstance(val, bool):
                    return None
                if isinstance(val, int):
                    return val
                if isinstance(val, float):
                    return int(val)
                if isinstance(val, str) and val.strip().isdigit():
                    return int(val.strip())
                return None
            except Exception:
                return None

        # Prefer series from item if provided, fallback to media.metadata.series
        s = item.get('series') if isinstance(item.get('series'), (dict, list, str)) else md.get('series')
        # If array, take first entry
        if isinstance(s, list) and s:
            s = s[0]
        if isinstance(s, dict):
            series = (s.get('name') or s.get('title') or s.get('seriesName') or None)
            cand_order = (
                s.get('sequence') or s.get('sequenceNumber') or s.get('number') or s.get('index') or s.get('position')
            )
            series_order = _to_int(cand_order)
            # If sequence is present, reflect it as volume string too for UI consistency
            if series_order is not None and not series_volume:
                series_volume = str(series_order)
            # Prefer explicit volume if ABS provides one in object
            if s.get('volume') is not None and not series_volume:
                series_volume = str(s.get('volume'))
            if s.get('volumeNumber') is not None and not series_volume:
                series_volume = str(s.get('volumeNumber'))
        elif isinstance(s, str):
            # Use the series name only; don't derive order from it
            series = s.strip() or None
        else:
            # Alternate field names used by some ABS versions
            alt_name = item.get('seriesName') or md.get('seriesName') or md.get('series_name')
            if isinstance(alt_name, str) and alt_name.strip():
                series = alt_name.strip()
            # Order candidates on item or metadata
            cand_order = (
                item.get('seriesSequence') or item.get('bookNumber') or
                md.get('seriesSequence') or md.get('seriesIndex') or md.get('seriesPosition') or md.get('sequenceNumber') or md.get('bookNumber')
            )
            if series_order is None:
                series_order = _to_int(cand_order)
            if not series_volume:
                cand_vol = md.get('seriesVolume') or md.get('volume') or md.get('volumeNumber') or item.get('bookNumber')
                if cand_vol is not None:
                    series_volume = str(cand_vol)

        asin = md.get('asin') or None
        isbn13 = md.get('isbn13') or md.get('isbn') or None
        isbn10 = md.get('isbn10') or None

        # Duration
        duration_ms = None
        duration = media.get('duration') or md.get('duration')
        if isinstance(duration, (int, float)):
            duration_ms = int(float(duration) * 1000) if duration < 10_000_000 else int(duration)

        # Cover
        cover_url = None
        cover_path = (
            item.get('coverPath') or media.get('coverPath') or md.get('coverPath') or
            item.get('imagePath') or md.get('imagePath') or item.get('posterPath') or md.get('posterPath') or None
        )
        if cover_path:
            cover_url = self.client.build_cover_url(cover_path)
        else:
            item_id = item.get('id') or item.get('_id') or item.get('itemId')
            if item_id:
                cover_url = self.client._url(f"/api/items/{item_id}/cover?width=600")

        simplified = SimplifiedBook(
            title=title,
            author=author or 'Unknown',
            subtitle=subtitle,
            description=description,
            language=language,
            series=series,
            series_order=series_order,
            series_volume=series_volume,
            asin=asin,
            isbn13=isbn13,
            isbn10=isbn10,
            cover_url=cover_url,
        )
        if addl:
            simplified.additional_authors = ', '.join(addl)
        if narrators:
            simplified.narrator = ', '.join(narrators)
        return simplified

    def _extract_categories(self, item: Dict[str, Any]) -> List[str]:
        media = item.get('media') or {}
        md = media.get('metadata') or {}
        out: List[str] = []
        for key in ('genres', 'tags', 'categories'):
            val = md.get(key) or item.get(key)
            if isinstance(val, list):
                out.extend([str(v).strip() for v in val if v])
            elif isinstance(val, str) and val.strip():
                # Comma or semicolon separated
                parts = [p.strip() for p in val.replace(';', ',').split(',') if p.strip()]
                out.extend(parts)
        # de-dup preserve order
        seen = set()
        uniq = []
        for c in out:
            cl = c.lower()
            if cl not in seen:
                seen.add(cl)
                uniq.append(c)
        return uniq

    def _resolve_book_id(self, book_data: SimplifiedBook) -> Optional[str]:
        """Best-effort resolve of created book's id using ISBN or title/author."""
        # Prefer ISBNs
        isbn13 = getattr(book_data, 'isbn13', None)
        if isinstance(isbn13, str) and isbn13.strip():
            b = self.kuzu_book_service.get_book_by_isbn_sync(isbn13)
            if b and getattr(b, 'id', None):
                return b.id
        isbn10 = getattr(book_data, 'isbn10', None)
        if isinstance(isbn10, str) and isbn10.strip():
            b = self.kuzu_book_service.get_book_by_isbn_sync(isbn10)
            if b and getattr(b, 'id', None):
                return b.id
        # Fallback: title + main author exact (case-insensitive)
        try:
            from app.infrastructure.kuzu_graph import safe_execute_kuzu_query
            from app.services.kuzu_service_facade import _convert_query_result_to_list
            title = (book_data.title or '').strip()
            author = (book_data.author or '').strip()
            if not title:
                return None
            if author:
                qr = safe_execute_kuzu_query(
                    """
                    MATCH (b:Book) WHERE toLower(b.title) = toLower($title)
                    MATCH (p:Person)-[r:AUTHORED]->(b)
                    WHERE toLower(p.name) = toLower($author)
                    RETURN b.id
                    LIMIT 1
                    """,
                    {'title': title, 'author': author}
                )
            else:
                qr = safe_execute_kuzu_query(
                    """
                    MATCH (b:Book) WHERE toLower(b.title) = toLower($title)
                    RETURN b.id
                    LIMIT 1
                    """,
                    {'title': title}
                )
            rows = _convert_query_result_to_list(qr)
            if rows:
                r0 = rows[0]
                return r0.get('result') or r0.get('col_0')
        except Exception:
            return None
        return None

    def _cache_cover_with_auth(self, item: Dict[str, Any]) -> Optional[str]:
        """Download ABS cover with auth headers and store under /covers; return local URL.

        This avoids unauthorized errors when ABS requires Authorization for image fetches.
        """
        try:
            # Determine cover URL similar to mapping logic
            media = item.get('media') or {}
            md = media.get('metadata') or {}
            cover_path = (
                item.get('coverPath') or media.get('coverPath') or md.get('coverPath') or
                item.get('imagePath') or md.get('imagePath') or item.get('posterPath') or md.get('posterPath') or None
            )
            item_id = item.get('id') or item.get('_id') or item.get('itemId')
            candidate_urls: List[str] = []
            # 1) Direct coverPath if present
            if cover_path:
                built = self.client.build_cover_url(cover_path)
                if built:
                    candidate_urls.append(built)
            # 2) Common ABS cover endpoints
            if item_id:
                candidate_urls.append(self.client._url(f"/api/items/{item_id}/cover?width=800"))
                candidate_urls.append(self.client._url(f"/api/items/{item_id}/cover?w=800"))
                candidate_urls.append(self.client._url(f"/api/items/{item_id}/cover"))

            # Pick covers dir similar to simplified service (works without Flask app context)
            covers_dir = Path('/app/data/covers')
            if not covers_dir.exists():
                # Try DATA_DIR if available via env
                data_dir = os.environ.get('DATA_DIR')
                if data_dir:
                    covers_dir = Path(data_dir) / 'covers'
                else:
                    covers_dir = Path(__file__).parent.parent.parent / 'data' / 'covers'
            covers_dir.mkdir(parents=True, exist_ok=True)

            # Download with auth headers
            headers = self.client._headers().copy()
            # Prefer generic Accept for images
            headers['Accept'] = '*/*'

            resp = None
            insecure = os.environ.get('ABS_INSECURE', '').lower() in ('1', 'true', 'yes')
            for u in candidate_urls:
                try:
                    r = requests.get(u, headers=headers, timeout=12, stream=True, verify=not insecure)
                    if r.status_code == 401:
                        continue
                    r.raise_for_status()
                    # Validate it's an image
                    ctype_try = (r.headers.get('Content-Type') or '').lower()
                    if 'image' not in ctype_try and not u.lower().endswith(('.jpg', '.jpeg', '.png', '.webp', '.gif')):
                        continue
                    resp = r
                    break
                except Exception:
                    continue

            if resp is None:
                return None
            # Guess extension
            ext = '.jpg'
            ctype = (resp.headers.get('Content-Type') or '').lower()
            if 'png' in ctype:
                ext = '.png'
            elif 'webp' in ctype:
                ext = '.webp'
            elif 'gif' in ctype:
                ext = '.gif'
            elif 'jpeg' in ctype:
                ext = '.jpg'
            # If content-type didn't help, try using URL suffix
            else:
                lower_urls = ''.join(candidate_urls).lower()
                if any(lower_urls.endswith(suf) for suf in ['.png', '.webp', '.gif', '.jpeg', '.jpg']):
                    if lower_urls.endswith('.png'):
                        ext = '.png'
                    elif lower_urls.endswith('.webp'):
                        ext = '.webp'
                    elif lower_urls.endswith('.gif'):
                        ext = '.gif'
                    else:
                        ext = '.jpg'

            filename = f"{uuid.uuid4()}{ext}"
            out_path = covers_dir / filename
            with open(out_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=16384):
                    if not chunk:
                        continue
                    f.write(chunk)
            return f"/covers/{filename}"
        except Exception:
            return None

    def _apply_audiobook_fields(self, book_id: str, abs_item: Dict[str, Any]):
        """Set media_type='audiobook', audio_duration_ms, and audiobookshelf_id."""
        media = abs_item.get('media') or {}
        md = media.get('metadata') or {}
        # Duration ms logic mirrors mapping above
        duration_ms = None
        duration = media.get('duration') or md.get('duration')
        if isinstance(duration, (int, float)):
            duration_ms = int(float(duration) * 1000) if duration < 10_000_000 else int(duration)
        updates: Dict[str, Any] = {
            'media_type': 'audiobook',
        }
        if duration_ms is not None:
            updates['audio_duration_ms'] = duration_ms
        # Also set external id in custom_metadata to keep allowed whitelist minimal
        existing = {'audiobookshelf_id': abs_item.get('id') or abs_item.get('_id')}
        # Persist allowed properties via update_book
        self.kuzu_book_service.update_book_sync(book_id, updates)
        # Set external id via direct query to avoid whitelist constraints
        try:
            from app.infrastructure.kuzu_graph import safe_execute_kuzu_query
            from datetime import datetime, timezone
            # Always set external ABS id
            safe_execute_kuzu_query(
                """
                MATCH (b:Book {id: $book_id})
                SET b.audiobookshelf_id = $abs_id, b.updated_at = timestamp($ts)
                RETURN b.id
                """,
                {
                    'book_id': book_id,
                    'abs_id': existing['audiobookshelf_id'] or '',
                    'ts': datetime.now(timezone.utc).isoformat()
                }
            )
            # Also persist audio_duration_ms when available (not in standard update whitelist)
            if duration_ms is not None:
                safe_execute_kuzu_query(
                    """
                    MATCH (b:Book {id: $book_id})
                    SET b.audio_duration_ms = $dur_ms, b.updated_at = timestamp($ts)
                    RETURN b.id
                    """,
                    {
                        'book_id': book_id,
                        'dur_ms': int(duration_ms),
                        'ts': datetime.now(timezone.utc).isoformat()
                    }
                )
        except Exception:
            pass

    def _resolve_library_ids(self, library_ids: List[str]) -> List[str]:
        """Resolve provided identifiers (IDs or names) to actual library IDs.

        If no library_ids provided or none match, return all available IDs.
        """
        # Fetch libraries from ABS
        ok, libs, _ = self.client.list_libraries()
        if not ok:
            return list(library_ids or [])
        id_map = {}
        name_map = {}
        for lib in libs:
            lid = str(lib.get('id') or lib.get('_id') or lib.get('libraryId') or '').strip()
            lname = str(lib.get('name') or lib.get('title') or '').strip()
            if lid:
                id_map[lid.lower()] = lid
            if lname:
                name_map[lname.lower()] = lid
        resolved: List[str] = []
        for raw in (library_ids or []):
            key = str(raw).strip().lower()
            if not key:
                continue
            if key in id_map:
                resolved.append(id_map[key])
            elif key in name_map:
                resolved.append(name_map[key])
        if not resolved:
            # default to all libraries if none specified/matched
            resolved = list(id_map.values())
        # de-dup while preserving order
        seen = set()
        out: List[str] = []
        for x in resolved:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    def start_test_sync(self, library_ids: List[str], limit: int = 5) -> Dict[str, Any]:
        """Kick off background test sync for up to N items from first library."""
        task_id = f"abs_test_{uuid.uuid4().hex[:8]}"
        total = limit
        safe_create_import_job(self.user_id, task_id, {
            'task_id': task_id,
            'type': 'abs_test_sync',
            'status': 'started',
            'created_at': datetime.now(timezone.utc).isoformat(),
            'processed': 0,
            'total': total,
            'total_books': total,  # for progress UI tiles
            'success_titles': [],
            'error_messages': [],
            'processed_books': []
        })

        # Capture Flask app to provide application context inside background thread
        _ctx_mgr = None
        try:
            from flask import current_app as _flask_current_app
            _app_obj = _flask_current_app
            if _app_obj:
                _ctx_mgr = _app_obj.app_context()
        except Exception:
            _app_obj = None

        def worker():
            # Ensure Flask application context is available for any services that need it
            if _ctx_mgr is not None:
                _ctx_mgr.push()
            try:
                processed = 0
                successes: List[str] = []
                errors: List[str] = []

                # Resolve libraries to try
                libs_to_try: List[str] = self._resolve_library_ids(library_ids)
                if not libs_to_try:
                    raise ValueError('No ABS library_id configured or discovered')

                # Fetch first non-empty page of items from any library
                items: List[Dict[str, Any]] = []
                last_msg: Optional[str] = None
                for lib_id in libs_to_try:
                    res = self.client.list_library_items(lib_id, page=1, size=limit)
                    last_msg = res.get('message')
                    cand = res.get('items') or []
                    if cand:
                        items = cand
                        break

                if not items:
                    # Update job with info and finish early
                    safe_update_import_job(self.user_id, task_id, {
                        'status': 'completed',
                        'processed': 0,
                        'total': 0,
                        'total_books': 0,
                        'error_messages': ([f"No items returned from ABS. Last message: {last_msg}"] if last_msg else ["No items returned from ABS."])
                    })
                    return

                # Now that we know how many we'll process, set totals accurately
                total_local = min(len(items), limit)
                safe_update_import_job(self.user_id, task_id, {
                    'total': total_local,
                    'total_books': total_local,
                })

                for item in items[:limit]:
                    current_title = 'Unknown'
                    book_data: Optional[SimplifiedBook] = None
                    try:
                        # Fetch expanded item for richer metadata if available
                        item_id = item.get('id') or item.get('_id') or item.get('itemId')
                        if item_id:
                            detail = self.client.get_item(item_id, expanded=True)
                            if detail.get('ok') and detail.get('item'):
                                item = detail['item']

                        book_data = self._map_abs_item_to_simplified(item)
                        # Pre-cache cover with ABS auth and use local path before creating the book
                        try:
                            pre_local_cover = self._cache_cover_with_auth(item)
                            if pre_local_cover:
                                book_data.cover_url = pre_local_cover
                        except Exception:
                            pass
                        current_title = book_data.title or 'Unknown'

                        # Create book (universal library mode)
                        created = self.simple_books.add_book_to_user_library_sync(book_data, self.user_id, media_type='audiobook')
                        if not created:
                            raise RuntimeError('Failed to create book')
                        # Resolve created book id then apply audiobook-specific fields
                        book_id: Optional[str] = self._resolve_book_id(book_data)
                        if book_id:
                            # Attempt to cache cover with ABS auth, then update cover_url if saved
                            try:
                                local_cover = self._cache_cover_with_auth(item)
                                if local_cover:
                                    self.kuzu_book_service.update_book_sync(book_id, {'cover_url': local_cover})
                            except Exception:
                                pass
                            self._apply_audiobook_fields(book_id, item)
                            # Ensure media_type persisted at book level too (defensive)
                            try:
                                self.kuzu_book_service.update_book_sync(book_id, {'media_type': 'audiobook'})
                            except Exception:
                                pass
                            # Map categories
                            cats = self._extract_categories(item)
                            if cats:
                                try:
                                    from app.services.kuzu_service_facade import KuzuServiceFacade
                                    svc = KuzuServiceFacade()
                                    svc.update_book_sync(book_id, self.user_id, raw_categories=cats)
                                except Exception:
                                    pass

                        processed += 1
                        successes.append(current_title)
                        current_job = safe_get_import_job(self.user_id, task_id) or {}
                        pb = list(current_job.get('processed_books') or [])
                        pb.append({'title': current_title, 'status': 'success'})
                        safe_update_import_job(self.user_id, task_id, {
                            'status': 'running',
                            'processed': processed,
                            'processed_books': pb[-100:],
                        })
                    except BookAlreadyExistsError:
                        processed += 1
                        # Even if book exists, try to update its cover with an authenticated cached copy
                        try:
                            pre_local_cover = self._cache_cover_with_auth(item)
                            if pre_local_cover:
                                # book_data may be None if exception occurred before mapping
                                bd = book_data or self._map_abs_item_to_simplified(item)
                                book_id = self._resolve_book_id(bd)
                                if book_id:
                                    self.kuzu_book_service.update_book_sync(book_id, {'cover_url': pre_local_cover})
                                    # Attempt to backfill series fields if missing
                                    try:
                                        existing = self.kuzu_book_service.get_book_by_id_sync(book_id)
                                        updates: Dict[str, Any] = {}
                                        if existing and bd:
                                            if (not getattr(existing, 'series', None)) and (bd.series):
                                                updates['series'] = bd.series
                                            if (getattr(existing, 'series_order', None) in (None, 0)) and (bd.series_order is not None):
                                                updates['series_order'] = bd.series_order
                                            if (not getattr(existing, 'series_volume', None)) and (bd.series_volume):
                                                updates['series_volume'] = bd.series_volume
                                        if updates:
                                            self.kuzu_book_service.update_book_sync(book_id, updates)
                                    except Exception:
                                        pass
                        except Exception:
                            pass
                        current_job = safe_get_import_job(self.user_id, task_id) or {}
                        pb = list(current_job.get('processed_books') or [])
                        pb.append({'title': current_title, 'status': 'merged'})
                        safe_update_import_job(self.user_id, task_id, {
                            'status': 'running',
                            'processed': processed,
                            'success_titles': successes[-10:],
                            'processed_books': pb[-100:],
                        })
                    except Exception as e:
                        processed += 1
                        errors.append(str(e))
                        current_job = safe_get_import_job(self.user_id, task_id) or {}
                        pb = list(current_job.get('processed_books') or [])
                        pb.append({'title': current_title, 'status': 'error'})
                        safe_update_import_job(self.user_id, task_id, {
                            'status': 'running',
                            'processed': processed,
                            'error_messages': errors[-10:],
                            'processed_books': pb[-100:],
                        })

                safe_update_import_job(self.user_id, task_id, {
                    'status': 'completed',
                    'processed': processed,
                    'total': total_local
                })
            except Exception:
                traceback.print_exc()
                safe_update_import_job(self.user_id, task_id, {
                    'status': 'failed',
                    'error_messages': ['Unexpected error during ABS test sync']
                })
            finally:
                try:
                    if _ctx_mgr is not None:
                        _ctx_mgr.pop()
                except Exception:
                    pass

        t = threading.Thread(target=worker, name=f"abs-test-sync-{task_id}")
        t.daemon = True
        t.start()
        return {'task_id': task_id, 'total': total}

    def start_full_sync(self, library_ids: Optional[List[str]] = None, page_size: int = 50) -> Dict[str, Any]:
        """Kick off a full one-way sync from ABS to MyBibliotheca.

        Iterates all configured (or discovered) libraries and paginates through
        items, mapping and creating books. Progress is reported via the import
        job manager UI.
        """
        task_id = f"abs_full_{uuid.uuid4().hex[:8]}"
        safe_create_import_job(self.user_id, task_id, {
            'task_id': task_id,
            'type': 'abs_full_sync',
            'status': 'started',
            'created_at': datetime.now(timezone.utc).isoformat(),
            'processed': 0,
            'total': 0,
            'total_books': 0,
            'success_titles': [],
            'error_messages': [],
            'processed_books': []
        })

        # Capture Flask app to provide application context inside background thread
        _ctx_mgr_full = None
        try:
            from flask import current_app as _flask_current_app
            _app_obj_full = _flask_current_app
            if _app_obj_full:
                _ctx_mgr_full = _app_obj_full.app_context()
        except Exception:
            _app_obj_full = None

        def worker():
            # Ensure Flask application context is available for any services that need it
            if _ctx_mgr_full is not None:
                _ctx_mgr_full.push()
            try:
                libs_to_try = self._resolve_library_ids(library_ids or [])
                if not libs_to_try:
                    raise ValueError('No ABS libraries found to sync')
                processed = 0
                errors: List[str] = []
                success_titles: List[str] = []
                grand_total = 0

                # First pass: compute totals (best-effort)
                for lib_id in libs_to_try:
                    meta = self.client.list_library_items(lib_id, page=1, size=page_size)
                    lib_total = int(meta.get('total') or 0)
                    grand_total += lib_total
                safe_update_import_job(self.user_id, task_id, {
                    'total': grand_total,
                    'total_books': grand_total,
                })

                for lib_id in libs_to_try:
                    page = 1
                    while True:
                        res = self.client.list_library_items(lib_id, page=page, size=page_size)
                        items = res.get('items') or []
                        if not items:
                            break
                        for item in items:
                            current_title = 'Unknown'
                            book_data: Optional[SimplifiedBook] = None
                            try:
                                item_id = item.get('id') or item.get('_id') or item.get('itemId')
                                if item_id:
                                    detail = self.client.get_item(item_id, expanded=True)
                                    if detail.get('ok') and detail.get('item'):
                                        item = detail['item']
                                book_data = self._map_abs_item_to_simplified(item)
                                # Pre-cache cover with ABS auth and use local path before creating the book
                                try:
                                    pre_local_cover = self._cache_cover_with_auth(item)
                                    if pre_local_cover:
                                        book_data.cover_url = pre_local_cover
                                except Exception:
                                    pass
                                current_title = book_data.title or 'Unknown'
                                created = self.simple_books.add_book_to_user_library_sync(book_data, self.user_id, media_type='audiobook')
                                if not created:
                                    raise RuntimeError('Failed to create book')
                                # Resolve created book id and apply fields
                                book_id: Optional[str] = self._resolve_book_id(book_data)
                                if book_id:
                                    # Attempt to cache cover with ABS auth, then update cover_url if saved
                                    try:
                                        local_cover = self._cache_cover_with_auth(item)
                                        if local_cover:
                                            self.kuzu_book_service.update_book_sync(book_id, {'cover_url': local_cover})
                                    except Exception:
                                        pass
                                    self._apply_audiobook_fields(book_id, item)
                                    try:
                                        self.kuzu_book_service.update_book_sync(book_id, {'media_type': 'audiobook'})
                                    except Exception:
                                        pass
                                    cats = self._extract_categories(item)
                                    if cats:
                                        try:
                                            from app.services.kuzu_service_facade import KuzuServiceFacade
                                            svc = KuzuServiceFacade()
                                            svc.update_book_sync(book_id, self.user_id, raw_categories=cats)
                                        except Exception:
                                            pass
                                processed += 1
                                success_titles.append(current_title)
                                current_job = safe_get_import_job(self.user_id, task_id) or {}
                                pb = list(current_job.get('processed_books') or [])
                                pb.append({'title': current_title, 'status': 'success'})
                                safe_update_import_job(self.user_id, task_id, {
                                    'status': 'running',
                                    'processed': processed,
                                    'processed_books': pb[-100:],
                                })
                            except BookAlreadyExistsError:
                                processed += 1
                                # Update cover for duplicates if possible
                                try:
                                    pre_local_cover = self._cache_cover_with_auth(item)
                                    if pre_local_cover:
                                        bd: SimplifiedBook = book_data if 'book_data' in locals() and book_data else self._map_abs_item_to_simplified(item)
                                        book_id = self._resolve_book_id(bd)
                                        if book_id:
                                            self.kuzu_book_service.update_book_sync(book_id, {'cover_url': pre_local_cover})
                                            # Backfill series fields if missing on existing book
                                            try:
                                                existing = self.kuzu_book_service.get_book_by_id_sync(book_id)
                                                updates: Dict[str, Any] = {}
                                                if existing and bd:
                                                    if (not getattr(existing, 'series', None)) and (bd.series):
                                                        updates['series'] = bd.series
                                                    if (getattr(existing, 'series_order', None) in (None, 0)) and (bd.series_order is not None):
                                                        updates['series_order'] = bd.series_order
                                                    if (not getattr(existing, 'series_volume', None)) and (bd.series_volume):
                                                        updates['series_volume'] = bd.series_volume
                                                if updates:
                                                    self.kuzu_book_service.update_book_sync(book_id, updates)
                                            except Exception:
                                                pass
                                except Exception:
                                    pass
                                current_job = safe_get_import_job(self.user_id, task_id) or {}
                                pb = list(current_job.get('processed_books') or [])
                                pb.append({'title': current_title, 'status': 'merged'})
                                safe_update_import_job(self.user_id, task_id, {
                                    'status': 'running',
                                    'processed': processed,
                                    'success_titles': success_titles[-10:],
                                    'processed_books': pb[-100:],
                                })
                            except Exception as e:
                                processed += 1
                                errors.append(str(e))
                                current_job = safe_get_import_job(self.user_id, task_id) or {}
                                pb = list(current_job.get('processed_books') or [])
                                pb.append({'title': current_title, 'status': 'error'})
                                safe_update_import_job(self.user_id, task_id, {
                                    'status': 'running',
                                    'processed': processed,
                                    'error_messages': errors[-10:],
                                    'processed_books': pb[-100:],
                                })
                        page += 1

                safe_update_import_job(self.user_id, task_id, {
                    'status': 'completed',
                    'processed': processed,
                    'total': grand_total
                })
            except Exception:
                traceback.print_exc()
                safe_update_import_job(self.user_id, task_id, {
                    'status': 'failed',
                    'error_messages': ['Unexpected error during ABS full sync']
                })
            finally:
                try:
                    if _ctx_mgr_full is not None:
                        _ctx_mgr_full.pop()
                except Exception:
                    pass

        t = threading.Thread(target=worker, name=f"abs-full-sync-{task_id}")
        t.daemon = True
        t.start()
        return {'task_id': task_id}
