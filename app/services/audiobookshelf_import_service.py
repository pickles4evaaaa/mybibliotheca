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
from app.utils.audiobookshelf_settings import load_abs_settings, save_abs_settings
from app.services.audiobookshelf_listening_sync import AudiobookshelfListeningSync
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

    def _needs_item_detail_for_persons(self, item: Dict[str, Any]) -> bool:
        """Heuristic to decide if we must fetch expanded item details.

        Some ABS list endpoints include media/metadata but omit contributors
        (authors/narrators). To correctly create Person contributions, fetch
        the expanded item when those fields are missing.
        """
        try:
            media = item.get('media') or {}
            md = media.get('metadata') or {}
            # If no media/metadata block, we certainly need details
            if not isinstance(media, dict) or not isinstance(md, dict):
                return True
            # If any contributor signal present, we're fine
            if (isinstance(md.get('authors'), list) and len(md.get('authors') or []) > 0):
                return False
            a = md.get('author')
            if isinstance(a, str) and a.strip():
                return False
            if (isinstance(md.get('narrators'), list) and len(md.get('narrators') or []) > 0):
                return False
            n = md.get('narrator')
            if isinstance(n, str) and n.strip():
                return False
            # Otherwise, fetch expanded details to try to get contributors
            return True
        except Exception:
            return True

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
        # Extract ABS updated timestamp if available
        abs_updated_iso = self._extract_abs_updated_at(abs_item)
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
                SET b.audiobookshelf_id = $abs_id,
                    b.updated_at = CASE WHEN $ts IS NULL OR $ts = '' THEN b.updated_at ELSE timestamp($ts) END
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
                    SET b.audio_duration_ms = $dur_ms,
                        b.updated_at = CASE WHEN $ts IS NULL OR $ts = '' THEN b.updated_at ELSE timestamp($ts) END
                    RETURN b.id
                    """,
                    {
                        'book_id': book_id,
                        'dur_ms': int(duration_ms),
                        'ts': datetime.now(timezone.utc).isoformat()
                    }
                )
            # Persist ABS updated timestamp if we have one (stored as STRING in schema)
            if abs_updated_iso and isinstance(abs_updated_iso, str) and abs_updated_iso.strip() != '':
                safe_execute_kuzu_query(
                    """
                    MATCH (b:Book {id: $book_id})
                    SET b.audiobookshelf_updated_at = $abs_updated,
                        b.updated_at = CASE WHEN $ts IS NULL OR $ts = '' THEN b.updated_at ELSE timestamp($ts) END
                    RETURN b.id
                    """,
                    {
                        'book_id': book_id,
                        'abs_updated': abs_updated_iso.strip(),
                        'ts': datetime.now(timezone.utc).isoformat()
                    }
                )
        except Exception:
            pass

    # -- Contributor helpers -------------------------------------------------
    def _ensure_person_exists(self, name: str) -> Optional[str]:
        if not name or str(name).strip().lower() == 'unknown':
            return None
        try:
            from app.infrastructure.kuzu_graph import safe_execute_kuzu_query
            nm = str(name).strip()
            norm = nm.lower()
            qr = safe_execute_kuzu_query(
                """
                MATCH (p:Person)
                WHERE toLower(p.normalized_name) = $norm OR toLower(p.name) = $norm
                RETURN p.id
                LIMIT 1
                """,
                {"norm": norm}
            )
            from app.simplified_book_service import _convert_query_result_to_list as _legacy_convert
            rows = _legacy_convert(qr)
            if rows:
                r0 = rows[0]
                if isinstance(r0, dict):
                    return r0.get('result') or r0.get('col_0') or r0.get('p.id') or r0.get('id')
                # If row is a scalar-like, best-effort cast
                try:
                    return str(r0)
                except Exception:
                    return None
            # Create
            pid = str(uuid.uuid4())
            safe_execute_kuzu_query(
                """
                CREATE (p:Person {id: $id, name: $name, normalized_name: toLower($name), created_at: CASE WHEN $ts IS NULL OR $ts = '' THEN NULL ELSE timestamp($ts) END})
                RETURN p.id
                """,
                {"id": pid, "name": nm, "ts": datetime.now(timezone.utc).isoformat()}
            )
            return pid
        except Exception:
            return None

    def _ensure_contributor(self, book_id: str, person_name: str, role: str, order_index: int = 0) -> None:
        pid = self._ensure_person_exists(person_name)
        if not pid:
            return
        try:
            from app.infrastructure.kuzu_graph import safe_execute_kuzu_query
            qr = safe_execute_kuzu_query(
                """
                MATCH (p:Person {id: $pid})- [r:AUTHORED]-> (b:Book {id: $bid})
                WHERE r.role = $role
                RETURN r
                LIMIT 1
                """,
                {"pid": pid, "bid": book_id, "role": role}
            )
            from app.simplified_book_service import _convert_query_result_to_list as _legacy_convert
            rows = _legacy_convert(qr)
            if rows:
                r0 = rows[0]
                if isinstance(r0, dict) and (r0.get('result') or r0.get('col_0') or r0.get('r') or r0.get('b.id') or r0.get('id')):
                    return  # already exists
            safe_execute_kuzu_query(
                """
                MATCH (p:Person {id: $pid}), (b:Book {id: $bid})
                CREATE (p)-[:AUTHORED {role: $role, order_index: $ord, created_at: CASE WHEN $ts IS NULL OR $ts = '' THEN NULL ELSE timestamp($ts) END}]->(b)
                RETURN b.id
                """,
                {"pid": pid, "bid": book_id, "role": role, "ord": int(order_index), "ts": datetime.now(timezone.utc).isoformat()}
            )
        except Exception:
            return

    def _ensure_contributors_for_book(self, book_id: str, book_data: SimplifiedBook) -> None:
        try:
            # Primary author
            if getattr(book_data, 'author', None) and str(book_data.author).strip().lower() != 'unknown':
                self._ensure_contributor(book_id, book_data.author, 'authored', 0)
            # Additional authors (comma-separated)
            addl = getattr(book_data, 'additional_authors', None)
            if addl:
                idx = 1
                for nm in [s.strip() for s in str(addl).split(',') if s.strip()]:
                    if nm.lower() == 'unknown':
                        continue
                    self._ensure_contributor(book_id, nm, 'authored', idx)
                    idx += 1
            # Narrators
            narr = getattr(book_data, 'narrator', None)
            if narr:
                idx = 0
                for nm in [s.strip() for s in str(narr).split(',') if s.strip()]:
                    if nm.lower() == 'unknown':
                        continue
                    self._ensure_contributor(book_id, nm, 'narrated', idx)
                    idx += 1
        except Exception:
            pass

    # --- Delta sync helpers -------------------------------------------------
    def _extract_abs_updated_at(self, item: Dict[str, Any]) -> Optional[str]:
        """Return an ISO8601 string for the ABS item's last updated timestamp if present.

        Tries common keys and normalizes numeric epochs (s/ms) and string dates.
        """
        try:
            cand = item.get('updatedAt') or item.get('lastUpdate') or item.get('lastUpdated') or item.get('lastModified') or None
            # Some servers nest under media/ or metadata
            if cand is None:
                media = item.get('media') or {}
                md = media.get('metadata') or {}
                cand = media.get('updatedAt') or md.get('updatedAt') or md.get('lastModified') or None
            if cand is None:
                return None
            from datetime import datetime, timezone
            # Numeric epoch
            if isinstance(cand, (int, float)):
                # Heuristic: treat values > 10^12 as ms
                epoch_sec = float(cand) / 1000.0 if float(cand) > 1_000_000_000_000 else float(cand)
                return datetime.fromtimestamp(epoch_sec, tz=timezone.utc).isoformat()
            # ISO-like string
            s = str(cand)
            try:
                # Normalize Z suffix
                if s.endswith('Z'):
                    return s
                # Attempt parse/normalize
                dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
                return dt.astimezone(timezone.utc).isoformat()
            except Exception:
                return s  # return raw string as best-effort
        except Exception:
            return None

    def _get_local_abs_updated_at(self, abs_id: Optional[str]) -> Optional[str]:
        if not abs_id:
            return None
        try:
            from app.infrastructure.kuzu_graph import safe_execute_kuzu_query
            from app.services.kuzu_service_facade import _convert_query_result_to_list
            qr = safe_execute_kuzu_query(
                """
                MATCH (b:Book) WHERE b.audiobookshelf_id = $abs_id RETURN b.audiobookshelf_updated_at LIMIT 1
                """,
                { 'abs_id': str(abs_id) }
            )
            rows = _convert_query_result_to_list(qr)
            if rows:
                r0 = rows[0]
                if isinstance(r0, dict):
                    return r0.get('result') or r0.get('col_0') or r0.get('b.audiobookshelf_updated_at')
                try:
                    return str(r0)
                except Exception:
                    return None
        except Exception:
            return None
        return None

    def _should_process_abs_item(self, item: Dict[str, Any]) -> bool:
        """Return True if item is new or updated compared to local store, else False."""
        try:
            abs_id = item.get('id') or item.get('_id') or item.get('itemId')
            if not abs_id:
                return True
            remote_iso = self._extract_abs_updated_at(item)
            # If no remote timestamp, process only if book is missing locally
            local_iso = self._get_local_abs_updated_at(abs_id)
            if remote_iso is None:
                return local_iso is None
            if local_iso is None:
                return True
            # Compare as ISO; fallback to lexical compare (works for ISO)
            from datetime import datetime
            try:
                rdt = datetime.fromisoformat(remote_iso.replace('Z', '+00:00'))
                ldt = datetime.fromisoformat(local_iso.replace('Z', '+00:00'))
                return rdt > ldt
            except Exception:
                return str(remote_iso) > str(local_iso)
        except Exception:
            return True

    # Public internal: run test sync using an existing task_id (no thread)
    def _run_test_sync_job(self, task_id: str, library_ids: List[str], limit: int = 5) -> None:
        try:
            processed = 0
            successes: List[str] = []
            errors: List[str] = []

            libs_to_try: List[str] = self._resolve_library_ids(library_ids)
            if not libs_to_try:
                raise ValueError('No ABS library_id configured or discovered')

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
                safe_update_import_job(self.user_id, task_id, {
                    'status': 'completed',
                    'processed': 0,
                    'total': 0,
                    'total_books': 0,
                    'error_messages': ([f"No items returned from ABS. Last message: {last_msg}"] if last_msg else ["No items returned from ABS."])
                })
                return

            total_local = min(len(items), limit)
            # Nudge UI and set accurate totals
            safe_update_import_job(self.user_id, task_id, {
                'status': 'running',
                'total': total_local,
                'total_books': total_local,
                'recent_activity': ['Starting ABS test import...']
            })

            for item in items[:limit]:
                current_title = 'Unknown'
                book_data: Optional[SimplifiedBook] = None
                try:
                    # Delta check: skip if not new/updated
                    if not self._should_process_abs_item(item):
                        # Immediately sync this item's listening progress for the user
                        try:
                            listener = AudiobookshelfListeningSync(self.user_id, self.client)
                            iid = item.get('id') or item.get('_id') or item.get('itemId')
                            if iid:
                                listener.sync_item_progress(iid)
                        except Exception:
                            pass

                        processed += 1
                        # Try to sync progress for existing item as well
                        try:
                            listener = AudiobookshelfListeningSync(self.user_id, self.client)
                            iid = item.get('id') or item.get('_id') or item.get('itemId')
                            if iid:
                                listener.sync_item_progress(iid)
                        except Exception:
                            pass

                        current_job = safe_get_import_job(self.user_id, task_id) or {}
                        pb = list(current_job.get('processed_books') or [])
                        pb.append({'title': (item.get('media') or {}).get('metadata',{}).get('title') or item.get('title') or 'Unknown', 'status': 'skipped'})
                        safe_update_import_job(self.user_id, task_id, {
                            'status': 'running',
                            'processed': processed,
                            'skipped': (current_job.get('skipped') or 0) + 1,
                            'processed_books': pb[-100:],
                        })
                        continue
                    item_id = item.get('id') or item.get('_id') or item.get('itemId')
                    # Only fetch expanded item if contributors/metadata are missing
                    need_detail = self._needs_item_detail_for_persons(item)
                    if item_id and need_detail:
                        detail = self.client.get_item(item_id, expanded=True)
                        if detail.get('ok') and detail.get('item'):
                            item = detail['item']

                    book_data = self._map_abs_item_to_simplified(item)
                    try:
                        pre_local_cover = self._cache_cover_with_auth(item)
                        if pre_local_cover:
                            book_data.cover_url = pre_local_cover
                    except Exception:
                        pass
                    current_title = book_data.title or 'Unknown'
                    # Surface current title early so UI shows movement
                    try:
                        safe_update_import_job(self.user_id, task_id, {
                            'current_book': current_title
                        })
                    except Exception:
                        pass

                    created = self.simple_books.add_book_to_user_library_sync(book_data, self.user_id, media_type='audiobook')
                    if not created:
                        raise RuntimeError('Failed to create book')
                    book_id: Optional[str] = self._resolve_book_id(book_data)
                    if book_id:
                        # Combine updates to minimize Kuzu roundtrips
                        updates: Dict[str, Any] = {'media_type': 'audiobook'}
                        try:
                            local_cover = self._cache_cover_with_auth(item)
                            if local_cover:
                                updates['cover_url'] = local_cover
                        except Exception:
                            pass
                        if updates:
                            try:
                                self.kuzu_book_service.update_book_sync(book_id, updates)
                            except Exception:
                                pass
                        self._apply_audiobook_fields(book_id, item)
                        # Ensure contributors are present
                        try:
                            self._ensure_contributors_for_book(book_id, book_data)
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
                    successes.append(current_title)
                    current_job = safe_get_import_job(self.user_id, task_id) or {}
                    pb = list(current_job.get('processed_books') or [])
                    pb.append({'title': current_title, 'status': 'success'})
                    safe_update_import_job(self.user_id, task_id, {
                        'status': 'running',
                        'processed': processed,
                        'success': (current_job.get('success') or 0) + 1,
                        'processed_books': pb[-100:],
                        'audiobooks_synced': (current_job.get('audiobooks_synced') or 0) + 1,
                        'recent_activity': (current_job.get('recent_activity') or [])[-9:] + [f"Imported: {current_title}"]
                    })
                except BookAlreadyExistsError:
                    processed += 1
                    try:
                        pre_local_cover = self._cache_cover_with_auth(item)
                        if pre_local_cover:
                            bd = book_data or self._map_abs_item_to_simplified(item)
                            book_id = self._resolve_book_id(bd)
                            if book_id:
                                self.kuzu_book_service.update_book_sync(book_id, {'cover_url': pre_local_cover})
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
                                    # Also ensure contributors now that we have richer metadata
                                    try:
                                        self._ensure_contributors_for_book(book_id, bd)
                                    except Exception:
                                        pass
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
                        'merged': (current_job.get('merged') or 0) + 1,
                        'success_titles': successes[-10:],
                        'processed_books': pb[-100:],
                        'audiobooks_synced': (current_job.get('audiobooks_synced') or 0) + 1,
                        'recent_activity': (current_job.get('recent_activity') or [])[-9:] + [f"Merged: {current_title}"]
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
                        'errors': (current_job.get('errors') or 0) + 1,
                        'error_messages': errors[-10:],
                        'processed_books': pb[-100:],
                    })
            # After books, run a listening sync and stream progress so UI moves
            try:
                listener = AudiobookshelfListeningSync(self.user_id, self.client)
                def _ls_cb(snapshot: dict):
                    try:
                        safe_update_import_job(self.user_id, task_id, {
                            'status': 'running',
                            'listening_sessions': int(snapshot.get('processed') or 0),
                            'listening_matched': int(snapshot.get('matched') or 0),
                        })
                    except Exception:
                        pass
                lsum = listener.sync(page_size=100, progress_cb=_ls_cb)
                current_job = safe_get_import_job(self.user_id, task_id) or {}
                pb = list(current_job.get('processed_books') or [])
                pb.append({'title': f"ABS listening sync: processed {lsum.get('processed', 0)}, matched {lsum.get('matched', 0)}", 'status': 'info'})
                safe_update_import_job(self.user_id, task_id, {
                    'processed_books': pb[-100:],
                    'listening_sessions': int(lsum.get('processed', 0)),
                    'listening_matched': int(lsum.get('matched', 0)),
                })
            except Exception:
                pass

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

    # Public internal: run full sync using an existing task_id (no thread)
    def _run_full_sync_job(self, task_id: str, library_ids: Optional[List[str]] = None, page_size: int = 50) -> None:
        try:
            libs_to_try = self._resolve_library_ids(library_ids or [])
            if not libs_to_try:
                raise ValueError('No ABS libraries found to sync')
            processed = 0
            errors: List[str] = []
            success_titles: List[str] = []
            grand_total = 0
            # Read per-library cutoff timestamps (best-effort)
            settings = load_abs_settings()
            last_sync_map = settings.get('last_library_sync_map') or {}
            latest_seen_map: Dict[str, str] = {}

            for lib_id in libs_to_try:
                meta = self.client.list_library_items(lib_id, page=1, size=page_size)
                lib_total = int(meta.get('total') or 0)
                grand_total += lib_total
            # Nudge UI: show totals and mark as running
            safe_update_import_job(self.user_id, task_id, {
                'status': 'running',
                'total': grand_total,
                'total_books': grand_total,
                'recent_activity': ['Starting ABS full import...']
            })

            for lib_id in libs_to_try:
                page = 1
                cutoff_iso = None
                try:
                    cutoff_iso = last_sync_map.get(lib_id)
                except Exception:
                    cutoff_iso = None
                while True:
                    # Cancellation check before fetching next page
                    try:
                        _job = safe_get_import_job(self.user_id, task_id) or {}
                        if _job.get('cancelled'):
                            safe_update_import_job(self.user_id, task_id, {
                                'status': 'cancelled',
                                'processed': processed,
                            })
                            return
                    except Exception:
                        pass
                    res = self.client.list_library_items(lib_id, page=page, size=page_size)
                    items = res.get('items') or []
                    if not items:
                        break
                    stop_due_to_cutoff = False
                    for item in items:
                        current_title = 'Unknown'
                        book_data: Optional[SimplifiedBook] = None
                        try:
                            # Track latest updatedAt for this page to aid cutoff persistence
                            try:
                                upd = self._extract_abs_updated_at(item)
                                if upd:
                                    prev = latest_seen_map.get(lib_id)
                                    if (not prev) or (str(upd) > str(prev)):
                                        latest_seen_map[lib_id] = str(upd)
                            except Exception:
                                pass
                            # Delta check: skip if not new/updated
                            if not self._should_process_abs_item(item):
                                processed += 1
                                current_job = safe_get_import_job(self.user_id, task_id) or {}
                                pb = list(current_job.get('processed_books') or [])
                                pb.append({'title': (item.get('media') or {}).get('metadata',{}).get('title') or item.get('title') or 'Unknown', 'status': 'skipped'})
                                safe_update_import_job(self.user_id, task_id, {
                                    'status': 'running',
                                    'processed': processed,
                                    'skipped': (current_job.get('skipped') or 0) + 1,
                                    'processed_books': pb[-100:],
                                })
                                # If sorted by updatedAt desc, once we encounter older than cutoff we can stop paging further
                                if cutoff_iso:
                                    try:
                                        r_iso = self._extract_abs_updated_at(item)
                                        if r_iso and str(r_iso) <= str(cutoff_iso):
                                            stop_due_to_cutoff = True
                                            break
                                    except Exception:
                                        pass
                                continue
                            item_id = item.get('id') or item.get('_id') or item.get('itemId')
                            need_detail = self._needs_item_detail_for_persons(item)
                            if item_id and need_detail:
                                detail = self.client.get_item(item_id, expanded=True)
                                if detail.get('ok') and detail.get('item'):
                                    item = detail['item']
                            book_data = self._map_abs_item_to_simplified(item)
                            try:
                                pre_local_cover = self._cache_cover_with_auth(item)
                                if pre_local_cover:
                                    book_data.cover_url = pre_local_cover
                            except Exception:
                                pass
                            current_title = book_data.title or 'Unknown'
                            # Surface current title early so UI shows movement
                            try:
                                safe_update_import_job(self.user_id, task_id, {
                                    'current_book': current_title
                                })
                            except Exception:
                                pass
                            created = self.simple_books.add_book_to_user_library_sync(book_data, self.user_id, media_type='audiobook')
                            if not created:
                                raise RuntimeError('Failed to create book')
                            book_id: Optional[str] = self._resolve_book_id(book_data)
                            if book_id:
                                updates2: Dict[str, Any] = {'media_type': 'audiobook'}
                                try:
                                    local_cover = self._cache_cover_with_auth(item)
                                    if local_cover:
                                        updates2['cover_url'] = local_cover
                                except Exception:
                                    pass
                                if updates2:
                                    try:
                                        self.kuzu_book_service.update_book_sync(book_id, updates2)
                                    except Exception:
                                        pass
                                self._apply_audiobook_fields(book_id, item)
                                # Ensure contributors are present
                                try:
                                    self._ensure_contributors_for_book(book_id, book_data)
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
                                'audiobooks_synced': (current_job.get('audiobooks_synced') or 0) + 1,
                                'processed_books': pb[-100:],
                                'recent_activity': (current_job.get('recent_activity') or [])[-9:] + [f"Imported: {current_title}"]
                            })
                        except BookAlreadyExistsError:
                            processed += 1
                            try:
                                pre_local_cover = self._cache_cover_with_auth(item)
                                if pre_local_cover:
                                    bd: SimplifiedBook = book_data if 'book_data' in locals() and book_data else self._map_abs_item_to_simplified(item)
                                    book_id = self._resolve_book_id(bd)
                                    if book_id:
                                        self.kuzu_book_service.update_book_sync(book_id, {'cover_url': pre_local_cover})
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
                                            # Also ensure contributors now that we have richer metadata
                                            try:
                                                self._ensure_contributors_for_book(book_id, bd)
                                            except Exception:
                                                pass
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
                                'audiobooks_synced': (current_job.get('audiobooks_synced') or 0) + 1,
                                'success_titles': success_titles[-10:],
                                'processed_books': pb[-100:],
                                'recent_activity': (current_job.get('recent_activity') or [])[-9:] + [f"Merged: {current_title}"]
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
                    if stop_due_to_cutoff:
                        break
                    page += 1

            # Persist latest seen per-library timestamps
            try:
                if latest_seen_map:
                    new_map = dict((settings.get('last_library_sync_map') or {}))
                    for k, v in latest_seen_map.items():
                        prev = new_map.get(k)
                        if (not prev) or (str(v) > str(prev)):
                            new_map[k] = v
                    save_abs_settings({'last_library_sync_map': new_map, 'last_library_sync': __import__('time').time()})
            except Exception:
                pass

            # After books, run a listening sync to reconcile reading status/progress
            try:
                listener = AudiobookshelfListeningSync(self.user_id, self.client)
                def _ls_cb(snapshot: dict):
                    try:
                        safe_update_import_job(self.user_id, task_id, {
                            'status': 'running',
                            'listening_sessions': int(snapshot.get('processed') or 0),
                            'listening_matched': int(snapshot.get('matched') or 0),
                        })
                    except Exception:
                        pass
                lsum = listener.sync(page_size=200, progress_cb=_ls_cb)
                current_job = safe_get_import_job(self.user_id, task_id) or {}
                pb = list(current_job.get('processed_books') or [])
                pb.append({'title': f"ABS listening sync: processed {lsum.get('processed', 0)}, matched {lsum.get('matched', 0)}", 'status': 'info'})
                safe_update_import_job(self.user_id, task_id, {
                    'processed_books': pb[-100:],
                    'listening_sessions': int(lsum.get('processed', 0)),
                    'listening_matched': int(lsum.get('matched', 0)),
                })
            except Exception:
                pass

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

    def _resolve_library_ids(self, library_ids: List[str]) -> List[str]:
        """Resolve provided identifiers (IDs or names) to actual library IDs.

        If no library_ids provided or none match, return all available IDs.
        """
        # Fetch libraries from ABS (may be unauthorized for limited tokens)
        try:
            ok, libs, _ = self.client.list_libraries()
        except Exception:
            ok, libs = False, []
        if not ok:
            # Trust provided IDs when listing is unavailable
            return [str(x).strip() for x in (library_ids or []) if str(x).strip()]
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
            resolved = list(id_map.values()) if id_map else [str(x).strip() for x in (library_ids or []) if str(x).strip()]
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
            'success': 0,
            'merged': 0,
            'errors': 0,
            'skipped': 0,
            'unmatched': 0,
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

                imported_item_ids: List[str] = []
                for item in items[:limit]:
                    # Cancellation check
                    try:
                        _job = safe_get_import_job(self.user_id, task_id) or {}
                        if _job.get('cancelled'):
                            safe_update_import_job(self.user_id, task_id, {
                                'status': 'cancelled',
                                'processed': processed,
                            })
                            return
                    except Exception:
                        pass
                    current_title = 'Unknown'
                    book_data: Optional[SimplifiedBook] = None
                    try:
                        # Fetch expanded item for richer metadata if available
                        item_id = item.get('id') or item.get('_id') or item.get('itemId')
                        if item_id:
                            detail = self.client.get_item(item_id, expanded=True)
                            if detail.get('ok') and detail.get('item'):
                                item = detail['item']
                            # Track imported ABS item ids for listening seed
                            try:
                                iid = item.get('id') or item.get('_id') or item.get('itemId') or item_id
                                if iid:
                                    imported_item_ids.append(str(iid))
                            except Exception:
                                pass

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
                            # Ensure contributors are present
                            try:
                                self._ensure_contributors_for_book(book_id, book_data)
                            except Exception:
                                pass
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
                            'success': (current_job.get('success') or 0) + 1,
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
                            'merged': (current_job.get('merged') or 0) + 1,
                            'success_titles': successes[-10:],
                            'processed_books': pb[-100:],
                            'audiobooks_synced': (current_job.get('audiobooks_synced') or 0) + 1,
                            'recent_activity': (current_job.get('recent_activity') or [])[-9:] + [f"Merged: {current_title}"]
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
                            'errors': (current_job.get('errors') or 0) + 1,
                            'error_messages': errors[-10:],
                            'processed_books': pb[-100:],
                        })

                # After mini book import, also run a listening sync to include history in Test Sync
                try:
                    listener = AudiobookshelfListeningSync(self.user_id, self.client)
                    # Pass imported ABS item ids to seed per-item progress if sessions are empty
                    lsum = listener.sync(page_size=100, seed_item_ids=imported_item_ids[:limit])
                    # Record a compact summary line in the processed list (non-blocking UI info)
                    current_job = safe_get_import_job(self.user_id, task_id) or {}
                    pb = list(current_job.get('processed_books') or [])
                    pb.append({'title': f"ABS listening sync: processed {lsum.get('processed', 0)}, matched {lsum.get('matched', 0)}", 'status': 'info'})
                    updates = {
                        'processed_books': pb[-100:],
                        'listening_processed': lsum.get('processed', 0),
                        'listening_matched': lsum.get('matched', 0),
                        'listening_sessions': lsum.get('processed', 0)
                    }
                    safe_update_import_job(self.user_id, task_id, updates)
                except Exception as le:
                    # Non-fatal: append an info/error line
                    try:
                        current_job = safe_get_import_job(self.user_id, task_id) or {}
                        pb = list(current_job.get('processed_books') or [])
                        pb.append({'title': f"ABS listening sync error: {le}", 'status': 'error'})
                        safe_update_import_job(self.user_id, task_id, {'processed_books': pb[-100:]})
                    except Exception:
                        pass

                # Finalize job
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
            'success': 0,
            'merged': 0,
            'errors': 0,
            'skipped': 0,
            'unmatched': 0,
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
                    # Per-library cutoff based on last sync
                    settings_local = load_abs_settings()
                    cutoff_iso = None
                    try:
                        cutoff_iso = (settings_local.get('last_library_sync_map') or {}).get(lib_id)
                    except Exception:
                        cutoff_iso = None
                    latest_seen_for_lib: Optional[str] = None
                    while True:
                        res = self.client.list_library_items(lib_id, page=page, size=page_size)
                        items = res.get('items') or []
                        if not items:
                            break

                        # Phase A: parallel network-bound enrichment (item detail + cover)
                        from concurrent.futures import ThreadPoolExecutor, as_completed
                        def _prepare(item_in: Dict[str, Any]) -> Dict[str, Any]:
                            loc_item = dict(item_in)
                            bd: Optional[SimplifiedBook] = None
                            local_cover: Optional[str] = None
                            try:
                                # Skip early in prepare if not updated; mark sentinel
                                if not self._should_process_abs_item(loc_item):
                                    return {'item': loc_item, 'skip': True}
                                item_id = loc_item.get('id') or loc_item.get('_id') or loc_item.get('itemId')
                                if item_id and self._needs_item_detail_for_persons(loc_item):
                                    detail = self.client.get_item(item_id, expanded=True)
                                    if detail.get('ok') and detail.get('item'):
                                        loc_item = detail['item']
                                try:
                                    local_cover = self._cache_cover_with_auth(loc_item)
                                except Exception:
                                    local_cover = None
                                bd = self._map_abs_item_to_simplified(loc_item)
                                if local_cover:
                                    try:
                                        bd.cover_url = local_cover
                                    except Exception:
                                        pass
                            except Exception:
                                # Fallback to minimal mapping
                                try:
                                    bd = self._map_abs_item_to_simplified(loc_item)
                                except Exception:
                                    bd = None
                            return {'item': loc_item, 'book_data': bd, 'local_cover': local_cover}

                        prepared: list = []
                        max_workers = 8
                        try:
                            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                                futures = [ex.submit(_prepare, it) for it in items]
                                for fut in as_completed(futures):
                                    try:
                                        prepared.append(fut.result())
                                    except Exception:
                                        prepared.append({'item': {}, 'book_data': None, 'local_cover': None})
                        except Exception:
                            # If thread pool fails, fall back to serial prepare
                            prepared = [_prepare(it) for it in items]

                        # Phase B: sequential Kuzu writes (respect single-worker constraint)
                        stop_due_to_cutoff = False
                        for entry in prepared:
                            # Cancellation check within page processing
                            try:
                                _job = safe_get_import_job(self.user_id, task_id) or {}
                                if _job.get('cancelled'):
                                    safe_update_import_job(self.user_id, task_id, {
                                        'status': 'cancelled',
                                        'processed': processed,
                                    })
                                    return
                            except Exception:
                                pass
                            # Handle skipped entries from prepare
                            if entry.get('skip'):
                                # Update latest seen timestamp if present
                                try:
                                    upd = self._extract_abs_updated_at(entry.get('item') or {})
                                    if upd and ((not latest_seen_for_lib) or (str(upd) > str(latest_seen_for_lib))):
                                        latest_seen_for_lib = str(upd)
                                except Exception:
                                    pass
                                processed += 1
                                current_job = safe_get_import_job(self.user_id, task_id) or {}
                                pb = list(current_job.get('processed_books') or [])
                                t = (entry.get('item') or {}).get('title') or ((entry.get('item') or {}).get('media') or {}).get('metadata',{}).get('title') or 'Unknown'
                                pb.append({'title': t, 'status': 'skipped'})
                                safe_update_import_job(self.user_id, task_id, {
                                    'status': 'running',
                                    'processed': processed,
                                    'skipped': (current_job.get('skipped') or 0) + 1,
                                    'processed_books': pb[-100:],
                                })
                                # If sorted desc and we hit older/equal to cutoff, we can stop
                                if cutoff_iso:
                                    try:
                                        r_iso = self._extract_abs_updated_at(entry.get('item') or {})
                                        if r_iso and str(r_iso) <= str(cutoff_iso):
                                            stop_due_to_cutoff = True
                                            break
                                    except Exception:
                                        pass
                                continue
                            obj_item = entry.get('item') or {}
                            book_data: Optional[SimplifiedBook] = entry.get('book_data')
                            current_title = 'Unknown'
                            try:
                                # Update latest seen timestamp
                                try:
                                    upd = self._extract_abs_updated_at(obj_item)
                                    if upd and ((not latest_seen_for_lib) or (str(upd) > str(latest_seen_for_lib))):
                                        latest_seen_for_lib = str(upd)
                                except Exception:
                                    pass
                                if not book_data:
                                    book_data = self._map_abs_item_to_simplified(obj_item)
                                current_title = getattr(book_data, 'title', None) or 'Unknown'
                                created = self.simple_books.add_book_to_user_library_sync(book_data, self.user_id, media_type='audiobook')
                                if not created:
                                    raise RuntimeError('Failed to create book')
                                book_id: Optional[str] = self._resolve_book_id(book_data)
                                if book_id:
                                    updates3: Dict[str, Any] = {'media_type': 'audiobook'}
                                    # We may already have a cached local cover; prefer that
                                    local_cover = entry.get('local_cover')
                                    if local_cover:
                                        updates3['cover_url'] = local_cover
                                    else:
                                        try:
                                            local_cover2 = self._cache_cover_with_auth(obj_item)
                                            if local_cover2:
                                                updates3['cover_url'] = local_cover2
                                        except Exception:
                                            pass
                                    if updates3:
                                        try:
                                            self.kuzu_book_service.update_book_sync(book_id, updates3)
                                        except Exception:
                                            pass
                                    self._apply_audiobook_fields(book_id, obj_item)
                                    cats = self._extract_categories(obj_item)
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
                                    'success': (current_job.get('success') or 0) + 1,
                                    'processed_books': pb[-100:],
                                })
                            except BookAlreadyExistsError:
                                processed += 1
                                # Update cover/backfill metadata for duplicates when possible
                                try:
                                    local_cover = entry.get('local_cover')
                                    bd_dup: SimplifiedBook = book_data or self._map_abs_item_to_simplified(obj_item)
                                    book_id = self._resolve_book_id(bd_dup)
                                    if book_id:
                                        if local_cover:
                                            try:
                                                self.kuzu_book_service.update_book_sync(book_id, {'cover_url': local_cover})
                                            except Exception:
                                                pass
                                        try:
                                            existing = self.kuzu_book_service.get_book_by_id_sync(book_id)
                                            updates: Dict[str, Any] = {}
                                            if existing and bd_dup:
                                                if (not getattr(existing, 'series', None)) and (bd_dup.series):
                                                    updates['series'] = bd_dup.series
                                                if (getattr(existing, 'series_order', None) in (None, 0)) and (bd_dup.series_order is not None):
                                                    updates['series_order'] = bd_dup.series_order
                                                if (not getattr(existing, 'series_volume', None)) and (bd_dup.series_volume):
                                                    updates['series_volume'] = bd_dup.series_volume
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
                                    'merged': (current_job.get('merged') or 0) + 1,
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
                                    'errors': (current_job.get('errors') or 0) + 1,
                                    'error_messages': errors[-10:],
                                    'processed_books': pb[-100:],
                                })

                        if stop_due_to_cutoff:
                            # Persist per-library timestamp before breaking
                            try:
                                if latest_seen_for_lib:
                                    settings_update = load_abs_settings()
                                    m = dict((settings_update.get('last_library_sync_map') or {}))
                                    prev = m.get(lib_id)
                                    if (not prev) or (str(latest_seen_for_lib) > str(prev)):
                                        m[lib_id] = latest_seen_for_lib
                                    save_abs_settings({'last_library_sync_map': m, 'last_library_sync': __import__('time').time()})
                            except Exception:
                                pass
                            break
                        page += 1

                    # Persist after finishing this library
                    try:
                        if latest_seen_for_lib:
                            settings_update = load_abs_settings()
                            m = dict((settings_update.get('last_library_sync_map') or {}))
                            prev = m.get(lib_id)
                            if (not prev) or (str(latest_seen_for_lib) > str(prev)):
                                m[lib_id] = latest_seen_for_lib
                            save_abs_settings({'last_library_sync_map': m, 'last_library_sync': __import__('time').time()})
                    except Exception:
                        pass

                # After finishing all libraries, reconcile listening/progress and stream to UI
                try:
                    listener = AudiobookshelfListeningSync(self.user_id, self.client)
                    def _ls_cb2(snapshot: dict):
                        try:
                            safe_update_import_job(self.user_id, task_id, {
                                'status': 'running',
                                'listening_sessions': int(snapshot.get('processed') or 0),
                                'listening_matched': int(snapshot.get('matched') or 0),
                            })
                        except Exception:
                            pass
                    lsum2 = listener.sync(page_size=200, progress_cb=_ls_cb2)
                    current_job2 = safe_get_import_job(self.user_id, task_id) or {}
                    pb2 = list(current_job2.get('processed_books') or [])
                    pb2.append({'title': f"ABS listening sync: processed {lsum2.get('processed', 0)}, matched {lsum2.get('matched', 0)}", 'status': 'info'})
                    safe_update_import_job(self.user_id, task_id, {
                        'processed_books': pb2[-100:],
                        'listening_sessions': int(lsum2.get('processed', 0)),
                        'listening_matched': int(lsum2.get('matched', 0)),
                    })
                except Exception:
                    pass

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
