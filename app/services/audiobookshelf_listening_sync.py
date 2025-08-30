"""
Audiobookshelf Listening Sessions Sync Service

Safely syncs listening sessions/progress from ABS into MyBibliotheca using
Kuzu-friendly operations. Data is written as:
 - PersonalMetadata JSON field: personal_custom_fields.progress_ms / last_listened_at
 - ReadingLog entries with minutes_read aggregated per day (optional best-effort)

Constraints:
 - Kuzu single connection/thread: use safe_execute_kuzu_query only from this thread
 - Idempotent updates keyed by (user_id, book_id)
 - Avoid heavy scans; prefer incremental updatedAfter when available
"""
from __future__ import annotations

from typing import Dict, Any, Optional, List
from datetime import datetime, timezone, date
import logging
import os

from app.services.audiobookshelf_service import AudiobookShelfClient
from app.utils.audiobookshelf_settings import load_abs_settings, save_abs_settings
from app.infrastructure.kuzu_graph import safe_execute_kuzu_query
from app.services.personal_metadata_service import personal_metadata_service

logger = logging.getLogger(__name__)


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        # Normalize Z to +00:00
        return datetime.fromisoformat(ts.replace('Z', '+00:00'))
    except Exception:
        try:
            return datetime.utcfromtimestamp(float(ts))
        except Exception:
            return None


class AudiobookshelfListeningSync:
    def __init__(self, user_id: str, client: AudiobookShelfClient):
        self.user_id = user_id
        self.client = client
        # Debug switch: set ABS_LISTENING_DEBUG=1 or settings['debug_listening_sync']=true
        try:
            settings = load_abs_settings()
            settings_debug = bool(settings.get('debug_listening_sync') or False)
        except Exception:
            settings_debug = False
        env_debug = (os.getenv('ABS_LISTENING_DEBUG') or '').lower() in ('1', 'true', 'yes', 'on')
        self.debug = bool(settings_debug or env_debug)

    def _dbg(self, msg: str, extra: Optional[Dict[str, Any]] = None):
        if self.debug:
            try:
                # Use ERROR so messages appear even when LOG_LEVEL=ERROR
                if extra is not None:
                    logger.error(f"[ABS LISTEN] {msg} | {extra}")
                else:
                    logger.error(f"[ABS LISTEN] {msg}")
            except Exception:
                pass

    def _find_book_id_by_abs_item(self, item: Dict[str, Any]) -> Optional[str]:
        """Resolve local Book.id by ABS identifiers: prefer audiobookshelf_id then ISBN then title/author."""
        abs_id = item.get('id') or item.get('_id') or item.get('itemId')
        if abs_id:
            try:
                if self.debug:
                    self._dbg("Attempt match by ABS id", {"abs_id": abs_id})
                qr = safe_execute_kuzu_query(
                    """
                    MATCH (b:Book) WHERE b.audiobookshelf_id = $abs_id RETURN b.id LIMIT 1
                    """,
                    {"abs_id": str(abs_id)}
                )
                # Normalize result
                bid = None
                if isinstance(qr, list):
                    if qr:
                        first = qr[0]
                        if isinstance(first, dict):
                            bid = first.get('result') or first.get('b.id') or first.get('col_0') or next(iter(first.values()), None)
                        elif isinstance(first, (list, tuple)) and first:
                            bid = first[0]
                elif hasattr(qr, 'has_next') and qr.has_next():
                    row = qr.get_next()
                    if isinstance(row, (list, tuple)) and row:
                        bid = row[0]
                if bid:
                    self._dbg("Matched by ABS id", {"book_id": bid})
                    return str(bid)
            except Exception:
                pass
        # ISBN fallback
        md = (item.get('media') or {}).get('metadata') or {}
        for key in ('isbn13', 'isbn', 'isbn10'):
            val = md.get(key)
            if not val:
                continue
            try:
                if self.debug:
                    self._dbg("Attempt match by ISBN", {"key": key, "value": val})
                qr = safe_execute_kuzu_query(
                    """MATCH (b:Book) WHERE b.isbn13 = $v OR b.isbn10 = $v RETURN b.id LIMIT 1""",
                    {"v": str(val)}
                )
                bid = None
                if isinstance(qr, list):
                    if qr:
                        first = qr[0]
                        if isinstance(first, dict):
                            bid = first.get('result') or first.get('b.id') or first.get('col_0') or next(iter(first.values()), None)
                        elif isinstance(first, (list, tuple)) and first:
                            bid = first[0]
                elif hasattr(qr, 'has_next') and qr.has_next():
                    row = qr.get_next()
                    if isinstance(row, (list, tuple)) and row:
                        bid = row[0]
                if bid:
                    self._dbg("Matched by ISBN", {"book_id": bid})
                    return str(bid)
            except Exception:
                continue
        # Title/author heuristic
        title = md.get('title') or item.get('title')
        author = None
        authors = md.get('authors') or []
        if isinstance(authors, list) and authors:
            a0 = authors[0]
            if isinstance(a0, dict):
                author = a0.get('name') or a0.get('title')
            else:
                author = str(a0)
        if title:
            try:
                if self.debug:
                    self._dbg("Attempt match by title/author", {"title": title, "author": author})
                params = {"title": str(title)}
                if author:
                    qr = safe_execute_kuzu_query(
                        """
                        MATCH (b:Book) WHERE toLower(b.title) = toLower($title)
                        MATCH (p:Person)-[:AUTHORED]->(b)
                        WHERE toLower(p.name) = toLower($author)
                        RETURN b.id LIMIT 1
                        """,
                        {"title": str(title), "author": str(author)}
                    )
                else:
                    qr = safe_execute_kuzu_query(
                        """MATCH (b:Book) WHERE toLower(b.title) = toLower($title) RETURN b.id LIMIT 1""",
                        params
                    )
                bid = None
                if isinstance(qr, list):
                    if qr:
                        first = qr[0]
                        if isinstance(first, dict):
                            bid = first.get('result') or first.get('b.id') or first.get('col_0') or next(iter(first.values()), None)
                        elif isinstance(first, (list, tuple)) and first:
                            bid = first[0]
                elif hasattr(qr, 'has_next') and qr.has_next():
                    row = qr.get_next()
                    if isinstance(row, (list, tuple)) and row:
                        bid = row[0]
                if bid:
                    self._dbg("Matched by title/author", {"book_id": bid})
                    return str(bid)
            except Exception:
                pass
        self._dbg("No book match for item", {"abs_id": abs_id, "title": title, "author": author})
        return None

    def _ensure_abs_user_id(self, settings: Dict[str, Any]) -> Optional[str]:
        """Resolve ABS user id if missing using /api/me and persist it for future calls."""
        abs_user_id = settings.get('abs_user_id') or None
        if abs_user_id:
            return abs_user_id
        # Try /api/me via client helper
        try:
            me = self.client.get_me()
            if me.get('ok'):
                data = me.get('user') or {}
                if isinstance(data, dict):
                    uid = data.get('id') or data.get('_id') or data.get('userId') or data.get('username')
                    if uid:
                        try:
                            save_abs_settings({'abs_user_id': uid})
                        except Exception:
                            pass
                        return str(uid)
        except Exception:
            pass
        return None

    def _apply_progress(self, book_id: str, position_ms: int, updated_at: Optional[datetime]):
        """Store progress in personal metadata JSON for the user/book."""
        updates: Dict[str, Any] = {}
        updates['progress_ms'] = int(position_ms)
        if updated_at:
            updates['last_listened_at'] = updated_at.isoformat()
        try:
            self._dbg("Apply progress", {"book_id": book_id, "progress_ms": updates.get('progress_ms'), "updated_at": updates.get('last_listened_at')})
            # Use personal metadata JSON blob for arbitrary keys
            personal_metadata_service.update_personal_metadata(
                self.user_id, book_id, custom_updates=updates, merge=True
            )
            self._dbg("Progress applied", {"book_id": book_id})
        except Exception as e:
            logger.warning(f"Listening progress update failed for {book_id}: {e}")

    def _log_minutes(self, book_id: str, minutes: int, when: Optional[datetime]):
        """Best-effort: create/update a reading log entry for the day with minutes_read."""
        try:
            if not minutes or minutes <= 0:
                return
            from app.domain.models import ReadingLog
            from app.services.kuzu_reading_log_service import KuzuReadingLogService
            svc = KuzuReadingLogService()
            d = (when or datetime.now(timezone.utc)).date()
            rl = ReadingLog(
                id=None,
                user_id=self.user_id,
                book_id=book_id,
                date=d,
                pages_read=0,
                minutes_read=int(minutes),
                notes=None,
                created_at=datetime.now(timezone.utc),
            )
            svc.create_reading_log_sync(rl)
        except Exception:
            # Non-critical
            pass

    def sync(self, page_size: int = 100) -> Dict[str, Any]:
        """Run an incremental listening sync for the user. Returns small summary dict."""
        settings = load_abs_settings()
        abs_user_id = self._ensure_abs_user_id(settings)
        last = settings.get('last_listening_sync')
        updated_after = None
        if last:
            # Pass through stored ISO or epoch seconds
            if isinstance(last, (int, float)):
                updated_after = str(last)
            else:
                updated_after = str(last)

        page = 0  # ABS API pages are 0-indexed
        processed = 0
        matched = 0
        self._dbg("Start listening sync", {"user_id": self.user_id, "abs_user_id": abs_user_id, "updated_after": updated_after, "page_size": page_size})
        first_page_checked = False
        while True:
            res = self.client.list_user_sessions(user_id=abs_user_id, updated_after=updated_after, limit=page_size, page=page)
            sessions = res.get('sessions') or []
            self._dbg("Fetched sessions page", {"page": page, "count": len(sessions)})
            if not sessions:
                # If page 0 and we used updated_after, retry once without filter to seed
                if not first_page_checked and page == 0 and updated_after:
                    self._dbg("Empty first page with updated_after; retrying without filter", {"updated_after": updated_after})
                    first_page_checked = True
                    updated_after = None
                    page = 0
                    continue
                break
            for s in sessions:
                try:
                    item = s.get('item') or s.get('libraryItem') or {}
                    # ABS playback session uses libraryItemId
                    if not item:
                        li_id = s.get('libraryItemId') or s.get('itemId')
                        if li_id:
                            item = {"id": li_id}
                    raw_pos = {k: s.get(k) for k in ('positionMs', 'position_ms', 'position', 'currentTimeMs', 'positionSec', 'position_seconds', 'currentTime') if k in s}
                    self._dbg("Session record", {"item_id": item.get('id') or item.get('_id') or item.get('itemId'), "raw_pos": raw_pos, "updated_at": s.get('updatedAt') or s.get('updated_at')})
                    book_id = self._find_book_id_by_abs_item(item)
                    if not book_id:
                        self._dbg("Skip: could not resolve book for session", None)
                        continue
                    matched += 1
                    # Normalize progress fields
                    pos_ms = None
                    for key in ('positionMs', 'position_ms', 'position', 'currentTimeMs'):
                        v = s.get(key)
                        if isinstance(v, (int, float)):
                            pos_ms = int(v)
                            break
                    if pos_ms is None:
                        # Sometimes seconds are provided
                        for key in ('positionSec', 'position_seconds', 'currentTime'):
                            v = s.get(key)
                            if isinstance(v, (int, float)):
                                pos_ms = int(float(v) * 1000)
                                break
                    updated_at = _parse_iso(s.get('updatedAt') or s.get('updated_at'))
                    if pos_ms is not None:
                        self._apply_progress(book_id, pos_ms, updated_at)
                        self._dbg("Reading log aggregation is disabled in sync; minutes not recorded", {"book_id": book_id})
                        # Approx minutes listened since last update not known from single event; skip aggregation here
                except Exception as e:
                    logger.exception(f"Skipping session due to error: {e}")
                finally:
                    processed += 1
            page += 1

        # Record last sync time
        try:
            save_abs_settings({'last_listening_sync': datetime.now(timezone.utc).isoformat()})
        except Exception:
            pass
        summary = {"ok": True, "processed": processed, "matched": matched}
        self._dbg("Listening sync complete", summary)
        return summary
