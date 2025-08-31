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

from typing import Dict, Any, Optional, List, Callable
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
            # Accept epoch seconds (string/number). If value looks like ms (large), divide.
            val = float(ts)
            if val > 10_000_000_000:  # likely ms
                val = val / 1000.0
            return datetime.utcfromtimestamp(val).replace(tzinfo=timezone.utc)
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
        """Store progress in personal metadata JSON and set reading_status when appropriate.

        - Always persist progress_ms and last_listened_at
        - If the book isn't marked finished, set reading_status to 'currently_reading'
        - Ensure a start_date exists for the user/book
        """
        updates: Dict[str, Any] = {}
        updates['progress_ms'] = int(position_ms)
        if updated_at:
            updates['last_listened_at'] = updated_at.isoformat()
        try:
            # Check existing metadata to avoid overriding finished state
            meta = personal_metadata_service.get_personal_metadata(self.user_id, book_id) or {}
            rs = (meta.get('reading_status') or '').lower() if isinstance(meta.get('reading_status'), str) else meta.get('reading_status')
            finished = bool(meta.get('finish_date')) or (rs == 'read')
            if not finished:
                updates['reading_status'] = 'currently_reading'
            # Ensure start date is set once
            try:
                if not meta.get('start_date'):
                    personal_metadata_service.ensure_start_date(self.user_id, book_id, (updated_at or datetime.now(timezone.utc)))
            except Exception:
                pass

            self._dbg("Apply progress", {"book_id": book_id, "progress_ms": updates.get('progress_ms'), "updated_at": updates.get('last_listened_at'), "reading_status": updates.get('reading_status')})
            # Use personal metadata JSON blob for arbitrary keys
            personal_metadata_service.update_personal_metadata(
                self.user_id, book_id, custom_updates=updates, merge=True
            )
            self._dbg("Progress applied", {"book_id": book_id})
        except Exception as e:
            logger.warning(f"Listening progress update failed for {book_id}: {e}")

    def _log_minutes(self, book_id: str, minutes: int, when: Optional[datetime], pages: Optional[int] = None):
        """Best-effort: create/update a reading log entry for the day with minutes_read (and optional pages_read)."""
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
                pages_read=int(pages or 0),
                minutes_read=int(minutes),
                notes=None,
                created_at=datetime.now(timezone.utc),
            )
            created = svc.create_reading_log_sync(rl)
            if created and self.debug:
                self._dbg("Reading log created/updated", {"book_id": book_id, "date": d.isoformat(), "minutes": int(minutes), "pages": int(pages or 0)})
        except Exception:
            # Non-critical
            pass

    def sync(self, page_size: int = 100, seed_item_ids: Optional[List[str]] = None, progress_cb: Optional[Callable[[Dict[str, Any]], None]] = None) -> Dict[str, Any]:
        """Run an incremental listening sync for the user. Returns small summary dict."""
        settings = load_abs_settings()
        abs_user_id = self._ensure_abs_user_id(settings)
        last = settings.get('last_listening_sync')
        updated_after: Optional[str] = None
        if last is not None:
            updated_after = str(last)
        page = 0  # ABS API pages are 0-indexed
        processed = 0
        matched = 0
        expected_total: Optional[int] = None
        # Track last seen position per book to compute deltas when ABS doesn't provide listened ms
        last_pos_ms_by_book: Dict[str, int] = {}
        # Cache page_count per book to estimate pages during sessions
        page_count_cache: Dict[str, Optional[int]] = {}
        self._dbg("Start listening sync", {"user_id": self.user_id, "abs_user_id": abs_user_id, "updated_after": updated_after, "page_size": page_size})
        first_page_checked = False
        while True:
            res = self.client.list_user_sessions(user_id=abs_user_id, updated_after=updated_after, limit=page_size, page=page)
            sessions = res.get('sessions') or []
            try:
                tot = res.get('total')
                if isinstance(tot, (int, float)) and (expected_total is None):
                    expected_total = int(tot)
            except Exception:
                pass
            # Process oldest first for saner delta calculations
            try:
                sessions = sorted(
                    sessions,
                    key=lambda x: _parse_iso(x.get('updatedAt') or x.get('updated_at')) or datetime.now(timezone.utc)
                )
            except Exception:
                pass
            self._dbg("Fetched sessions page", {"page": page, "count": len(sessions)})
            if not sessions:
                # If page 0 and we used updated_after, retry once without filter to seed
                if not first_page_checked and page == 0 and updated_after:
                    self._dbg("Empty first page with updated_after; retrying without filter", {"updated_after": updated_after})
                    first_page_checked = True
                    updated_after = None
                    page = 0
                    continue
                # If we have seed item ids and no sessions, try per-item progress as a fallback
                if seed_item_ids:
                    try:
                        for sid in seed_item_ids:
                            try:
                                gp = self.client.get_user_progress_for_item(sid)
                                if not gp.get('ok'):
                                    continue
                                prog = gp.get('progress') or {}
                                # Build a minimal item for mapping by ABS id
                                item = {"id": sid}
                                book_id = self._find_book_id_by_abs_item(item)
                                if not book_id:
                                    continue
                                # Normalize progress
                                pos_ms: Optional[int] = None
                                for key in ('positionMs', 'position_ms', 'position', 'currentTimeMs'):
                                    v = prog.get(key)
                                    if isinstance(v, (int, float)):
                                        pos_ms = int(v)
                                        break
                                if pos_ms is None:
                                    for key in ('positionSec', 'position_seconds', 'currentTime'):
                                        v = prog.get(key)
                                        if isinstance(v, (int, float)):
                                            pos_ms = int(float(v) * 1000)
                                            break
                                updated_at = _parse_iso(prog.get('updatedAt') or prog.get('updated_at') or prog.get('lastUpdate') or prog.get('last_updated'))
                                # Seed a conservative reading log (position in minutes, at least 1)
                                if isinstance(pos_ms, int) and pos_ms > 0:
                                    self._apply_progress(book_id, pos_ms, updated_at)
                                    minutes = max(1, int(pos_ms // 60000))
                                    self._log_minutes(book_id, minutes, updated_at)
                                    matched += 1
                                    processed += 1
                            except Exception:
                                continue
                    except Exception:
                        pass
                # Emit progress snapshot before break
                if callable(progress_cb):
                    try:
                        progress_cb({"processed": processed, "matched": matched, "total": expected_total or 0})
                    except Exception:
                        pass
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
                    pos_ms: Optional[int] = None
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
                    # Detect finished state via flags or duration parity
                    finished_flag = False
                    for k in ('isFinished', 'finished', 'completed'):
                        val = s.get(k)
                        if isinstance(val, bool) and val:
                            finished_flag = True
                            break
                    # Try to infer using duration
                    duration_ms: Optional[int] = None
                    for k in ('durationMs', 'duration_ms', 'duration'):
                        dv = s.get(k)
                        if isinstance(dv, (int, float)):
                            duration_ms = int(float(dv))
                            break
                    if duration_ms is None:
                        md = (item.get('media') or {}).get('metadata') or {}
                        dm = (item.get('media') or {}).get('duration') or md.get('duration')
                        if isinstance(dm, (int, float)):
                            # ABS duration typically seconds
                            duration_ms = int(dm * 1000) if dm < 10_000_000 else int(dm)
                    if not finished_flag and isinstance(pos_ms, int) and isinstance(duration_ms, int) and duration_ms > 0:
                        # Consider finished if >= 98% of duration
                        try:
                            if pos_ms >= max(duration_ms - 60_000, int(duration_ms * 0.98)):
                                finished_flag = True
                        except Exception:
                            pass
                    # Derive minutes listened for ReadingLog
                    minutes_from_session: Optional[int] = None
                    for key in ('msPlayed', 'msListened', 'timeListenedMs', 'time_listened_ms', 'playedMs'):
                        v = s.get(key)
                        if isinstance(v, (int, float)) and v > 0:
                            minutes_from_session = max(1, int(v // 60000))
                            break
                    if minutes_from_session is None:
                        for key in ('secondsListened', 'timeListened', 'playedSeconds'):
                            v = s.get(key)
                            if isinstance(v, (int, float)) and v > 0:
                                minutes_from_session = max(1, int(float(v) // 60))
                                break
                    if minutes_from_session is None and isinstance(pos_ms, int):
                        prev = last_pos_ms_by_book.get(book_id)
                        if isinstance(prev, int):
                            delta = pos_ms - prev
                            # Ignore tiny jitter; clamp to a reasonable cap
                            if delta > 15_000:
                                minutes_from_session = max(1, min(int(delta // 60000), 240))
                        # Update last position regardless
                        last_pos_ms_by_book[book_id] = pos_ms if isinstance(pos_ms, int) else last_pos_ms_by_book.get(book_id, 0)
                    if minutes_from_session is None and finished_flag and isinstance(duration_ms, int) and duration_ms > 0:
                        minutes_from_session = max(1, int(duration_ms // 60000))

                    # Estimate pages if we know both duration and page_count
                    pages_from_session: Optional[int] = None
                    try:
                        if (minutes_from_session and minutes_from_session > 0) and isinstance(duration_ms, int) and duration_ms > 0:
                            # Fetch cached page_count for this book
                            if book_id not in page_count_cache:
                                try:
                                    qr = safe_execute_kuzu_query("MATCH (b:Book) WHERE b.id = $id RETURN b.page_count LIMIT 1", {"id": book_id})
                                    pc = None
                                    if isinstance(qr, list) and qr:
                                        first = qr[0]
                                        if isinstance(first, dict):
                                            pc = first.get('result') or first.get('b.page_count') or first.get('col_0')
                                        elif isinstance(first, (list, tuple)) and first:
                                            pc = first[0]
                                    page_count_cache[book_id] = int(pc) if isinstance(pc, (int, float)) else None
                                except Exception:
                                    page_count_cache[book_id] = None
                            pcache = page_count_cache.get(book_id)
                            if isinstance(pcache, int) and pcache > 0:
                                total_min = max(1, int(duration_ms // 60000))
                                pages_from_session = max(0, int(round((minutes_from_session / total_min) * pcache)))
                    except Exception:
                        pages_from_session = None

                    if minutes_from_session and minutes_from_session > 0:
                        self._log_minutes(book_id, minutes_from_session, updated_at, pages_from_session)

                    if pos_ms is not None:
                        if finished_flag:
                            try:
                                # Mark finished: set finish_date and reading_status='read'
                                personal_metadata_service.update_personal_metadata(
                                    self.user_id,
                                    book_id,
                                    finish_date=(updated_at or datetime.now(timezone.utc)),
                                    custom_updates={'reading_status': 'read'},
                                    merge=True,
                                )
                            except Exception:
                                pass
                        self._apply_progress(book_id, pos_ms, updated_at)
                        if not minutes_from_session:
                            self._dbg("No minutes derived from session (skipped logging)", {"book_id": book_id})
                except Exception as e:
                    logger.exception(f"Skipping session due to error: {e}")
                finally:
                    processed += 1
            # Emit progress at end of page
            if callable(progress_cb):
                try:
                    progress_cb({"processed": processed, "matched": matched, "total": expected_total or 0})
                except Exception:
                    pass
            page += 1

        # Record last sync time
        try:
            save_abs_settings({'last_listening_sync': datetime.now(timezone.utc).isoformat()})
        except Exception:
            pass
        summary = {"ok": True, "processed": processed, "matched": matched, "total": expected_total or 0}
        self._dbg("Listening sync complete", summary)
        return summary

    def sync_item_progress(self, item_or_id: Any) -> Dict[str, Any]:
        """Sync progress for a single ABS item (libraryItemId) for this user.

        - Resolves local Book by audiobookshelf_id/ISBN/title
        - Fetches per-item progress via /api/me/progress/{item_id}
        - Applies personal progress and best-effort reading minutes

        Returns a compact summary: { ok, processed, matched }
        """
        try:
            item_id = None
            if isinstance(item_or_id, dict):
                item_id = item_or_id.get('id') or item_or_id.get('_id') or item_or_id.get('itemId')
            else:
                item_id = str(item_or_id)
            if not item_id:
                return {"ok": False, "processed": 0, "matched": 0, "message": "missing item_id"}
            # Map to local Book
            book_id = self._find_book_id_by_abs_item({"id": item_id})
            if not book_id:
                self._dbg("sync_item_progress: no local match", {"item_id": item_id})
                return {"ok": True, "processed": 1, "matched": 0}
            gp = self.client.get_user_progress_for_item(item_id)
            if not gp.get('ok'):
                return {"ok": False, "processed": 1, "matched": 0, "message": gp.get('message')}
            prog = gp.get('progress') or {}
            # Normalize position
            pos_ms: Optional[int] = None
            for key in ('positionMs', 'position_ms', 'position', 'currentTimeMs'):
                v = prog.get(key)
                if isinstance(v, (int, float)):
                    pos_ms = int(v)
                    break
            if pos_ms is None:
                for key in ('positionSec', 'position_seconds', 'currentTime'):
                    v = prog.get(key)
                    if isinstance(v, (int, float)):
                        pos_ms = int(float(v) * 1000)
                        break
            updated_at = _parse_iso(prog.get('updatedAt') or prog.get('updated_at') or prog.get('lastUpdate') or prog.get('last_updated'))
            # Derive minutes from explicit listened time if available; otherwise from position
            minutes: Optional[int] = None
            for key in ('msPlayed', 'msListened', 'timeListenedMs', 'time_listened_ms', 'playedMs'):
                v = prog.get(key)
                if isinstance(v, (int, float)) and v > 0:
                    minutes = max(1, int(v // 60000))
                    break
            if minutes is None:
                for key in ('secondsListened', 'timeListened', 'playedSeconds'):
                    v = prog.get(key)
                    if isinstance(v, (int, float)) and v > 0:
                        minutes = max(1, int(float(v) // 60))
                        break
            if minutes is None and isinstance(pos_ms, int):
                minutes = max(1, int(pos_ms // 60000))

            if isinstance(pos_ms, int):
                self._apply_progress(book_id, pos_ms, updated_at)
            if isinstance(minutes, int) and minutes > 0:
                self._log_minutes(book_id, minutes, updated_at)
            self._dbg("sync_item_progress: applied", {"item_id": item_id, "book_id": book_id, "pos_ms": pos_ms, "minutes": minutes})
            return {"ok": True, "processed": 1, "matched": 1}
        except Exception as e:
            logger.warning(f"sync_item_progress failed: {e}")
            return {"ok": False, "processed": 0, "matched": 0, "message": str(e)}
