"""
Audiobookshelf Background Sync Runner

Provides a single background thread that processes ABS sync jobs from a queue
and also triggers scheduled syncs based on settings. Jobs are visible via the
existing SafeImportJobManager progress UI.
"""
from __future__ import annotations

import threading
import time
from collections import deque
from typing import Deque, Dict, Any, Optional, Tuple

from flask import current_app

from app.utils.audiobookshelf_settings import load_abs_settings, save_abs_settings
from app.services.audiobookshelf_service import get_client_from_settings, AudiobookShelfClient
from app.services.audiobookshelf_import_service import AudiobookshelfImportService
from app.services.audiobookshelf_listening_sync import AudiobookshelfListeningSync
from app.utils.safe_import_manager import safe_create_import_job, safe_update_import_job
import uuid


class _AbsSyncRunner:
    def __init__(self):
        self._queue: Deque[Tuple[str, Dict[str, Any]]] = deque()
        self._lock = threading.RLock()
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._app = None

    def ensure_started(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._running = True
            try:
                # Capture a concrete Flask app object from the active context
                self._app = current_app._get_current_object()  # type: ignore[attr-defined]
            except Exception:
                self._app = None
            self._thread = threading.Thread(target=self._run_loop, name="abs-sync-runner", daemon=True)
            self._thread.start()

    def enqueue_test_sync(self, user_id: str, library_ids: list[str], limit: int = 5) -> str:
        task_id = f"abs_test_{uuid.uuid4().hex[:8]}"
        self._create_job(user_id, task_id, 'abs_test_sync', total=limit)
        # Ensure background thread is running
        self.ensure_started()
        with self._lock:
            self._queue.append((task_id, {
                'kind': 'test', 'user_id': user_id, 'library_ids': library_ids, 'limit': limit
            }))
        return task_id
    
    def enqueue_user_composite_sync(self, user_id: str, page_size: int = 50, *, force_books: bool = False, force_listening: bool = False) -> str:
        """Enqueue a user composite sync (books first then listening).

        If force_books/force_listening are True, override per-user preferences.
        """
        task_id = f"abs_user_{uuid.uuid4().hex[:8]}"
        self._create_job(user_id, task_id, 'abs_user_composite', total=0)
        self.ensure_started()
        with self._lock:
            self._queue.append((task_id, {
                'kind': 'user_composite', 'user_id': user_id, 'page_size': page_size,
                'force_books': bool(force_books), 'force_listening': bool(force_listening)
            }))
        return task_id

    def enqueue_full_sync(self, user_id: str, library_ids: list[str], page_size: int = 50) -> str:
        task_id = f"abs_full_{uuid.uuid4().hex[:8]}"
        self._create_job(user_id, task_id, 'abs_full_sync', total=0)
        # Ensure background thread is running
        self.ensure_started()
        with self._lock:
            self._queue.append((task_id, {
                'kind': 'full', 'user_id': user_id, 'library_ids': library_ids, 'page_size': page_size
            }))
        return task_id

    def _create_job(self, user_id: str, task_id: str, job_type: str, total: int) -> None:
        safe_create_import_job(user_id, task_id, {
            'task_id': task_id,
            'type': job_type,
            'status': 'started',
            'created_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            'processed': 0,
            'total': total,
            'total_books': total,
            # Running counters for accurate, uncapped totals in progress UI
            'success': 0,
            'merged': 0,
            'errors': 0,
            'skipped': 0,
            'unmatched': 0,
            'success_titles': [],
            'error_messages': [],
            'processed_books': []
        })

    def _run_loop(self) -> None:
        # Run inside Flask application context
        app = self._app
        if app is None:
            return
        ctx = app.app_context()
        ctx.push()
        try:
            while self._running:
                # Scheduled sync check every 60 seconds
                try:
                    self._maybe_schedule_automatic_sync()
                except Exception:
                    pass

                task: Optional[Tuple[str, Dict[str, Any]]] = None
                with self._lock:
                    if self._queue:
                        task = self._queue.popleft()
                if task is None:
                    time.sleep(1.0)
                    continue
                task_id, payload = task
                try:
                    settings = load_abs_settings()
                    client = get_client_from_settings(settings)
                    if not client:
                        # Mark job as failed so UI isn't stuck at started
                        try:
                            safe_update_import_job(payload['user_id'], task_id, {
                                'status': 'failed',
                                'error_messages': ['ABS not configured: base_url or api_key missing']
                            })
                        except Exception:
                            pass
                        continue
                    kind = payload.get('kind')
                    if kind == 'listen':
                        # Prefer per-user API key override for listening-only jobs as well
                        eff_client = client
                        try:
                            from app.utils.user_settings import load_user_settings
                            u = load_user_settings(payload['user_id'])
                        except Exception:
                            u = {}
                        base_url = settings.get('base_url') or ''
                        user_api_key = (u.get('abs_api_key') or '').strip() if isinstance(u, dict) else ''
                        if base_url and user_api_key:
                            eff_client = AudiobookShelfClient(base_url, user_api_key)

                        try:
                            current_app.logger.info(
                                f"[ABS Listen] Starting listening job task={task_id} user={payload['user_id']} page_size={int(payload.get('page_size') or 200)}"
                            )
                            # Nudge progress UI
                            safe_update_import_job(payload['user_id'], task_id, {
                                'status': 'running',
                                'recent_activity': ['Starting listening sync...']
                            })
                        except Exception:
                            pass
                        listener = AudiobookshelfListeningSync(payload['user_id'], eff_client)
                        def _cb(snapshot: dict):
                            try:
                                safe_update_import_job(payload['user_id'], task_id, {
                                    'status': 'running',
                                    'processed': int(snapshot.get('processed') or 0),
                                    'total': int(snapshot.get('total') or 0),
                                    'matched': int(snapshot.get('matched') or 0)
                                })
                            except Exception:
                                pass
                        summary = listener.sync(page_size=int(payload.get('page_size') or 200), progress_cb=_cb)
                        try:
                            current_app.logger.info(
                                f"[ABS Listen] Finished task={task_id} user={payload['user_id']} processed={summary.get('processed')} matched={summary.get('matched')}"
                            )
                            # Surface summary to UI
                            safe_update_import_job(payload['user_id'], task_id, {
                                'recent_activity': [
                                    f"Listening sync complete: processed {summary.get('processed', 0)} sessions; matched {summary.get('matched', 0)} books."
                                ],
                                'processed': int(summary.get('processed', 0)),
                                'total': int(summary.get('total', 0) or 0),
                                'listening_sessions': int(summary.get('processed', 0)),
                                'listening_matched': int(summary.get('matched', 0))
                            })
                        except Exception:
                            pass
                        try:
                            from datetime import datetime, timezone
                            save_abs_settings({'last_listening_sync': datetime.now(timezone.utc).isoformat()})
                        except Exception:
                            pass
                        # mark job completed
                        try:
                            safe_update_import_job(payload['user_id'], task_id, {
                                'status': 'completed', 'processed': 0, 'total': 0
                            })
                        except Exception:
                            pass
                    elif kind == 'user_composite':
                        # Optional per-user API key override
                        try:
                            from app.utils.user_settings import load_user_settings
                            u = load_user_settings(payload['user_id'])
                        except Exception:
                            u = {}
                        base_url = settings.get('base_url') or ''
                        user_api_key = (u.get('abs_api_key') or '').strip() if isinstance(u, dict) else ''
                        eff_client = client
                        if base_url and user_api_key:
                            eff_client = AudiobookShelfClient(base_url, user_api_key)
                        force_books = bool(payload.get('force_books'))
                        force_listening = bool(payload.get('force_listening'))
                        do_books = force_books or (bool(u.get('abs_sync_books')) if isinstance(u, dict) else False)
                        do_listen = force_listening or (bool(u.get('abs_sync_listening')) if isinstance(u, dict) else False)
                        enforce_order = bool(settings.get('enforce_book_first', True))
                        # Always run books first when enabled
                        if do_books:
                            svc = AudiobookshelfImportService(payload['user_id'], eff_client)
                            svc._run_full_sync_job(task_id, settings.get('library_ids') or [], int(payload.get('page_size') or 50))
                        if do_listen:
                            listener = AudiobookshelfListeningSync(payload['user_id'], eff_client)
                            def _cb(snapshot: dict):
                                try:
                                    safe_update_import_job(payload['user_id'], task_id, {
                                        'status': 'running',
                                        'listening_sessions': int(snapshot.get('processed') or 0),
                                        'listening_matched': int(snapshot.get('matched') or 0)
                                    })
                                except Exception:
                                    pass
                            listener.sync(page_size=int(payload.get('page_size') or 200), progress_cb=_cb)
                        try:
                            safe_update_import_job(payload['user_id'], task_id, {'status': 'completed'})
                        except Exception:
                            pass
                    else:
                        # Prefer per-user API key for test/full as well
                        eff_client = client
                        try:
                            from app.utils.user_settings import load_user_settings
                            u = load_user_settings(payload['user_id'])
                        except Exception:
                            u = {}
                        base_url = settings.get('base_url') or ''
                        user_api_key = (u.get('abs_api_key') or '').strip() if isinstance(u, dict) else ''
                        if base_url and user_api_key:
                            eff_client = AudiobookShelfClient(base_url, user_api_key)
                        svc = AudiobookshelfImportService(payload['user_id'], eff_client)
                        if kind == 'test':
                            svc._run_test_sync_job(task_id, payload.get('library_ids') or [], int(payload.get('limit') or 5))
                        else:
                            svc._run_full_sync_job(task_id, payload.get('library_ids') or [], int(payload.get('page_size') or 50))
                except Exception:
                    # Errors recorded inside service methods
                    pass
        finally:
            try:
                ctx.pop()
            except Exception:
                pass

    def _maybe_schedule_automatic_sync(self) -> None:
        settings = load_abs_settings()
        if not settings.get('auto_sync_enabled'):
            return
        hours = int(settings.get('library_sync_every_hours') or 24)
        last_ts = settings.get('last_library_sync')
        now = time.time()
        due = False
        try:
            if last_ts:
                # last_ts expected as epoch seconds or ISO; handle both
                if isinstance(last_ts, (int, float)):
                    due = (now - float(last_ts)) >= (hours * 3600)
                else:
                    # crude ISO parse: if older than interval, run
                    from datetime import datetime, timezone
                    dt = datetime.fromisoformat(str(last_ts).replace('Z', '+00:00'))
                    due = (datetime.now(timezone.utc) - dt).total_seconds() >= (hours * 3600)
            else:
                due = True
        except Exception:
            due = True
        if not due:
            return
        # Determine library IDs
        lib_ids = settings.get('library_ids') or []
        if isinstance(lib_ids, str):
            lib_ids = [s.strip() for s in lib_ids.split(',') if s.strip()]
        # Fan out to all users so per-user credentials and preferences are respected
        try:
            from app.services import user_service, run_async
            users = run_async(user_service.get_all_users(limit=1000))  # type: ignore[attr-defined]
        except Exception:
            users = []
        queued = 0
        if users:
            for u in users:
                try:
                    if not getattr(u, 'is_active', True):
                        continue
                    # When a scheduled run occurs, run both books and listening for the user
                    self.enqueue_user_composite_sync(str(getattr(u, 'id')), force_books=True, force_listening=True)
                    queued += 1
                except Exception:
                    continue
        else:
            # Fallback: run a single job under a system context (imports may still proceed without per-user prefs)
            user_id = '__system__'
            self.enqueue_full_sync(user_id, lib_ids, page_size=50)
        # Update last sync time
        try:
            save_abs_settings({'last_library_sync': now})
        except Exception:
            pass
        # Also run listening sessions sync if due
        listen_hours = int(settings.get('listening_sync_every_hours') or 12)
        last_listen = settings.get('last_listening_sync')
        listen_due = False
        try:
            if last_listen:
                if isinstance(last_listen, (int, float)):
                    listen_due = (now - float(last_listen)) >= (listen_hours * 3600)
                else:
                    from datetime import datetime, timezone
                    dt = datetime.fromisoformat(str(last_listen).replace('Z', '+00:00'))
                    listen_due = (datetime.now(timezone.utc) - dt).total_seconds() >= (listen_hours * 3600)
            else:
                listen_due = True
        except Exception:
            listen_due = True
        if listen_due:
            # Fan out listening syncs to all users
            try:
                if not users:
                    from app.services import user_service, run_async  # type: ignore[attr-defined]
                    users = run_async(user_service.get_all_users(limit=1000))
            except Exception:
                users = []
            for u in users or []:
                try:
                    if not getattr(u, 'is_active', True):
                        continue
                    self.enqueue_listening_sync(str(getattr(u, 'id')))
                except Exception:
                    continue
            try:
                save_abs_settings({'last_listening_sync': time.time()})
            except Exception:
                pass

    def enqueue_listening_sync(self, user_id: str, page_size: int = 200) -> str:
        """Enqueue a listening sessions sync for a specific user."""
        task_id = f"abs_listen_{uuid.uuid4().hex[:8]}"
        self._create_job(user_id, task_id, 'abs_listen_sync', total=0)
        self.ensure_started()
        with self._lock:
            self._queue.append((task_id, {
                'kind': 'listen', 'user_id': user_id, 'page_size': page_size
            }))
        return task_id


_runner_singleton = _AbsSyncRunner()


def get_abs_sync_runner() -> _AbsSyncRunner:
    return _runner_singleton


def ensure_abs_sync_runner() -> None:
    try:
        get_abs_sync_runner().ensure_started()
    except Exception:
        pass
