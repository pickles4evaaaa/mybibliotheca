"""Background runner for OPDS sync jobs."""
from __future__ import annotations

import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Optional, Tuple
import traceback

from flask import current_app, has_request_context, url_for

from .opds_sync_service import opds_sync_service
from app.utils.opds_settings import load_opds_settings, save_opds_settings
from app.utils.safe_import_manager import safe_create_import_job, safe_update_import_job


@dataclass
class _QueuedItem:
    task_id: str
    payload: Dict[str, Any]


class _OpdsSyncRunner:
    """Single background thread responsible for OPDS sync operations."""

    def __init__(self) -> None:
        self._queue: Deque[_QueuedItem] = deque()
        self._lock = threading.RLock()
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._app = None
        self._last_scheduler_check = 0.0

    def _describe_exception(self, exc: Exception) -> str:
        message = str(exc).strip()
        if message:
            return f"{exc.__class__.__name__}: {message}"
        return exc.__class__.__name__

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def ensure_started(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._running = True
            try:
                self._app = current_app._get_current_object()  # type: ignore[attr-defined]
            except Exception:
                self._app = None
            self._thread = threading.Thread(target=self._run_loop, name="opds-sync-runner", daemon=True)
            self._thread.start()

    def enqueue_test_sync(self, user_id: str, limit: int = 10) -> Dict[str, Any]:
        task_id = f"opds_test_{uuid.uuid4().hex[:10]}"
        self._create_job(
            user_id,
            task_id,
            job_type="opds_test_sync",
            total=limit,
            metadata={"recent_activity": ["Queued test sync"]},
        )
        self.ensure_started()
        with self._lock:
            self._queue.append(_QueuedItem(task_id, {
                "kind": "test",
                "user_id": user_id,
                "limit": limit,
            }))
        return self._response_payload(user_id, task_id)

    def enqueue_sync(self, user_id: str, *, limit: Optional[int] = None, origin: str = "manual") -> Dict[str, Any]:
        task_id = f"opds_sync_{uuid.uuid4().hex[:10]}"
        self._create_job(
            user_id,
            task_id,
            job_type="opds_sync",
            total=0,
            metadata={"recent_activity": ["Queued OPDS sync"], "origin": origin},
        )
        self.ensure_started()
        with self._lock:
            self._queue.append(_QueuedItem(task_id, {
                "kind": "sync",
                "user_id": user_id,
                "limit": limit,
                "origin": origin,
            }))
        return self._response_payload(user_id, task_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _create_job(self, user_id: str, task_id: str, job_type: str, total: int, metadata: Optional[Dict[str, Any]] = None) -> None:
        payload = {
            "task_id": task_id,
            "type": job_type,
            "status": "queued",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "processed": 0,
            "total": total,
            "total_books": total,
            "success": 0,
            "errors": 0,
            "skipped": 0,
            "recent_activity": [],
        }
        if metadata:
            payload.update(metadata)
        safe_create_import_job(user_id, task_id, payload)

    def _response_payload(self, user_id: str, task_id: str) -> Dict[str, Any]:
        progress_url: Optional[str] = None
        api_progress_url: Optional[str] = None
        if has_request_context():
            try:
                progress_url = url_for('import.import_books_progress', task_id=task_id)
                api_progress_url = url_for('import.api_import_progress', task_id=task_id)
            except Exception:
                pass
        return {
            "ok": True,
            "task_id": task_id,
            "progress_url": progress_url,
            "api_progress_url": api_progress_url,
            "user_id": user_id,
        }

    def _run_loop(self) -> None:
        app = self._app
        if app is None:
            return
        ctx = app.app_context()
        ctx.push()
        try:
            while self._running:
                now = time.time()
                if now - self._last_scheduler_check >= 60:
                    self._last_scheduler_check = now
                    try:
                        self._maybe_schedule_automatic_sync()
                    except Exception:
                        pass

                item: Optional[_QueuedItem] = None
                with self._lock:
                    if self._queue:
                        item = self._queue.popleft()
                if item is None:
                    time.sleep(1.0)
                    continue
                try:
                    self._process_item(item)
                except Exception as exc:
                    current_app.logger.exception("OPDS sync runner error: %s", exc)
        finally:
            try:
                ctx.pop()
            except Exception:
                pass

    def _process_item(self, item: _QueuedItem) -> None:
        payload = item.payload
        user_id = str(payload.get("user_id") or "__system__")
        kind = payload.get("kind")
        settings = load_opds_settings()
        base_url = (settings.get("base_url") or "").strip()
        mapping = settings.get("mapping") or {}
        username = (settings.get("username") or "").strip() or None
        user_agent = (settings.get("user_agent") or "").strip() or None
        password = (settings.get("password") or "").strip() or None
        if not base_url or not mapping:
            safe_update_import_job(user_id, item.task_id, {
                "status": "failed",
                "error_messages": ["OPDS base URL and field mapping must be configured before syncing."],
            })
            return

        if kind == "test":
            limit = int(payload.get("limit") or 10)
            safe_update_import_job(user_id, item.task_id, {
                "status": "running",
                "recent_activity": ["Running OPDS test sync..."]
            })
            try:
                result = opds_sync_service.test_sync_sync(
                    base_url,
                    username=username,
                    password=password,
                    user_agent=user_agent,
                    mapping=mapping,
                    max_samples=limit,
                )
                summary = result.get("summary", {})
                preview = result.get("preview", [])
                processed = int(summary.get("would_create", 0) + summary.get("would_update", 0) + summary.get("skipped", 0))
                safe_update_import_job(user_id, item.task_id, {
                    "status": "completed",
                    "recent_activity": [
                        f"Test sync complete: would create {summary.get('would_create', 0)}, update {summary.get('would_update', 0)}, skip {summary.get('skipped', 0)}."
                    ],
                    "processed": processed,
                    "total": limit,
                    "preview": preview,
                    "summary": summary,
                })
                save_opds_settings({
                    "last_test_summary": {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "limit": limit,
                        "summary": summary,
                        "status": "completed",
                    },
                    "last_test_preview": preview[:limit],
                    "last_field_inventory": result.get("probe", {}).get("field_inventory", {}),
                    "last_test_task_id": None,
                    "last_test_task_api_url": None,
                    "last_test_task_progress_url": None,
                })
            except Exception as exc:
                error_details = self._describe_exception(exc)
                current_app.logger.exception("OPDS test sync failed (task_id=%s, user=%s)", item.task_id, user_id)
                error_trace = traceback.format_exc()
                safe_update_import_job(user_id, item.task_id, {
                    "status": "failed",
                    "error_messages": [f"Test sync failed: {error_details}"],
                    "recent_activity": [f"Test sync failed: {error_details}"],
                    "error_traceback": error_trace,
                })
                save_opds_settings({
                    "last_test_summary": {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "error": error_details,
                        "exception_type": exc.__class__.__name__,
                        "status": "failed",
                    },
                    "last_test_preview": [],
                    "last_test_task_id": None,
                    "last_test_task_api_url": None,
                    "last_test_task_progress_url": None,
                })
            return

        # Otherwise run a full sync
        limit = payload.get("limit")
        max_samples = None
        if limit is not None:
            try:
                max_samples = max(1, int(limit))
            except Exception:
                max_samples = None
        origin = payload.get("origin") or "manual"
        safe_update_import_job(user_id, item.task_id, {
            "status": "running",
            "recent_activity": ["OPDS sync running..."]
        })
        start_ts = datetime.now(timezone.utc).isoformat()
        try:
            result = opds_sync_service.quick_probe_sync_sync(
                base_url,
                username=username,
                password=password,
                user_agent=user_agent,
                mapping=mapping,
                max_samples=max_samples,
                user_id=user_id,
            )
            sync_result = result.get("sync", {})
            created = int(sync_result.get("created", 0))
            updated = int(sync_result.get("updated", 0))
            skipped = int(sync_result.get("skipped", 0))
            processed = created + updated + skipped
            safe_update_import_job(user_id, item.task_id, {
                "status": "completed",
                "processed": processed,
                "total": processed,
                "success": created + updated,
                "skipped": skipped,
                "errors": 0,
                "recent_activity": [
                    f"Sync complete: created {created}, updated {updated}, skipped {skipped}."
                ],
                "sync_result": sync_result,
            })
            settings_update = {
                "last_sync_summary": {
                    "created": created,
                    "updated": updated,
                    "skipped": skipped,
                    "timestamp": start_ts,
                    "book_ids": sync_result.get("book_ids", []),
                    "status": "completed",
                },
                "last_sync_at": start_ts,
                "last_sync_status": "completed",
                "last_field_inventory": result.get("probe", {}).get("field_inventory", {}),
                "last_sync_task_id": None,
                "last_sync_task_api_url": None,
                "last_sync_task_progress_url": None,
            }
            if origin == "auto":
                settings_update["last_auto_sync"] = start_ts
                settings_update["last_auto_sync_status"] = "completed"
            save_opds_settings(settings_update)
        except Exception as exc:
            error_details = self._describe_exception(exc)
            current_app.logger.exception(
                "OPDS sync failed (task_id=%s, user=%s, origin=%s)",
                item.task_id,
                user_id,
                origin,
            )
            safe_update_import_job(user_id, item.task_id, {
                "status": "failed",
                "error_messages": [f"OPDS sync failed: {error_details}"],
                "recent_activity": [f"Sync failed: {error_details}"],
                "error_traceback": traceback.format_exc(),
            })
            settings_update = {
                "last_sync_status": f"failed: {error_details}",
                "last_sync_at": start_ts,
                "last_sync_task_id": None,
                "last_sync_task_api_url": None,
                "last_sync_task_progress_url": None,
            }
            if origin == "auto":
                settings_update["last_auto_sync"] = start_ts
                settings_update["last_auto_sync_status"] = f"failed: {error_details}"
            save_opds_settings(settings_update)

    def _maybe_schedule_automatic_sync(self) -> None:
        settings = load_opds_settings()
        if not settings.get("auto_sync_enabled"):
            return
        base_url = (settings.get("base_url") or "").strip()
        mapping = settings.get("mapping") or {}
        if not base_url or not mapping:
            return
        try:
            interval_hours = max(1, int(settings.get("auto_sync_every_hours") or 24))
        except Exception:
            interval_hours = 24
        last_run = settings.get("last_auto_sync")
        due = False
        now = datetime.now(timezone.utc)
        if not last_run:
            due = True
        else:
            try:
                last_dt = datetime.fromisoformat(str(last_run).replace("Z", "+00:00"))
                due = (now - last_dt).total_seconds() >= interval_hours * 3600
            except Exception:
                due = True
        if not due:
            return
        user_id = str(settings.get("auto_sync_user_id") or "__system__")
        # Reuse the regular enqueue path so job bookkeeping is consistent
        self.enqueue_sync(user_id, origin="auto")


_runner_singleton = _OpdsSyncRunner()


def get_opds_sync_runner() -> _OpdsSyncRunner:
    return _runner_singleton


def ensure_opds_sync_runner() -> None:
    try:
        get_opds_sync_runner().ensure_started()
    except Exception:
        pass


__all__ = ["get_opds_sync_runner", "ensure_opds_sync_runner", "_OpdsSyncRunner"]
