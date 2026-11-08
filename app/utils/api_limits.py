"""Awareness of third-party API limits (Google Books, OpenLibrary).

* Counts requests per day for each service and exposes them via ``get_status()``.
* Stops calling Google Books once its daily limit is reached (configured limit or a
  quota 429 from Google) until the quota resets (midnight Pacific time).
* Throttles OpenLibrary requests to a configurable rate (default 3 per 2 seconds).

Gunicorn runs several worker processes, so state is shared through a small JSON file
(``data/api_limits.json``) guarded by an ``flock``. The limits are enforced
transparently by wrapping ``requests.adapters.HTTPAdapter.send`` (see ``install``),
so every existing call site is covered without changes.

Environment variables:
    GOOGLE_BOOKS_DAILY_LIMIT          requests/day before we stop calling Google (default 1000, 0 = no local limit)
    OPENLIBRARY_MAX_REQUESTS          requests allowed per window (default 3)
    OPENLIBRARY_WINDOW_SECONDS        window length in seconds (default 2)  -> 1 req / 2s: set MAX_REQUESTS=1
    OPENLIBRARY_MAX_WAIT_SECONDS      longest a request waits for a slot before being skipped (default 8)
    API_LIMITS_ENABLED                set to "false" to disable all of the above
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

try:  # POSIX only; Windows would simply fall back to the thread lock
    import fcntl
except Exception:  # pragma: no cover
    fcntl = None  # type: ignore

try:
    from zoneinfo import ZoneInfo
    _PACIFIC = ZoneInfo("America/Los_Angeles")
except Exception:  # pragma: no cover
    _PACIFIC = None

GOOGLE = "google_books"
OPENLIBRARY = "openlibrary"

_QUOTA_MARKERS = ("Queries per day", "quotaExceeded", "Quota exceeded", "RESOURCE_EXHAUSTED", "dailyLimitExceeded")
_RATE_COOLDOWN_SECONDS = 60

_thread_lock = threading.Lock()
_installed = False


def _enabled() -> bool:
    return (os.getenv("API_LIMITS_ENABLED") or "true").strip().lower() != "false"


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(str(os.getenv(name, default)).strip())
    except Exception:
        return default


def _state_path() -> Path:
    return Path(os.getenv("API_LIMITS_STATE_FILE") or (Path.cwd() / "data" / "api_limits.json"))


def _today() -> str:
    now = datetime.now(_PACIFIC) if _PACIFIC else datetime.utcnow()
    return now.strftime("%Y-%m-%d")


def _next_reset_epoch() -> float:
    """Google's daily quota resets at midnight Pacific time."""
    if _PACIFIC:
        now = datetime.now(_PACIFIC)
        nxt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return nxt.timestamp()
    now = datetime.utcnow()
    return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).timestamp()


class _Locked:
    """Cross-process (flock) + cross-thread lock around the state file."""

    def __enter__(self):
        _thread_lock.acquire()
        self._fh = None
        try:
            path = _state_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            if fcntl is not None:
                self._fh = open(str(path) + ".lock", "a")
                fcntl.flock(self._fh, fcntl.LOCK_EX)
        except Exception:
            self._fh = None
        return self

    def __exit__(self, *exc):
        try:
            if self._fh is not None:
                fcntl.flock(self._fh, fcntl.LOCK_UN)  # type: ignore[union-attr]
                self._fh.close()
        finally:
            _thread_lock.release()


def _load() -> Dict[str, Any]:
    try:
        with open(_state_path(), "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def _save(state: Dict[str, Any]) -> None:
    path = _state_path()
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f)
        os.replace(tmp, path)
    except Exception:
        try:
            tmp.unlink()
        except Exception:
            pass


def _service_state(state: Dict[str, Any], service: str) -> Dict[str, Any]:
    s = state.setdefault(service, {})
    today = _today()
    if s.get("day") != today:
        s["day"] = today
        s["count"] = 0
        s["throttled"] = 0
        s["blocked_count"] = 0
        s["total_429_today"] = 0
        # A quota block from yesterday is over once the day rolls (but keep short rate blocks)
        if s.get("blocked_until", 0) and s.get("blocked_reason") == "quota":
            s["blocked_until"] = 0
            s["blocked_reason"] = None
    return s


def classify(url: str) -> Optional[str]:
    """Which rate-limited service (if any) a URL belongs to."""
    try:
        p = urlparse(url)
    except Exception:
        return None
    host = (p.hostname or "").lower()
    path = p.path or ""
    if host == "www.googleapis.com" and path.startswith("/books/"):
        return GOOGLE
    if host == "openlibrary.org":
        return OPENLIBRARY
    return None


def google_daily_limit() -> int:
    return _env_int("GOOGLE_BOOKS_DAILY_LIMIT", 1000)


def google_has_key() -> bool:
    try:
        from app.utils.google_books_settings import get_api_key
        return bool(get_api_key())
    except Exception:
        return bool((os.getenv("GOOGLE_BOOKS_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip())


def clear_block(service: str = GOOGLE) -> None:
    """Forget a pause (e.g. after the user saves a new API key with its own quota)."""
    with _Locked():
        state = _load()
        s = _service_state(state, service)
        s["blocked_until"] = 0
        s["blocked_reason"] = None
        _save(state)


def _with_google_key(request) -> None:
    """Add the configured API key to Google Books requests that don't carry one."""
    try:
        url = request.url or ""
        if "key=" in url:
            return
        from app.utils.google_books_settings import get_api_key
        key = get_api_key()
        if key:
            request.url = url + ("&" if "?" in url else "?") + "key=" + key
    except Exception:
        pass


def is_blocked(service: str = GOOGLE) -> bool:
    """True if calls to this service should currently be skipped (cheap, read-only)."""
    if not _enabled():
        return False
    s = _load().get(service) or {}
    if s.get("day") != _today() and s.get("blocked_reason") == "quota":
        return False
    return float(s.get("blocked_until") or 0) > time.time()


def _google_acquire() -> Optional[str]:
    """Reserve one Google request. Returns None if allowed, else the reason it was refused."""
    limit = google_daily_limit()
    with _Locked():
        state = _load()
        s = _service_state(state, GOOGLE)
        now = time.time()
        if float(s.get("blocked_until") or 0) > now:
            s["blocked_count"] = s.get("blocked_count", 0) + 1
            _save(state)
            return s.get("blocked_reason") or "blocked"
        if limit > 0 and s.get("count", 0) >= limit:
            s["blocked_until"] = _next_reset_epoch()
            s["blocked_reason"] = "quota"
            s["blocked_count"] = s.get("blocked_count", 0) + 1
            _save(state)
            return "quota"
        s["count"] = s.get("count", 0) + 1
        s["last_request"] = now
        _save(state)
    return None


def _openlibrary_acquire() -> Optional[str]:
    """Reserve an OpenLibrary slot, waiting for one if necessary."""
    max_req = max(1, _env_int("OPENLIBRARY_MAX_REQUESTS", 3))
    window = max(0.1, _env_float("OPENLIBRARY_WINDOW_SECONDS", 2.0))
    max_wait = max(0.0, _env_float("OPENLIBRARY_MAX_WAIT_SECONDS", 8.0))
    deadline = time.monotonic() + max_wait
    waited = False
    while True:
        with _Locked():
            state = _load()
            s = _service_state(state, OPENLIBRARY)
            now = time.time()
            if float(s.get("blocked_until") or 0) > now:
                s["blocked_count"] = s.get("blocked_count", 0) + 1
                _save(state)
                return "rate_limited"
            recent = [t for t in s.get("recent", []) if now - t < window]
            if len(recent) < max_req:
                recent.append(now)
                s["recent"] = recent
                s["count"] = s.get("count", 0) + 1
                s["last_request"] = now
                if waited:
                    s["throttled"] = s.get("throttled", 0) + 1
                _save(state)
                return None
            wait = max(0.05, recent[0] + window - now)
            s["recent"] = recent
            _save(state)
        if time.monotonic() + wait > deadline:
            with _Locked():
                state = _load()
                s = _service_state(state, OPENLIBRARY)
                s["blocked_count"] = s.get("blocked_count", 0) + 1
                _save(state)
            return "throttle_timeout"
        waited = True
        time.sleep(wait)


def _record_429(service: str, body: str) -> None:
    quota = service == GOOGLE and any(m in (body or "") for m in _QUOTA_MARKERS)
    with _Locked():
        state = _load()
        s = _service_state(state, service)
        now = time.time()
        s["last_429"] = now
        s["total_429_today"] = s.get("total_429_today", 0) + 1
        if quota:
            s["blocked_until"] = _next_reset_epoch()
            s["blocked_reason"] = "quota"
        else:
            s["blocked_until"] = max(float(s.get("blocked_until") or 0), now + _RATE_COOLDOWN_SECONDS)
            s["blocked_reason"] = "rate_limited"
        _save(state)
    print(f"[API_LIMITS] {service} returned 429 (quota={quota}); pausing calls to it")


def _blocked_response(request, service: str, reason: str):
    import requests
    resp = requests.Response()
    resp.status_code = 429
    resp.url = getattr(request, "url", "")
    resp.reason = "Blocked locally by MyBibliotheca"
    resp.headers["X-MyBibliotheca-Blocked"] = f"{service}:{reason}"
    resp.headers["Content-Type"] = "application/json"
    resp._content = json.dumps({"error": {"code": 429, "message": f"{service} calls paused locally ({reason})"}}).encode()
    resp.request = request
    return resp


def install() -> None:
    """Wrap the requests transport so every Google Books/OpenLibrary call is counted and limited."""
    global _installed
    if _installed or not _enabled():
        return
    import requests.adapters

    original_send = requests.adapters.HTTPAdapter.send

    def send(self, request, *args, **kwargs):
        service = classify(getattr(request, "url", "") or "")
        if service is None:
            return original_send(self, request, *args, **kwargs)
        try:
            reason = _google_acquire() if service == GOOGLE else _openlibrary_acquire()
        except Exception:
            reason = None  # never let bookkeeping break a lookup
        if reason:
            return _blocked_response(request, service, reason)
        if service == GOOGLE:
            _with_google_key(request)
        resp = original_send(self, request, *args, **kwargs)
        try:
            if resp.status_code == 429:
                body = ""
                try:
                    body = resp.text[:2000]
                except Exception:
                    pass
                _record_429(service, body)
        except Exception:
            pass
        return resp

    requests.adapters.HTTPAdapter.send = send  # type: ignore[assignment]
    _installed = True


def get_status() -> Dict[str, Any]:
    """Snapshot of usage for the UI / API."""
    now = time.time()
    state = _load()
    out: Dict[str, Any] = {"enabled": _enabled(), "reset_day": _today()}

    g = state.get(GOOGLE) or {}
    same_day = g.get("day") == _today()
    count = int(g.get("count", 0)) if same_day else 0
    limit = google_daily_limit()
    blocked_until = float(g.get("blocked_until") or 0)
    blocked = blocked_until > now and (same_day or g.get("blocked_reason") != "quota")
    out[GOOGLE] = {
        "label": "Google Books",
        "api_key_configured": google_has_key(),
        "requests_today": count,
        "daily_limit": limit,
        "remaining": max(0, limit - count) if limit > 0 else None,
        "blocked": blocked,
        "blocked_reason": g.get("blocked_reason") if blocked else None,
        "blocked_until": datetime.fromtimestamp(blocked_until).isoformat() if blocked else None,
        "skipped_today": int(g.get("blocked_count", 0)) if same_day else 0,
        "rate_limited_today": int(g.get("total_429_today", 0)) if same_day else 0,
        "resets_at": datetime.fromtimestamp(_next_reset_epoch()).isoformat(),
    }

    o = state.get(OPENLIBRARY) or {}
    same_day = o.get("day") == _today()
    window = max(0.1, _env_float("OPENLIBRARY_WINDOW_SECONDS", 2.0))
    out[OPENLIBRARY] = {
        "label": "OpenLibrary",
        "requests_today": int(o.get("count", 0)) if same_day else 0,
        "max_requests": max(1, _env_int("OPENLIBRARY_MAX_REQUESTS", 3)),
        "window_seconds": window,
        "in_current_window": len([t for t in o.get("recent", []) if now - t < window]),
        "delayed_today": int(o.get("throttled", 0)) if same_day else 0,
        "skipped_today": int(o.get("blocked_count", 0)) if same_day else 0,
        "rate_limited_today": int(o.get("total_429_today", 0)) if same_day else 0,
        "blocked": float(o.get("blocked_until") or 0) > now,
    }
    return out
