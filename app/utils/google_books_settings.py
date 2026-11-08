"""Persisted Google Books API key (data/google_books_settings.json).

Lets the key be entered in the onboarding wizard / admin settings instead of .env.
Precedence: key saved in the GUI, then GOOGLE_BOOKS_API_KEY / GOOGLE_API_KEY env vars.
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Dict

_lock = threading.Lock()
_cache: Dict[str, Any] = {"mtime": None, "data": {}}


def _path() -> Path:
    return Path(os.getenv("GOOGLE_BOOKS_SETTINGS_FILE") or (Path.cwd() / "data" / "google_books_settings.json"))


def load() -> Dict[str, Any]:
    """Read settings; cached by file mtime so it is cheap to call per request and
    always reflects writes made by other gunicorn workers."""
    p = _path()
    try:
        mtime = p.stat().st_mtime_ns
    except Exception:
        return {}
    if _cache["mtime"] == mtime:
        return _cache["data"]
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            data = {}
    except Exception:
        data = {}
    _cache["mtime"], _cache["data"] = mtime, data
    return data


def save(*, api_key: str | None = None, skip_acknowledged: bool | None = None) -> None:
    with _lock:
        data = dict(load())
        if api_key is not None:
            data["api_key"] = api_key.strip()
            if data["api_key"]:
                data["skip_acknowledged"] = False
        if skip_acknowledged is not None:
            data["skip_acknowledged"] = bool(skip_acknowledged)
        p = _path()
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(f"{p.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, p)
        _cache["mtime"] = None


def get_api_key() -> str:
    key = (load().get("api_key") or "").strip()
    if key:
        return key
    return (os.getenv("GOOGLE_BOOKS_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()


def key_source() -> str:
    if (load().get("api_key") or "").strip():
        return "settings"
    if (os.getenv("GOOGLE_BOOKS_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip():
        return "environment"
    return "none"


def mask(key: str) -> str:
    return (key[:4] + "…" + key[-4:]) if len(key) > 10 else ("•" * len(key))
