"""Lightweight "new release available" check against GitHub releases.

Design notes:
* Never called per request. ``get_status`` returns whatever is cached and, when
  the cache is stale, starts a single background thread to refresh it.
* Fails silently (debug log) on any network/JSON/rate-limit problem.
* Uses ETag / If-None-Match so unauthenticated rate limits are respected.
* Sends no identifying data, only ``User-Agent: MyBibliotheca/<version>``.
* Disabled with ``UPDATE_CHECK_ENABLED=false``.
* Cache lives in memory and is persisted to ``<DATA_DIR>/update_check.json``
  (best effort) so restarts do not re-hit GitHub.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

RELEASES_URL = "https://api.github.com/repos/pickles4evaaaa/mybibliotheca/releases"
RELEASES_PAGE = "https://github.com/pickles4evaaaa/mybibliotheca/releases"
DEFAULT_TTL_SECONDS = 6 * 3600
REQUEST_TIMEOUT = 5
_FAILURE_RETRY_SECONDS = 1800

_VERSION_RE = re.compile(
    r"^\s*[vV]?(\d+(?:\.\d+)*)(?:[-_.]?(a|alpha|b|beta|rc|c|pre|preview|dev)[-_.]?(\d*))?(?:[-+].*)?\s*$"
)
_PRE_ORDER = {"dev": 0, "a": 1, "alpha": 1, "b": 2, "beta": 2, "pre": 3, "preview": 3, "rc": 4, "c": 4}
_FINAL = 99

_lock = threading.Lock()
_state: Dict[str, Any] = {
    "latest": None, "url": None, "etag": None, "checked_at": None,
    "fetched_monotonic": 0.0, "loaded": False, "refreshing": False,
    "next_try": 0.0,
}


def parse_version(value: Any) -> Optional[Tuple[Tuple[int, ...], Tuple[int, int]]]:
    """Return a sortable key, or None when unparseable.

    Final releases sort after their pre-releases (2.2.1-beta < 2.2.1).
    """
    if not isinstance(value, str):
        return None
    m = _VERSION_RE.match(value)
    if not m:
        return None
    nums = [int(x) for x in m.group(1).split(".")]
    while len(nums) > 1 and nums[-1] == 0:
        nums.pop()
    if m.group(2):
        return tuple(nums), (_PRE_ORDER[m.group(2).lower()], int(m.group(3) or 0))
    return tuple(nums), (_FINAL, 0)


def is_prerelease_version(value: str) -> bool:
    key = parse_version(value)
    return bool(key and key[1][0] != _FINAL)


def is_newer(latest: str, current: str) -> bool:
    a, b = parse_version(latest), parse_version(current)
    return bool(a and b and a > b)


def is_enabled() -> bool:
    return os.getenv("UPDATE_CHECK_ENABLED", "true").strip().lower() not in ("0", "false", "no", "off")


def get_current_version() -> str:
    try:
        import tomllib
        path = Path(__file__).resolve().parents[2] / "pyproject.toml"
        with open(path, "rb") as f:
            return str(tomllib.load(f).get("project", {}).get("version") or "unknown")
    except Exception:
        return os.getenv("APP_VERSION", "unknown")


def _cache_file() -> Optional[Path]:
    try:
        from flask import current_app
        base = current_app.config.get("DATA_DIR")
    except Exception:
        base = None
    base = base or os.getenv("DATA_DIR")
    return Path(base) / "update_check.json" if base else None


def _load_persisted() -> None:
    if _state["loaded"]:
        return
    _state["loaded"] = True
    try:
        path = _cache_file()
        if path and path.exists():
            data = json.loads(path.read_text())
            for k in ("latest", "url", "etag", "checked_at"):
                _state[k] = data.get(k)
            ts = data.get("checked_ts")
            if isinstance(ts, (int, float)):
                _state["fetched_monotonic"] = time.monotonic() - max(0.0, time.time() - ts)
    except Exception as exc:
        logger.debug("update check: could not load cache: %s", exc)


def _persist(path: Optional[Path]) -> None:
    try:
        if path:
            path.write_text(json.dumps({
                "latest": _state["latest"], "url": _state["url"], "etag": _state["etag"],
                "checked_at": _state["checked_at"], "checked_ts": time.time(),
            }))
    except Exception as exc:
        logger.debug("update check: could not persist cache: %s", exc)


def _pick_release(releases: Any, current: str) -> Optional[Dict[str, Any]]:
    """Choose the newest eligible release from a /releases payload.

    Drafts are always skipped; pre-releases only count when the installed
    version is itself a pre-release.
    """
    if not isinstance(releases, list):
        return None
    allow_pre = is_prerelease_version(current)
    best, best_key = None, None
    for rel in releases:
        if not isinstance(rel, dict) or rel.get("draft"):
            continue
        key = parse_version(rel.get("tag_name"))
        if key is None:
            continue
        if (rel.get("prerelease") or key[1][0] != _FINAL) and not allow_pre:
            continue
        if best_key is None or key > best_key:
            best, best_key = rel, key
    return best


def refresh(current: Optional[str] = None, cache_path: Optional[Path] = None) -> None:
    """Fetch the release list now (blocking). Never raises."""
    current = current or get_current_version()
    try:
        headers = {"Accept": "application/vnd.github+json", "User-Agent": f"MyBibliotheca/{current}"}
        if _state["etag"]:
            headers["If-None-Match"] = _state["etag"]
        resp = requests.get(RELEASES_URL, headers=headers, params={"per_page": 30}, timeout=REQUEST_TIMEOUT)
        now_iso = datetime.now(timezone.utc).isoformat()
        if resp.status_code == 304:
            _state["checked_at"] = now_iso
        elif resp.status_code == 200:
            rel = _pick_release(resp.json(), current)
            _state["etag"] = resp.headers.get("ETag")
            _state["checked_at"] = now_iso
            if rel:
                _state["latest"] = str(rel["tag_name"])
                url = rel.get("html_url")
                _state["url"] = url if isinstance(url, str) and url.startswith("https://") else RELEASES_PAGE
            else:
                _state["latest"] = _state["url"] = None
        else:  # 403/429 rate limit, 5xx, ...
            logger.debug("update check: HTTP %s", resp.status_code)
            _state["next_try"] = time.monotonic() + _FAILURE_RETRY_SECONDS
            return
        _state["fetched_monotonic"] = time.monotonic()
        _persist(cache_path)
    except Exception as exc:  # network, timeout, bad JSON...
        logger.debug("update check failed: %s", exc)
        _state["next_try"] = time.monotonic() + _FAILURE_RETRY_SECONDS
    finally:
        _state["refreshing"] = False


def _is_stale(ttl: int) -> bool:
    now = time.monotonic()
    if now < _state["next_try"]:
        return False
    return not _state["checked_at"] or now - _state["fetched_monotonic"] >= ttl


def get_status(background: bool = True, ttl: Optional[int] = None) -> Dict[str, Any]:
    """Return cached status; schedule a refresh if stale.

    With ``background=True`` (default) this never blocks on the network.
    """
    current = get_current_version()
    out = {"current": current, "latest": None, "update_available": False,
           "url": None, "checked_at": None, "enabled": is_enabled()}
    if not out["enabled"]:
        return out
    if ttl is None:
        try:
            ttl = int(os.getenv("UPDATE_CHECK_TTL_SECONDS", DEFAULT_TTL_SECONDS))
        except ValueError:
            ttl = DEFAULT_TTL_SECONDS
    with _lock:
        _load_persisted()
        start = _is_stale(ttl) and not _state["refreshing"]
        if start:
            _state["refreshing"] = True
        path = _cache_file() if start else None
    if start:
        if background:
            threading.Thread(target=refresh, args=(current, path), daemon=True,
                             name="update-check").start()
        else:
            refresh(current, path)
    latest = _state["latest"]
    out.update(latest=latest, url=_state["url"], checked_at=_state["checked_at"],
               update_available=bool(latest and is_newer(latest, current)))
    return out


def reset_cache() -> None:
    """Testing helper."""
    with _lock:
        _state.update(latest=None, url=None, etag=None, checked_at=None, fetched_monotonic=0.0,
                      loaded=False, refreshing=False, next_try=0.0)
