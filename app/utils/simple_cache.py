import time
import threading
from typing import Any, Optional, Tuple, Dict


class TTLCache:
    """Very small in-process TTL cache suitable for single-worker setups."""
    def __init__(self):
        self._store: Dict[str, Tuple[Any, float]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        now = time.time()
        with self._lock:
            item = self._store.get(key)
            if not item:
                return None
            value, exp = item
            if exp < now:
                self._store.pop(key, None)
                return None
            return value

    def set(self, key: str, value: Any, ttl_seconds: int = 60) -> None:
        exp = time.time() + max(1, int(ttl_seconds))
        with self._lock:
            self._store[key] = (value, exp)

    def delete(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


_cache = TTLCache()
_user_versions: Dict[str, int] = {}
_version_lock = threading.Lock()


def cache_get(key: str) -> Optional[Any]:
    return _cache.get(key)


def cache_set(key: str, value: Any, ttl_seconds: int = 60) -> None:
    _cache.set(key, value, ttl_seconds)


def get_user_library_version(user_id: str) -> int:
    with _version_lock:
        return int(_user_versions.get(user_id, 0))


def bump_user_library_version(user_id: str) -> int:
    with _version_lock:
        current = int(_user_versions.get(user_id, 0)) + 1
        _user_versions[user_id] = current
        return current
