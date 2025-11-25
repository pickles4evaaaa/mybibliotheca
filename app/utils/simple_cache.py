import time
import threading
import functools
import asyncio
import hashlib
from typing import Any, Optional, Tuple, Dict, Callable, Union


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


def cached(ttl_seconds: int = 60, key_builder: Optional[Callable] = None):
    """
    Decorator to cache function results.
    Supports both sync and async functions.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Build cache key
            if key_builder:
                key = key_builder(*args, **kwargs)
            else:
                # Simple default key builder
                key_parts = [func.__module__, func.__name__]
                key_parts.extend([str(arg) for arg in args])
                key_parts.extend([f"{k}={v}" for k, v in sorted(kwargs.items())])
                key_str = ":".join(key_parts)
                key = hashlib.md5(key_str.encode()).hexdigest()

            # Check cache
            cached_value = cache_get(key)
            if cached_value is not None:
                return cached_value

            # Call function
            result = func(*args, **kwargs)
            
            # Store result
            cache_set(key, result, ttl_seconds)
            return result

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
             # Build cache key (same logic)
            if key_builder:
                key = key_builder(*args, **kwargs)
            else:
                key_parts = [func.__module__, func.__name__]
                key_parts.extend([str(arg) for arg in args])
                key_parts.extend([f"{k}={v}" for k, v in sorted(kwargs.items())])
                key_str = ":".join(key_parts)
                key = hashlib.md5(key_str.encode()).hexdigest()

            # Check cache
            cached_value = cache_get(key)
            if cached_value is not None:
                return cached_value

            # Call function
            result = await func(*args, **kwargs)
            
            # Store result
            cache_set(key, result, ttl_seconds)
            return result

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return wrapper
    return decorator


def cache_delete(key: str) -> None:
    _cache.delete(key)
