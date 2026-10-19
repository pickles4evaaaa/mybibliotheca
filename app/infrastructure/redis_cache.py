"""Small Redis cache adapter shared by the Kuzu transition and PostgreSQL app.

Cache failures are treated as misses.  Callers must use explicit tags for
mutation invalidation; this module never caches database writes by itself.
"""

from __future__ import annotations

import hashlib
import base64
import json
import logging
import os
import threading
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable

logger = logging.getLogger(__name__)
_CACHE_WIRE_MARKER = "$mybibliotheca_cache_value_v1"
_client: Any = None
_client_lock = threading.Lock()


def _redis_url() -> str | None:
    url = (os.getenv("CACHE_REDIS_URL") or os.getenv("REDIS_URL") or "").strip()
    return url or None


def get_redis_client() -> Any | None:
    """Return the configured shared Redis client, or None when not configured."""
    global _client
    url = _redis_url()
    if not url:
        return None
    if _client is not None:
        return _client
    with _client_lock:
        if _client is None:
            try:
                import redis

                client = redis.Redis.from_url(
                    url,
                    decode_responses=True,
                    socket_connect_timeout=float(os.getenv("CACHE_REDIS_CONNECT_TIMEOUT", "1.0")),
                    socket_timeout=float(os.getenv("CACHE_REDIS_SOCKET_TIMEOUT", "1.0")),
                    health_check_interval=30,
                )
                client.ping()
                _client = client
            except Exception as exc:
                logger.warning("Redis cache unavailable; bypassing cache: %s", exc)
                return None
    return _client


def _encode(value: Any) -> str:
    def tagged(kind: str, payload: Any = None) -> list[Any]:
        return [_CACHE_WIRE_MARKER, kind, payload]

    def encode(item: Any) -> list[Any]:
        if item is None:
            return tagged("none")
        if isinstance(item, bool):
            return tagged("bool", item)
        if isinstance(item, str):
            return tagged("str", item)
        if isinstance(item, int):
            return tagged("int", str(item))
        if isinstance(item, float):
            return tagged("float", repr(item))
        if isinstance(item, datetime):
            return tagged("datetime", item.isoformat())
        if isinstance(item, date):
            return tagged("date", item.isoformat())
        if isinstance(item, Decimal):
            return tagged("decimal", str(item))
        if isinstance(item, bytes):
            return tagged("bytes", base64.b64encode(item).decode("ascii"))
        if isinstance(item, dict):
            pairs = [(encode(key), encode(value)) for key, value in item.items()]
            pairs.sort(key=lambda pair: json.dumps(pair[0], ensure_ascii=False, separators=(",", ":")))
            return tagged("map", pairs)
        if isinstance(item, list):
            return tagged("list", [encode(value) for value in item])
        if isinstance(item, tuple):
            return tagged("tuple", [encode(value) for value in item])
        raise TypeError(f"Unsupported cache value: {type(item).__name__}")

    return json.dumps(encode(value), ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def _decode(value: str) -> Any:
    def decode(item: Any) -> Any:
        if not isinstance(item, list) or len(item) != 3 or item[0] != _CACHE_WIRE_MARKER:
            raise ValueError("Invalid tagged Redis cache value")
        kind, raw = item[1], item[2]
        if kind == "none":
            return None
        if kind == "bool":
            return bool(raw)
        if kind == "str":
            return str(raw)
        if kind == "int":
            return int(raw)
        if kind == "float":
            return float(raw)
        if kind == "datetime":
            return datetime.fromisoformat(raw)
        if kind == "date":
            return date.fromisoformat(raw)
        if kind == "decimal":
            return Decimal(raw)
        if kind == "bytes":
            return base64.b64decode(raw, validate=True)
        if kind == "map":
            return {decode(key): decode(value) for key, value in raw}
        if kind == "list":
            return [decode(value) for value in raw]
        if kind == "tuple":
            return tuple(decode(value) for value in raw)
        raise ValueError(f"Unknown tagged Redis cache type: {kind!r}")

    return decode(json.loads(value))


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _cache_prefix() -> str:
    """Namespace Redis keys by the PostgreSQL database they cache.

    A shared Redis instance can serve multiple MyBibliotheca stacks. Including
    the database identity prevents one stack (or a disposable migration test)
    from returning another database's user-scoped rows under identical IDs.
    Credentials are used only as hash input and never appear in Redis keys.
    """
    configured_prefix = (os.getenv("CACHE_REDIS_PREFIX") or "mybibliotheca").strip(":")
    dsn = (os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL") or "").strip()
    if dsn:
        database_identity = {"dsn": dsn}
    else:
        database_identity = {
            "host": (os.getenv("POSTGRES_HOST") or "").strip().lower(),
            "port": (os.getenv("POSTGRES_PORT") or "5432").strip(),
            "database": (os.getenv("POSTGRES_DB") or "").strip(),
            "user": (os.getenv("POSTGRES_USER") or "").strip(),
        }
    identity_hash = _digest(json.dumps(database_identity, sort_keys=True, separators=(",", ":")))[:16]
    return f"{configured_prefix}:{identity_hash}"


def _tag_generation_keys(tags: Iterable[str]) -> list[str]:
    prefix = _cache_prefix()
    return [f"{prefix}:cache:tag-version:{_digest(str(tag))}" for tag in sorted(set(tags))]


def snapshot_tag_versions(tags: Iterable[str]) -> tuple[str | None, ...] | None:
    """Capture invalidation generations before a database read starts."""
    client = get_redis_client()
    if client is None:
        return None
    try:
        version_keys = _tag_generation_keys(tuple(tags))
        versions = client.mget(version_keys) if version_keys else []
        return tuple(versions)
    except Exception as exc:
        logger.warning("Redis cache generation lookup failed: %s", exc)
        return None


def tag_versions_current(tags: Iterable[str], snapshot: tuple[str | None, ...] | None) -> bool:
    """Check that no tagged write completed during a cached database read."""
    if snapshot is None:
        return False
    return snapshot_tag_versions(tags) == snapshot


def _versioned_key(
    key: str,
    tags: tuple[str, ...],
    snapshot: tuple[str | None, ...] | None = None,
) -> str | None:
    if snapshot is None:
        snapshot = snapshot_tag_versions(tags)
    if snapshot is None:
        return None
    try:
        key_material = json.dumps([str(key), snapshot], separators=(",", ":"))
        prefix = _cache_prefix()
        return f"{prefix}:cache:data:{_digest(key_material)}"
    except Exception as exc:
        logger.warning("Redis cache key lookup failed; bypassing cache: %s", exc)
        return None


def get_json(
    key: str,
    *,
    tags: Iterable[str] = (),
    snapshot: tuple[str | None, ...] | None = None,
) -> tuple[bool, Any]:
    """Return (hit, value); Redis errors become a cache miss."""
    normalized_tags = tuple(tags)
    redis_key = _versioned_key(key, normalized_tags, snapshot)
    if redis_key is None:
        return False, None
    try:
        value = get_redis_client().get(redis_key)
        return (False, None) if value is None else (True, _decode(value))
    except Exception as exc:
        logger.warning("Redis cache read failed; bypassing cache: %s", exc)
        return False, None


def set_json(
    key: str,
    value: Any,
    *,
    ttl_seconds: int = 30,
    tags: Iterable[str] = (),
    snapshot: tuple[str | None, ...] | None = None,
) -> bool:
    """Store a JSON-compatible value under the current tag generations."""
    normalized_tags = tuple(tags)
    redis_key = _versioned_key(key, normalized_tags, snapshot)
    if redis_key is None:
        return False
    try:
        if snapshot is not None and not tag_versions_current(normalized_tags, snapshot):
            return False
        ttl = max(1, int(ttl_seconds))
        stored = bool(get_redis_client().set(redis_key, _encode(value), ex=ttl))
        # Close the race where a write invalidates the tag while this value is
        # being serialized and stored.
        if stored and snapshot is not None and not tag_versions_current(normalized_tags, snapshot):
            get_redis_client().delete(redis_key)
            return False
        return stored
    except Exception as exc:
        logger.warning("Redis cache write failed; bypassing cache: %s", exc)
        return False


def invalidate_tags(*tags: str) -> bool:
    """Invalidate all cache entries associated with any supplied tag."""
    client = get_redis_client()
    if client is None or not tags:
        return False
    try:
        pipe = client.pipeline(transaction=True)
        for version_key in _tag_generation_keys(tags):
            pipe.incr(version_key)
        pipe.execute()
        return True
    except Exception as exc:
        # A failed invalidation must be surfaced to the data-service caller;
        # silently presenting stale private library data is not acceptable.
        logger.exception("Redis cache invalidation failed for tags %s", tags)
        raise RuntimeError("Redis cache invalidation failed") from exc


def reset_client_for_tests() -> None:
    """Clear the lazy client singleton; intended only for isolated tests."""
    global _client
    with _client_lock:
        _client = None
