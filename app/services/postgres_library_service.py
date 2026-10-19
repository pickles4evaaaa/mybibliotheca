"""PostgreSQL and Redis-backed user library operations.

This service uses normalized ``books`` and ``user_books`` rows. It is a
backend component for the eventual PostgreSQL service facade; it is not
selected by the current Kuzu service registry yet.
"""

from __future__ import annotations

import json
from typing import Any

from ..infrastructure.postgres_store import PostgresStore, get_postgres_store
from ..infrastructure import redis_cache


_SORTS = {
    "title_asc": "b.title ASC, b.id ASC",
    "title_desc": "b.title DESC, b.id ASC",
    "date_added_desc": "ub.date_added DESC NULLS LAST, b.title ASC, b.id ASC",
    "date_added_asc": "ub.date_added ASC NULLS LAST, b.title ASC, b.id ASC",
    "rating_desc": "ub.user_rating DESC NULLS LAST, b.title ASC, b.id ASC",
    "published_date_asc": "b.published_date ASC NULLS LAST, b.title ASC, b.id ASC",
}

_LIBRARY_COLUMNS = (
    "ub.user_id", "ub.book_id", "ub.reading_status", "ub.date_added", "ub.start_date",
    "ub.finish_date", "ub.current_page", "ub.total_pages", "ub.ownership_status",
    "ub.media_type", "ub.borrowed_from", "ub.borrowed_from_user_id", "ub.borrowed_date",
    "ub.borrowed_due_date", "ub.loaned_to", "ub.loaned_to_user_id", "ub.loaned_date",
    "ub.loaned_due_date", "ub.location_id", "ub.user_rating", "ub.rating_date",
    "ub.user_review", "ub.review_date", "ub.is_review_spoiler", "ub.personal_notes",
    "ub.pace", "ub.character_driven", "ub.source", "ub.custom_metadata", "ub.created_at",
    "ub.updated_at", "b.title", "b.subtitle", "b.isbn13", "b.isbn10", "b.description",
    "b.published_date", "b.page_count", "b.language", "b.cover_url", "b.series",
    "b.series_volume", "b.series_order", "b.custom_metadata AS book_custom_metadata",
    "b.raw_categories", "b.media_type AS book_media_type", "b.extra AS book_extra",
)
_LIBRARY_KEYS = (
    "user_id", "book_id", "reading_status", "date_added", "start_date", "finish_date",
    "current_page", "total_pages", "ownership_status", "media_type", "borrowed_from",
    "borrowed_from_user_id", "borrowed_date", "borrowed_due_date", "loaned_to",
    "loaned_to_user_id", "loaned_date", "loaned_due_date", "location_id", "user_rating",
    "rating_date", "user_review", "review_date", "is_review_spoiler", "personal_notes",
    "pace", "character_driven", "source", "custom_metadata", "created_at", "updated_at",
    "title", "subtitle", "isbn13", "isbn10", "description", "published_date", "page_count",
    "language", "cover_url", "series", "series_volume", "series_order", "book_custom_metadata",
    "raw_categories", "book_media_type", "book_extra",
)


class PostgresLibraryService:
    """Normalized PostgreSQL persistence for user-library reads and writes."""

    def __init__(self, store: PostgresStore | None = None, *, cache_ttl_seconds: int = 30):
        self.store = store or get_postgres_store()
        self.cache_ttl_seconds = max(1, int(cache_ttl_seconds))

    @staticmethod
    def _user_tag(user_id: str) -> str:
        return f"postgres:user_books:{user_id}"

    @staticmethod
    def _with_book_id_aliases(row: dict[str, Any] | None) -> dict[str, Any] | None:
        """Keep legacy consumers working with normalized PostgreSQL rows."""
        if row is None:
            return None
        normalized = dict(row)
        book_id = normalized.get("uid") or normalized.get("id") or normalized.get("book_id")
        if book_id:
            normalized.setdefault("id", str(book_id))
            normalized.setdefault("uid", str(book_id))
        return normalized

    def _cached_query(self, key: str, tags: tuple[str, ...], callback):
        snapshot = redis_cache.snapshot_tag_versions(tags)
        hit, cached = redis_cache.get_json(key, tags=tags, snapshot=snapshot)
        if hit:
            return cached
        value = callback()
        redis_cache.set_json(
            key,
            value,
            ttl_seconds=self.cache_ttl_seconds,
            tags=tags,
            snapshot=snapshot,
        )
        return value

    def get_user_book_sync(self, user_id: str, book_id: str) -> dict[str, Any] | None:
        cache_key = f"postgres:library:item:{user_id}:{book_id}"
        tags = (self._user_tag(user_id), "postgres:books")

        def read():
            with self.store.connection() as connection, connection.cursor() as cursor:
                cursor.execute(
                    "SELECT " + ", ".join(_LIBRARY_COLUMNS) +
                    " FROM user_books ub JOIN books b ON b.id = ub.book_id "
                    "WHERE ub.user_id = %s AND ub.book_id = %s",
                    (user_id, book_id),
                )
                row = cursor.fetchone()
            return dict(zip(_LIBRARY_KEYS, row)) if row else None

        return self._with_book_id_aliases(self._cached_query(cache_key, tags, read))

    def get_books_with_user_overlay_paginated_sync(
        self,
        user_id: str,
        limit: int = 50,
        offset: int = 0,
        sort: str = "title_asc",
    ) -> list[dict[str, Any]]:
        limit = max(0, min(int(limit), 1000))
        offset = max(0, int(offset))
        order_by = _SORTS.get(sort, _SORTS["title_asc"])
        cache_key = f"postgres:library:page:{user_id}:{limit}:{offset}:{sort}"
        tags = (self._user_tag(user_id), "postgres:books")

        def read():
            with self.store.connection() as connection, connection.cursor() as cursor:
                cursor.execute(
                    "SELECT " + ", ".join(_LIBRARY_COLUMNS) +
                    " FROM user_books ub JOIN books b ON b.id = ub.book_id "
                    "WHERE ub.user_id = %s ORDER BY " + order_by + " LIMIT %s OFFSET %s",
                    (user_id, limit, offset),
                )
                return [dict(zip(_LIBRARY_KEYS, row)) for row in cursor.fetchall()]

        rows = self._cached_query(cache_key, tags, read)
        return [self._with_book_id_aliases(row) or row for row in rows]

    def search_user_books_sync(self, user_id: str, query: str, limit: int = 20) -> list[dict[str, Any]]:
        query = str(query or "").strip()
        limit = max(0, min(int(limit), 200))
        if not query or limit == 0:
            return []
        cache_key = f"postgres:library:search:{user_id}:{query.casefold()}:{limit}"
        tags = (self._user_tag(user_id), "postgres:books", "postgres:people")

        def read():
            pattern = f"%{query}%"
            with self.store.connection() as connection, connection.cursor() as cursor:
                cursor.execute(
                    "SELECT b.id AS book_id, b.title, b.subtitle, b.isbn13, b.isbn10, "
                    "ub.reading_status, ub.ownership_status, ub.user_rating, ub.date_added "
                    "FROM user_books ub JOIN books b ON b.id = ub.book_id "
                    "WHERE ub.user_id = %s AND (b.title ILIKE %s OR b.subtitle ILIKE %s "
                    "OR b.isbn13 ILIKE %s OR b.isbn10 ILIKE %s OR EXISTS ("
                    "SELECT 1 FROM book_contributors c JOIN people p ON p.id = c.person_id "
                    "WHERE c.book_id = b.id AND p.name ILIKE %s)) "
                    "ORDER BY b.title ASC, b.id ASC LIMIT %s",
                    (user_id, pattern, pattern, pattern, pattern, pattern, limit),
                )
                return [
                    dict(zip(("book_id", "title", "subtitle", "isbn13", "isbn10", "reading_status", "ownership_status", "user_rating", "date_added"), row))
                    for row in cursor.fetchall()
                ]

        rows = self._cached_query(cache_key, tags, read)
        return [self._with_book_id_aliases(row) or row for row in rows]

    def get_library_status_counts_sync(self, user_id: str) -> dict[str, int]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT COALESCE(reading_status, ''), count(*) FROM user_books "
                "WHERE user_id = %s GROUP BY reading_status",
                (user_id,),
            )
            counts = {str(status): int(count) for status, count in cursor.fetchall()}
        return counts

    def add_book_to_user_library_sync(
        self,
        user_id: str,
        book_id: str,
        reading_status: str = "library_only",
        ownership_status: str = "owned",
        locations: list[str] | None = None,
        custom_metadata: dict[str, Any] | None = None,
    ) -> bool:
        if locations and len(set(locations)) > 1:
            raise ValueError("A user-book row supports one primary location; migrate additional locations as relationships")
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO user_books (user_id, book_id, reading_status, ownership_status, location_id, "
                "custom_metadata, date_added, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s::jsonb, now(), now(), now()) "
                "ON CONFLICT (user_id, book_id) DO NOTHING RETURNING id",
                (user_id, book_id, reading_status, ownership_status, locations[0] if locations else None,
                 json.dumps(custom_metadata or {}, ensure_ascii=False, separators=(",", ":"))),
            )
            inserted = cursor.fetchone() is not None
        if inserted:
            redis_cache.invalidate_tags(self._user_tag(user_id), "postgres:books")
        return inserted

    def update_user_book_sync(self, user_id: str, book_id: str, updates: dict[str, Any]) -> bool:
        allowed = {
            "reading_status", "date_added", "start_date", "finish_date", "current_page",
            "total_pages", "ownership_status", "media_type", "borrowed_from", "borrowed_from_user_id",
            "borrowed_date", "borrowed_due_date", "loaned_to", "loaned_to_user_id", "loaned_date",
            "loaned_due_date", "location_id", "user_rating", "rating_date", "user_review",
            "review_date", "is_review_spoiler", "personal_notes", "pace", "character_driven",
            "source", "custom_metadata",
        }
        fields = [name for name in updates if name in allowed]
        if not fields:
            return False
        assignments = [f"{name} = %s::jsonb" if name == "custom_metadata" else f"{name} = %s" for name in fields]
        values = [
            json.dumps(updates[name], ensure_ascii=False, separators=(",", ":")) if name == "custom_metadata" else updates[name]
            for name in fields
        ]
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute(
                "UPDATE user_books SET " + ", ".join(assignments) +
                ", updated_at = now() WHERE user_id = %s AND book_id = %s",
                (*values, user_id, book_id),
            )
            changed = cursor.rowcount > 0
        if changed:
            redis_cache.invalidate_tags(self._user_tag(user_id), "postgres:books")
        return changed
