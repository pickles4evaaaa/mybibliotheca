"""PostgreSQL implementation of location and STORED_AT relationship operations."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from ..domain.models import Location
from ..infrastructure import redis_cache
from ..infrastructure.postgres_store import PostgresStore, get_postgres_store

_LOCATION_COLUMNS = ("id", "owner_user_id", "name", "description", "location_type", "address",
                     "is_default", "is_active", "created_at", "updated_at")
_LOCATION_LINKS = """(
    SELECT bl.book_id,bl.location_id FROM book_locations bl
    UNION ALL
    SELECT e.book_id,CASE WHEN e.source_label='Book' THEN e.target_id ELSE e.source_id END AS location_id
    FROM book_relationship_edges e
    WHERE e.relationship='STORED_AT'
      AND ((e.source_label='Book' AND e.target_label='Location') OR (e.source_label='Location' AND e.target_label='Book'))
      AND NOT EXISTS (SELECT 1 FROM book_location_overrides o WHERE o.book_id=e.book_id)
)"""


def _location_dict(row: Any) -> dict[str, Any]:
    return dict(zip(_LOCATION_COLUMNS, row))


def _location(row: Any) -> Location:
    item = _location_dict(row)
    return Location(
        id=str(item["id"]), user_id=str(item.get("owner_user_id") or ""),
        name=item.get("name") or "", description=item.get("description"),
        location_type=item.get("location_type") or "home", address=item.get("address"),
        is_default=bool(item.get("is_default")), is_active=True if item.get("is_active") is None else bool(item["is_active"]),
        created_at=item.get("created_at") or datetime.now(timezone.utc),
        updated_at=item.get("updated_at") or item.get("created_at") or datetime.now(timezone.utc),
    )


def _location_cache(item: Location) -> dict[str, Any]:
    return {"id": item.id, "user_id": item.user_id, "name": item.name,
            "description": item.description, "location_type": item.location_type,
            "address": item.address, "is_default": item.is_default, "is_active": item.is_active,
            "created_at": item.created_at, "updated_at": item.updated_at}


def _location_from_cache(item: dict[str, Any]) -> Location:
    return Location(**item)


class PostgresLocationService:
    """Location service preserving the existing global-location behavior."""

    def __init__(self, store: PostgresStore | None = None):
        self.store = store or get_postgres_store()

    @staticmethod
    def _cached_query(key: str, tags: tuple[str, ...], callback):
        snapshot = redis_cache.snapshot_tag_versions(tags)
        hit, cached = redis_cache.get_json(key, tags=tags, snapshot=snapshot)
        if hit:
            return cached
        value = callback()
        redis_cache.set_json(key, value, ttl_seconds=30, tags=tags, snapshot=snapshot)
        return value

    @staticmethod
    def _invalidate(location_id: str | None = None) -> None:
        tags = ["postgres:locations", "postgres:books"]
        if location_id:
            tags.append(f"postgres:location:{location_id}")
        redis_cache.invalidate_tags(*tags)

    def _location_row(self, location_id: str) -> dict[str, Any] | None:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(f"SELECT {', '.join(_LOCATION_COLUMNS)} FROM locations WHERE id=%s", (location_id,))
            row = cursor.fetchone()
        return _location_dict(row) if row else None

    def create_location(self, name: str, description: str | None = None, location_type: str = "home",
                        address: str | None = None, is_default: bool = False) -> Location:
        location_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        with self.store.transaction() as connection, connection.cursor() as cursor:
            if is_default:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("mybibliotheca:locations:default",))
                cursor.execute("UPDATE locations SET is_default=FALSE,updated_at=%s WHERE is_default", (now,))
            cursor.execute(
                "INSERT INTO locations(id,name,description,location_type,address,is_default,is_active,created_at,updated_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,TRUE,%s,%s) RETURNING " + ", ".join(_LOCATION_COLUMNS),
                (location_id, name.strip(), description, location_type, address, bool(is_default), now, now),
            )
            row = cursor.fetchone()
        self._invalidate(location_id)
        return _location(row)

    def get_location(self, location_id: str) -> Location | None:
        key = f"postgres:locations:item:{location_id}"
        row = self._cached_query(key, ("postgres:locations", f"postgres:location:{location_id}"),
                                 lambda: self._location_row(location_id))
        return _location_from_cache(row) if row else None

    def get_user_locations(self, user_id: str, active_only: bool = True) -> list[Location]:
        return self._get_locations(active_only=active_only, only_with_books=True)

    def get_all_locations(self, active_only: bool = True) -> list[Location]:
        return self._get_locations(active_only=active_only, only_with_books=False)

    def _get_locations(self, *, active_only: bool, only_with_books: bool) -> list[Location]:
        key = f"postgres:locations:list:{int(active_only)}:{int(only_with_books)}"
        tags = ("postgres:locations", "postgres:books")
        def read():
            where = " WHERE l.is_active IS TRUE" if active_only else ""
            join = " JOIN " + _LOCATION_LINKS + " links ON links.location_id=l.id" if only_with_books else ""
            with self.store.connection() as connection, connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT DISTINCT {', '.join('l.' + name for name in _LOCATION_COLUMNS)} "
                    "FROM locations l" + join + where + " ORDER BY l.is_default DESC NULLS LAST,l.name,l.id"
                )
                return [_location_cache(_location(row)) for row in cursor.fetchall()]
        return [_location_from_cache(row) for row in self._cached_query(key, tags, read)]

    def update_location(self, location_id: str, **updates: Any) -> Location | None:
        allowed = {"name", "description", "location_type", "address", "is_default", "is_active"}
        fields = {name: value for name, value in updates.items() if name in allowed}
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT is_default FROM locations WHERE id=%s FOR UPDATE", (location_id,))
            row = cursor.fetchone()
            if not row:
                return None
            if fields.get("is_default") is True and not row[0]:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", ("mybibliotheca:locations:default",))
                cursor.execute("UPDATE locations SET is_default=FALSE,updated_at=now() WHERE is_default AND id<>%s", (location_id,))
            if fields:
                assignments = ",".join(f"{name}=%s" for name in fields)
                cursor.execute(f"UPDATE locations SET {assignments},updated_at=now() WHERE id=%s",
                               (*fields.values(), location_id))
        if fields:
            self._invalidate(location_id)
        return self.get_location(location_id)

    def delete_location(self, location_id: str) -> bool:
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM locations WHERE id=%s FOR UPDATE", (location_id,))
            if not cursor.fetchone():
                return False
            cursor.execute("SELECT 1 FROM " + _LOCATION_LINKS + " WHERE location_id=%s LIMIT 1", (location_id,))
            if cursor.fetchone():
                return False
            cursor.execute("DELETE FROM locations WHERE id=%s", (location_id,))
            deleted = cursor.rowcount == 1
        if deleted:
            self._invalidate(location_id)
        return deleted

    def get_default_location(self) -> Location | None:
        key = "postgres:locations:default"
        def read():
            with self.store.connection() as connection, connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT {', '.join(_LOCATION_COLUMNS)} FROM locations "
                    "ORDER BY is_default DESC NULLS LAST,is_active DESC NULLS LAST,lower(name),id LIMIT 1"
                )
                row = cursor.fetchone()
            return _location_cache(_location(row)) if row else None
        item = self._cached_query(key, ("postgres:locations",), read)
        return _location_from_cache(item) if item else None

    def setup_default_locations(self) -> list[Location]:
        current = self.get_default_location()
        if current:
            return [current]
        return [self.create_location("Home", "Primary residence", "home", is_default=True)]

    def get_location_book_count(self, location_id: str, user_id: str | None = None) -> int:
        key = f"postgres:locations:count:{location_id}"
        def read():
            with self.store.connection() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT count(DISTINCT book_id) FROM " + _LOCATION_LINKS + " WHERE location_id=%s", (location_id,))
                return int(cursor.fetchone()[0])
        return self._cached_query(key, ("postgres:locations", "postgres:books", f"postgres:location:{location_id}"), read)

    def get_all_location_book_counts(self, user_id: str | None = None) -> dict[str, int]:
        key = "postgres:locations:counts:all"
        def read():
            with self.store.connection() as connection, connection.cursor() as cursor:
                cursor.execute(
                    "SELECT l.id,count(DISTINCT links.book_id) FROM locations l LEFT JOIN " + _LOCATION_LINKS +
                    " links ON links.location_id=l.id GROUP BY l.id"
                )
                return {str(location_id): int(count) for location_id, count in cursor.fetchall()}
        return self._cached_query(key, ("postgres:locations", "postgres:books"), read)

    def get_books_at_location(self, location_id: str, user_id: str | None = None) -> list[str]:
        key = f"postgres:locations:books:{location_id}"
        def read():
            with self.store.connection() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT DISTINCT book_id FROM " + _LOCATION_LINKS + " WHERE location_id=%s ORDER BY book_id", (location_id,))
                return [str(row[0]) for row in cursor.fetchall()]
        return self._cached_query(key, ("postgres:locations", "postgres:books", f"postgres:location:{location_id}"), read)

    def _ensure_book_override(self, cursor: Any, book_id: str) -> bool:
        cursor.execute("SELECT 1 FROM books WHERE id=%s FOR UPDATE", (book_id,))
        if not cursor.fetchone():
            return False
        cursor.execute("INSERT INTO book_location_overrides(book_id) VALUES (%s) ON CONFLICT(book_id) DO NOTHING RETURNING book_id", (book_id,))
        first_override = cursor.fetchone() is not None
        if first_override:
            cursor.execute("SELECT to_regclass('public.app_graph_edges')")
            if cursor.fetchone()[0] is not None:
                cursor.execute(
                    "INSERT INTO book_locations(book_id,location_id,created_at) "
                    "SELECT e.book_id,CASE WHEN e.source_label='Book' THEN e.target_id ELSE e.source_id END,now() "
                    "FROM book_relationship_edges e JOIN locations l ON l.id=CASE WHEN e.source_label='Book' THEN e.target_id ELSE e.source_id END "
                    "WHERE e.book_id=%s AND e.relationship='STORED_AT' "
                    "AND ((e.source_label='Book' AND e.target_label='Location') OR (e.source_label='Location' AND e.target_label='Book')) "
                    "ON CONFLICT(book_id,location_id) DO NOTHING",
                    (book_id,),
                )
        return True

    def add_book_to_location(self, book_id: str, location_id: str, user_id: str) -> bool:
        try:
            with self.store.transaction() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT 1 FROM locations WHERE id=%s", (location_id,))
                if not cursor.fetchone() or not self._ensure_book_override(cursor, book_id):
                    return False
                cursor.execute(
                    "INSERT INTO book_locations(book_id,location_id,created_at) VALUES (%s,%s,now()) ON CONFLICT DO NOTHING",
                    (book_id, location_id),
                )
            self._invalidate(location_id)
            return True
        except Exception:
            return False

    def remove_book_from_location(self, book_id: str, location_id: str, user_id: str) -> bool:
        try:
            with self.store.transaction() as connection, connection.cursor() as cursor:
                if not self._ensure_book_override(cursor, book_id):
                    return False
                cursor.execute("DELETE FROM book_locations WHERE book_id=%s AND location_id=%s", (book_id, location_id))
            self._invalidate(location_id)
            return True
        except Exception:
            return False

    def get_book_locations(self, book_id: str, user_id: str | None = None) -> list[Location]:
        key = f"postgres:locations:for-book:{book_id}"
        def read():
            with self.store.connection() as connection, connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT DISTINCT {', '.join('l.' + name for name in _LOCATION_COLUMNS)} "
                    "FROM locations l JOIN " + _LOCATION_LINKS + " links ON links.location_id=l.id "
                    "WHERE links.book_id=%s ORDER BY l.is_default DESC NULLS LAST,l.name,l.id",
                    (book_id,),
                )
                return [_location_cache(_location(row)) for row in cursor.fetchall()]
        return [_location_from_cache(row) for row in self._cached_query(key, ("postgres:locations", "postgres:books"), read)]

    def set_book_location(self, book_id: str, location_id: str | None, user_id: str) -> bool:
        try:
            with self.store.transaction() as connection, connection.cursor() as cursor:
                if location_id:
                    cursor.execute("SELECT 1 FROM locations WHERE id=%s", (location_id,))
                    if not cursor.fetchone():
                        return False
                if not self._ensure_book_override(cursor, book_id):
                    return False
                cursor.execute("DELETE FROM book_locations WHERE book_id=%s", (book_id,))
                if location_id:
                    cursor.execute("INSERT INTO book_locations(book_id,location_id,created_at) VALUES (%s,%s,now())", (book_id, location_id))
            self._invalidate(location_id)
            return True
        except Exception:
            return False

    def migrate_user_books_to_default_location(self, user_id: str) -> int:
        default_location = self.get_default_location()
        if not default_location or not default_location.id:
            return 0
        count = 0
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT book_id FROM user_books WHERE user_id=%s ORDER BY book_id", (user_id,))
            book_ids = [str(row[0]) for row in cursor.fetchall()]
            for book_id in book_ids:
                if not self._ensure_book_override(cursor, book_id):
                    continue
                cursor.execute("SELECT 1 FROM book_locations WHERE book_id=%s LIMIT 1", (book_id,))
                if cursor.fetchone():
                    continue
                cursor.execute("INSERT INTO book_locations(book_id,location_id,created_at) VALUES (%s,%s,now()) ON CONFLICT DO NOTHING",
                               (book_id, default_location.id))
                count += cursor.rowcount
        if count:
            self._invalidate(default_location.id)
        return count

    def assign_all_books_without_location(self, location_id: str) -> int:
        """Assign a default location without replacing existing assignments."""
        count = 0
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM locations WHERE id=%s AND is_active IS TRUE", (location_id,))
            if not cursor.fetchone():
                return 0
            cursor.execute(
                "SELECT b.id FROM books b WHERE NOT EXISTS (SELECT 1 FROM book_locations bl WHERE bl.book_id=b.id) "
                "AND NOT EXISTS (SELECT 1 FROM book_relationship_edges e WHERE e.book_id=b.id "
                "AND e.relationship='STORED_AT' AND NOT EXISTS (SELECT 1 FROM book_location_overrides o WHERE o.book_id=b.id)) "
                "ORDER BY b.id FOR UPDATE"
            )
            book_ids = [str(row[0]) for row in cursor.fetchall()]
            for book_id in book_ids:
                if self._ensure_book_override(cursor, book_id):
                    cursor.execute(
                        "INSERT INTO book_locations(book_id,location_id,created_at) VALUES (%s,%s,now()) "
                        "ON CONFLICT DO NOTHING",
                        (book_id, location_id),
                    )
                    count += cursor.rowcount
        if count:
            self._invalidate(location_id)
        return count


_location_service: PostgresLocationService | None = None


def get_postgres_location_service() -> PostgresLocationService:
    global _location_service
    if _location_service is None:
        _location_service = PostgresLocationService()
    return _location_service
