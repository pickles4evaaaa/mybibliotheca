"""PostgreSQL implementation of series queries and user-specific series notes."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from ..domain.models import Book, Series
from ..infrastructure import redis_cache
from ..infrastructure.postgres_store import PostgresStore, get_postgres_store
from .postgres_book_service import PostgresBookService

_SERIES_COLUMNS = ("id", "name", "normalized_name", "description", "user_cover", "cover_url",
                   "custom_cover", "generated_placeholder", "created_at", "book_count", "effective_cover")
_SERIES_LINKS = """(
    SELECT bs.book_id,bs.series_id,bs.volume_number,bs.volume_number_double,bs.series_order
    FROM book_series bs
    UNION ALL
    SELECT e.book_id,
           CASE WHEN e.source_label='Book' THEN e.target_id ELSE e.source_id END AS series_id,
           CASE WHEN e.properties->>'volume_number' ~ '^[+-]?[0-9]+$' THEN (e.properties->>'volume_number')::bigint END,
           CASE WHEN e.properties->>'volume_number_double' ~ '^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$'
                THEN (e.properties->>'volume_number_double')::double precision END,
           CASE WHEN e.properties->>'series_order' ~ '^[+-]?[0-9]+$' THEN (e.properties->>'series_order')::bigint END
    FROM book_relationship_edges e
    WHERE e.relationship='PART_OF_SERIES'
      AND ((e.source_label='Book' AND e.target_label='Series') OR (e.source_label='Series' AND e.target_label='Book'))
      AND NOT EXISTS (
          SELECT 1 FROM book_series bs
          WHERE bs.book_id=e.book_id
            AND bs.series_id=CASE WHEN e.source_label='Book' THEN e.target_id ELSE e.source_id END
      )
)"""
_SERIES_FROM = (
    "FROM series s LEFT JOIN " + _SERIES_LINKS + " bs ON bs.series_id=s.id "
    "LEFT JOIN LATERAL (SELECT b.cover_url FROM " + _SERIES_LINKS + " fb JOIN books b ON b.id=fb.book_id "
    "WHERE fb.series_id=s.id ORDER BY b.published_date ASC NULLS LAST,lower(b.title),b.id LIMIT 1) first_book ON TRUE "
)


def _series(row: Any) -> Series:
    item = Series(
        id=str(row[0]), name=row[1] or "", normalized_name=row[2] or "", description=row[3],
        user_cover=row[4], cover_url=row[4] or row[10], custom_cover=bool(row[4]),
        generated_placeholder=bool(row[7]), created_at=row[8],
    )
    item.book_count = int(row[9] or 0)
    return item


def _series_to_cache(item: Series) -> dict[str, Any]:
    return {
        "id": item.id, "name": item.name, "normalized_name": item.normalized_name,
        "description": item.description, "user_cover": item.user_cover, "cover_url": item.cover_url,
        "custom_cover": item.custom_cover, "generated_placeholder": item.generated_placeholder,
        "created_at": item.created_at, "book_count": int(getattr(item, "book_count", 0) or 0),
    }


def _series_from_cache(item: dict[str, Any]) -> Series:
    series = Series(**{key: item.get(key) for key in (
        "id", "name", "normalized_name", "description", "user_cover", "cover_url",
        "custom_cover", "generated_placeholder", "created_at",
    )})
    series.book_count = int(item.get("book_count") or 0)
    return series


class PostgresSeriesService:
    """Series API retaining the Kuzu service's public methods."""

    def __init__(self, user_id: str | None = None, store: PostgresStore | None = None):
        self.user_id = str(user_id or "series_service")
        self.store = store or get_postgres_store()
        self.book_service = PostgresBookService(self.store)

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
    def _invalidate(series_id: str | None = None, user_id: str | None = None) -> None:
        tags = ["postgres:series", "postgres:books"]
        if series_id:
            tags.append(f"postgres:series:{series_id}")
        if user_id:
            tags.append(f"postgres:user_series:{user_id}")
        redis_cache.invalidate_tags(*tags)

    def get_all_series(self, limit: int = 500) -> list[Series]:
        limit = max(0, min(int(limit), 5000))
        def read():
            with self.store.connection() as connection, connection.cursor() as cursor:
                cursor.execute(
                    "SELECT s.id,s.name,s.normalized_name,s.description,s.user_cover,s.cover_url,s.custom_cover,"
                    "s.generated_placeholder,s.created_at,count(DISTINCT bs.book_id),first_book.cover_url "
                    + _SERIES_FROM + "GROUP BY s.id,first_book.cover_url ORDER BY s.normalized_name NULLS LAST,s.id LIMIT %s",
                    (limit,),
                )
                return [_series_to_cache(_series(row)) for row in cursor.fetchall()]
        cached = self._cached_query(f"postgres:series:list:{limit}", ("postgres:series", "postgres:books"), read)
        return [_series_from_cache(item) for item in cached]

    async def get_all_series_async(self, limit: int = 500) -> list[Series]:
        return await asyncio.to_thread(self.get_all_series, limit)

    def get_series(self, series_id: str) -> Series | None:
        def read():
            with self.store.connection() as connection, connection.cursor() as cursor:
                cursor.execute(
                    "SELECT s.id,s.name,s.normalized_name,s.description,s.user_cover,s.cover_url,s.custom_cover,"
                    "s.generated_placeholder,s.created_at,count(DISTINCT bs.book_id),first_book.cover_url "
                    + _SERIES_FROM + "WHERE s.id=%s GROUP BY s.id,first_book.cover_url",
                    (series_id,),
                )
                row = cursor.fetchone()
            return _series_to_cache(_series(row)) if row else None
        cached = self._cached_query(f"postgres:series:item:{series_id}", ("postgres:series", f"postgres:series:{series_id}", "postgres:books"), read)
        return _series_from_cache(cached) if cached else None

    async def get_series_async(self, series_id: str) -> Series | None:
        return await asyncio.to_thread(self.get_series, series_id)

    def get_book_series_relationship_sync(self, book_id: str) -> dict[str, Any] | None:
        """Return the typed series edge and its ordering fields for a book."""
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT s.id,s.name,bs.volume_number,bs.volume_number_double,bs.series_order "
                "FROM book_series bs JOIN series s ON s.id=bs.series_id WHERE bs.book_id=%s "
                "UNION ALL "
                "SELECT s.id,s.name, "
                "CASE WHEN e.properties->>'volume_number' ~ '^[+-]?[0-9]+$' "
                "THEN (e.properties->>'volume_number')::bigint END, "
                "CASE WHEN e.properties->>'volume_number_double' ~ '^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$' "
                "THEN (e.properties->>'volume_number_double')::double precision END, "
                "CASE WHEN e.properties->>'series_order' ~ '^[+-]?[0-9]+$' "
                "THEN (e.properties->>'series_order')::bigint END "
                "FROM book_relationship_edges e JOIN series s ON s.id=CASE WHEN e.source_label='Book' "
                "THEN e.target_id ELSE e.source_id END WHERE e.book_id=%s AND e.relationship='PART_OF_SERIES' "
                "AND NOT EXISTS (SELECT 1 FROM book_series bs WHERE bs.book_id=e.book_id AND bs.series_id=s.id) "
                "ORDER BY 1 LIMIT 1",
                (book_id, book_id),
            )
            row = cursor.fetchone()
        return dict(zip(("id", "name", "volume_number", "volume_number_double", "series_order"), row)) if row else None

    def build_order_clause(self, order: str) -> str:
        if order == "publication":
            return "b.published_date ASC NULLS LAST,lower(b.title),b.id"
        if order in ("volume", "series_order"):
            return "COALESCE(bs.volume_number_double,bs.volume_number::double precision,1e12),lower(b.title),b.id"
        if order == "suggested":
            return "COALESCE(bs.volume_number_double,bs.volume_number::double precision,1e12),b.published_date ASC NULLS LAST,lower(b.title),b.id"
        return "lower(b.title),b.id"

    def get_books_for_series(self, series_id: str, order: str = "alpha") -> list[Book]:
        return self._get_books_for_series_uncached(series_id, order)

    def _get_books_for_series_uncached(self, series_id: str, order: str = "alpha") -> list[Book]:
        order_clause = self.build_order_clause(order)
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT b.id,bs.volume_number,bs.volume_number_double,bs.series_order "
                "FROM books b JOIN " + _SERIES_LINKS + " bs ON bs.book_id=b.id "
                "WHERE bs.series_id=%s ORDER BY " + order_clause,
                (series_id,),
            )
            rows = cursor.fetchall()
        books: list[Book] = []
        for book_id, volume, volume_double, series_order in rows:
            book = self.book_service.get_book_by_id_sync(str(book_id))
            if not book:
                continue
            book.series_volume = str(volume_double) if volume_double is not None else (str(volume) if volume is not None else None)
            book.series_order = int(series_order) if series_order is not None else None
            books.append(book)
        if order in ("publication", "volume", "series_order", "suggested") and books:
            if order == "publication" and all(not book.published_date for book in books):
                books.sort(key=lambda book: (book.title or "").casefold())
            if order in ("volume", "series_order", "suggested") and all(book.series_volume in (None, "") for book in books):
                books.sort(key=lambda book: (book.title or "").casefold())
        return books

    async def get_books_for_series_async(self, series_id: str, order: str = "alpha") -> list[Book]:
        return await asyncio.to_thread(self.get_books_for_series, series_id, order)

    def add_contributors(self, books: list[Book]):
        for book in books:
            try:
                contributions = getattr(book, "contributors", []) or []
                book.contributors = sorted(
                    [{"id": item.person_id, "name": item.person.name if item.person else "",
                      "role": getattr(item.contribution_type, "value", item.contribution_type)} for item in contributions],
                    key=lambda item: str(item.get("name") or "").casefold(),
                )
            except Exception:
                book.contributors = []
        return books

    async def add_contributors_async(self, books: list[Book]):
        return await asyncio.to_thread(self.add_contributors, books)

    def update_series_name(self, series_id: str, new_name: str) -> bool:
        name = str(new_name or "").strip()
        if not name:
            return False
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute("UPDATE series SET name=%s,normalized_name=%s WHERE id=%s", (name, name.casefold(), series_id))
            changed = cursor.rowcount == 1
        if changed:
            self._invalidate(series_id)
        return changed

    async def update_series_name_async(self, series_id: str, new_name: str) -> bool:
        return await asyncio.to_thread(self.update_series_name, series_id, new_name)

    def update_series_description(self, series_id: str, description: str) -> bool:
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute("UPDATE series SET description=%s WHERE id=%s", (description, series_id))
            changed = cursor.rowcount == 1
        if changed:
            self._invalidate(series_id)
        return changed

    async def update_series_description_async(self, series_id: str, description: str) -> bool:
        return await asyncio.to_thread(self.update_series_description, series_id, description)

    def update_series_cover(self, series_id: str, cover_url: str, custom: bool = True,
                            generated_placeholder: bool | None = None, user_uploaded: bool = True) -> bool:
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute(
                "UPDATE series SET user_cover=%s,custom_cover=%s,generated_placeholder=COALESCE(%s,generated_placeholder) WHERE id=%s",
                (cover_url, bool(custom and cover_url), generated_placeholder, series_id),
            )
            changed = cursor.rowcount == 1
        if changed:
            self._invalidate(series_id)
        return changed

    def delete_series(self, series_id: str) -> bool:
        try:
            with self.store.transaction() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT id FROM series WHERE id=%s FOR UPDATE", (series_id,))
                if not cursor.fetchone():
                    return False
                cursor.execute("DELETE FROM book_series WHERE series_id=%s", (series_id,))
                cursor.execute(
                    "DELETE FROM book_relationship_edges WHERE relationship='PART_OF_SERIES' AND "
                    "((source_label='Series' AND source_id=%s) OR (target_label='Series' AND target_id=%s))",
                    (series_id, series_id),
                )
                cursor.execute("DELETE FROM user_series_notes WHERE series_id=%s", (series_id,))
                cursor.execute("DELETE FROM series WHERE id=%s", (series_id,))
                deleted = cursor.rowcount == 1
            if deleted:
                self._invalidate(series_id)
            return deleted
        except Exception:
            return False

    async def update_series_cover_async(self, series_id: str, cover_url: str, custom: bool = True,
                                        generated_placeholder: bool | None = None, user_uploaded: bool = True) -> bool:
        return await asyncio.to_thread(self.update_series_cover, series_id, cover_url, custom, generated_placeholder, user_uploaded)

    def search_series(self, query: str, limit: int = 15) -> list[dict[str, Any]]:
        cleaned = str(query or "").strip().casefold()
        if not cleaned:
            return []
        limit = max(1, min(int(limit), 200))
        return self._cached_query(
            f"postgres:series:search:{cleaned}:{limit}", ("postgres:series", "postgres:books"),
            lambda: self._search_series_uncached(cleaned, limit),
        )

    def _search_series_uncached(self, query: str, limit: int) -> list[dict[str, Any]]:
        query = str(query or "").strip().casefold()
        if not query:
            return []
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT s.id,s.name,COALESCE(s.normalized_name,lower(s.name)),count(DISTINCT bs.book_id) "
                "FROM series s LEFT JOIN " + _SERIES_LINKS + " bs ON bs.series_id=s.id "
                "WHERE COALESCE(s.normalized_name,lower(s.name)) ILIKE %s "
                "GROUP BY s.id ORDER BY strpos(COALESCE(s.normalized_name,lower(s.name)),%s),"
                "COALESCE(s.normalized_name,lower(s.name)),s.id LIMIT %s",
                (f"%{query}%", query, limit),
            )
            return [dict(zip(("id", "name", "normalized_name", "book_count"), row)) for row in cursor.fetchall()]

    async def search_series_async(self, query: str, limit: int = 15) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self.search_series, query, limit)

    def create_series(self, name: str) -> Series | None:
        raw = str(name or "").strip()
        if not raw:
            return None
        normalized = raw.casefold()
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ("series:" + normalized,))
            cursor.execute("SELECT id,name,normalized_name,description,user_cover,cover_url,custom_cover,generated_placeholder,created_at,0,NULL FROM series WHERE normalized_name=%s OR lower(name)=%s ORDER BY id LIMIT 1", (normalized, normalized))
            row = cursor.fetchone()
            if row:
                return _series(row)
            series_id = str(uuid.uuid4())
            cursor.execute(
                "INSERT INTO series(id,name,normalized_name,created_at) VALUES (%s,%s,%s,now()) "
                "RETURNING id,name,normalized_name,description,user_cover,cover_url,custom_cover,generated_placeholder,created_at,0,NULL",
                (series_id, raw, normalized),
            )
            row = cursor.fetchone()
        self._invalidate(series_id)
        return _series(row) if row else None

    async def create_series_async(self, name: str) -> Series | None:
        return await asyncio.to_thread(self.create_series, name)

    async def ensure_book_series_async(self, book_id: str, series_name: str | None,
                                       volume: str | None = None, order_number: int | None = None) -> bool:
        name = str(series_name or "").strip()
        if not name or not book_id:
            return False
        try:
            order_number = int(order_number) if order_number is not None else None
        except (TypeError, ValueError):
            order_number = None
        series = await self.create_series_async(name)
        return bool(series and series.id and await self.attach_book_async(book_id, series.id, volume, order_number))

    def ensure_book_series(self, book_id: str, series_name: str | None,
                           volume: str | None = None, order_number: int | None = None) -> bool:
        series = self.create_series(str(series_name or ""))
        if not series or not series.id:
            return False
        try:
            order_number = int(order_number) if order_number is not None else None
        except (TypeError, ValueError):
            order_number = None
        return self.attach_book(book_id, series.id, volume, order_number)

    def attach_book(self, book_id: str, series_id: str, volume: str | None = None,
                    order_number: int | None = None, volume_number_double: float | None = None) -> bool:
        volume_text = str(volume).strip() if volume is not None else None
        parsed_int: int | None = None
        parsed_double: float | None = None
        if volume_text:
            try:
                parsed_double = float(volume_text)
                if parsed_double.is_integer():
                    parsed_int = int(parsed_double)
            except (TypeError, ValueError):
                pass
        if volume_number_double is not None:
            try:
                parsed_double = float(volume_number_double)
                if parsed_double.is_integer():
                    parsed_int = int(parsed_double)
            except (TypeError, ValueError):
                pass
        try:
            order_number = int(order_number) if order_number is not None else None
            with self.store.transaction() as connection, connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO book_series(book_id,series_id,volume_number,volume_number_double,series_order,created_at) "
                    "SELECT b.id,s.id,%s,%s,%s,now() FROM books b CROSS JOIN series s WHERE b.id=%s AND s.id=%s "
                    "ON CONFLICT(book_id,series_id) DO UPDATE SET "
                    "volume_number=COALESCE(EXCLUDED.volume_number,book_series.volume_number), "
                    "volume_number_double=COALESCE(EXCLUDED.volume_number_double,book_series.volume_number_double), "
                    "series_order=COALESCE(EXCLUDED.series_order,book_series.series_order)",
                    (parsed_int, parsed_double, order_number, book_id, series_id),
                )
                changed = cursor.rowcount == 1
            if changed:
                self._invalidate(series_id)
            return changed
        except Exception:
            return False

    async def attach_book_async(self, book_id: str, series_id: str, volume: str | None = None,
                                order_number: int | None = None, volume_number_double: float | None = None) -> bool:
        return await asyncio.to_thread(self.attach_book, book_id, series_id, volume, order_number, volume_number_double)

    def count_series_books(self, series_id: str) -> int:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM " + _SERIES_LINKS + " WHERE series_id=%s", (series_id,))
            return int(cursor.fetchone()[0])

    async def count_series_books_async(self, series_id: str) -> int:
        return await asyncio.to_thread(self.count_series_books, series_id)

    def get_user_series_notes(self, user_id: str, series_id: str) -> str | None:
        return self._cached_query(
            f"postgres:series:notes:{user_id}:{series_id}",
            (f"postgres:user_series:{user_id}", "postgres:series"),
            lambda: self._get_user_series_notes_uncached(user_id, series_id),
        )

    def _get_user_series_notes_uncached(self, user_id: str, series_id: str) -> str | None:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT notes FROM user_series_notes WHERE user_id=%s AND series_id=%s", (user_id, series_id))
            row = cursor.fetchone()
            if row:
                return row[0]
            cursor.execute("SELECT to_regclass('public.app_graph_edges')")
            if cursor.fetchone()[0] is None:
                return None
            cursor.execute(
                "SELECT properties->>'notes' FROM app_graph_edges WHERE relationship='HAS_SERIES_NOTES' "
                "AND source_label='User' AND source_id=%s AND target_label='Series' AND target_id=%s "
                "ORDER BY edge_id LIMIT 1",
                (user_id, series_id),
            )
            row = cursor.fetchone()
        return row[0] if row else None

    async def get_user_series_notes_async(self, user_id: str, series_id: str) -> str | None:
        return await asyncio.to_thread(self.get_user_series_notes, user_id, series_id)

    def upsert_user_series_notes(self, user_id: str, series_id: str, notes: str) -> bool:
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO user_series_notes(user_id,series_id,notes) "
                "SELECT u.id,s.id,%s FROM users u CROSS JOIN series s WHERE u.id=%s AND s.id=%s "
                "ON CONFLICT(user_id,series_id) DO UPDATE SET notes=EXCLUDED.notes,updated_at=now()",
                (notes, user_id, series_id),
            )
            changed = cursor.rowcount == 1
        if changed:
            self._invalidate(series_id, user_id)
        return changed

    async def upsert_user_series_notes_async(self, user_id: str, series_id: str, notes: str) -> bool:
        return await asyncio.to_thread(self.upsert_user_series_notes, user_id, series_id, notes)


_series_service: PostgresSeriesService | None = None


def get_postgres_series_service() -> PostgresSeriesService:
    global _series_service
    if _series_service is None:
        _series_service = PostgresSeriesService()
    return _series_service
