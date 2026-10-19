"""PostgreSQL implementation of global book persistence.

User-specific fields live in ``user_books`` and graph relationships remain in
their normalized tables plus the verified relationship archive. This service
does not choose a backend; the application facade will select it only after
the complete cutover is ready.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from ..domain.models import Book, BookContribution, Category, ContributionType, Person, Publisher, Series
from ..infrastructure import redis_cache
from ..infrastructure.postgres_store import PostgresStore, get_postgres_store


_BOOK_COLUMNS = (
    "id", "title", "normalized_title", "subtitle", "isbn13", "isbn10", "asin",
    "description", "published_date", "page_count", "language", "cover_url",
    "google_books_id", "openlibrary_id", "opds_source_id", "opds_source_updated_at",
    "opds_source_entry_hash", "average_rating", "rating_count", "series",
    "series_volume", "series_order", "custom_metadata", "raw_categories",
    "audio_length_minutes", "audio_is_abridged", "audio_narrators", "media_format",
    "audiobookshelf_id", "audio_duration_ms", "media_type", "audiobookshelf_updated_at",
    "quantity", "created_at", "updated_at", "extra",
)
_BOOK_JSON_COLUMNS = {"custom_metadata", "raw_categories", "audio_narrators", "extra"}
_BOOK_FIELDS = set(_BOOK_COLUMNS) - {"id", "created_at", "updated_at", "extra"}


def _book_data(book: Book) -> dict[str, Any]:
    series = getattr(book, "series", None)
    return {
        "id": str(book.id or uuid.uuid4()),
        "title": book.title or "",
        "normalized_title": book.normalized_title or Book._normalize_title(book.title or ""),
        "subtitle": book.subtitle,
        "isbn13": book.isbn13,
        "isbn10": book.isbn10,
        "asin": book.asin,
        "description": book.description,
        "published_date": book.published_date,
        "page_count": book.page_count,
        "language": book.language or "en",
        "cover_url": book.cover_url,
        "google_books_id": book.google_books_id,
        "openlibrary_id": book.openlibrary_id,
        "opds_source_id": book.opds_source_id,
        "opds_source_updated_at": book.opds_source_updated_at,
        "opds_source_entry_hash": book.opds_source_entry_hash,
        "average_rating": book.average_rating,
        "rating_count": book.rating_count,
        "series": getattr(series, "name", None) if series else None,
        "series_volume": book.series_volume,
        "series_order": book.series_order,
        "custom_metadata": book.custom_metadata or {},
        "raw_categories": book.raw_categories or [],
        "media_type": book.media_type,
        "quantity": getattr(book, "quantity", 1),
    }


def _book_from_row(row: Any) -> Book:
    data = dict(zip(_BOOK_COLUMNS, row))
    series_name = data.pop("series", None)
    data.pop("extra", None)
    for column in ("audio_length_minutes", "audio_is_abridged", "audio_narrators", "media_format", "audiobookshelf_id", "audio_duration_ms", "audiobookshelf_updated_at"):
        data.pop(column, None)
    data["created_at"] = data["created_at"] or datetime.now(timezone.utc)
    data["updated_at"] = data["updated_at"] or data["created_at"]
    data["custom_metadata"] = data.get("custom_metadata") or {}
    data["raw_categories"] = data.get("raw_categories") or []
    book = Book(**data)
    if series_name:
        book.series = Series(name=series_name)
    return book


class PostgresBookService:
    """CRUD and relationship persistence for global book records."""

    def __init__(self, store: PostgresStore | None = None):
        self.store = store or get_postgres_store()

    def _read_book(self, where: str, value: Any, *, order: str = "") -> Book | None:
        if where not in ("id", "isbn13", "isbn10", "opds_source_id", "audiobookshelf_id", "lower(title)"):
            raise ValueError("Unsupported book lookup")
        condition = f"lower({where[6:-1]}) = lower(%s)" if where.startswith("lower(") else f"{where} = %s"
        query = f"SELECT {', '.join(_BOOK_COLUMNS)} FROM books WHERE {condition} {order} LIMIT 1"
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, (value,))
            row = cursor.fetchone()
        if not row:
            return None
        book = _book_from_row(row)
        self._populate_relationships(book)
        return book

    def _populate_relationships(self, book: Book) -> None:
        if not book.id:
            return
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT p.id, p.name, c.contribution_type, c.order_index "
                "FROM book_contributors c JOIN people p ON p.id = c.person_id "
                "WHERE c.book_id = %s ORDER BY c.order_index NULLS LAST, c.id",
                (book.id,),
            )
            contributor_rows = cursor.fetchall()
            book.contributors = []
            for person_id, name, contribution_type, order_index in contributor_rows:
                try:
                    role = ContributionType(str(contribution_type or "authored").lower())
                except ValueError:
                    role = ContributionType.AUTHORED
                person = Person(id=str(person_id), name=str(name or ""))
                book.contributors.append(BookContribution(
                    person_id=str(person_id), book_id=book.id,
                    contribution_type=role, order=order_index, person=person,
                ))

            cursor.execute(
                "SELECT c.id, c.name, c.normalized_name, c.parent_id, c.description, c.level, c.color, "
                "c.icon, c.aliases, c.book_count, c.user_book_count, c.created_at, c.updated_at "
                "FROM book_categories bc JOIN categories c ON c.id = bc.category_id "
                "WHERE bc.book_id = %s ORDER BY lower(c.name), c.id", (book.id,),
            )
            category_rows = cursor.fetchall()
            if not category_rows:
                cursor.execute(
                    "SELECT c.id, c.name, c.normalized_name, c.parent_id, c.description, "
                    "c.level, c.color, c.icon, c.aliases, c.book_count, c.user_book_count, c.created_at, c.updated_at "
                    "FROM categories c JOIN (SELECT DISTINCT CASE WHEN source_label = 'Book' "
                    "THEN target_id ELSE source_id END AS category_id FROM book_relationship_edges "
                    "WHERE book_id = %s AND relationship = 'CATEGORIZED_AS') e ON c.id = e.category_id "
                    "ORDER BY lower(c.name), c.id",
                    (book.id,),
                )
                category_rows = cursor.fetchall()
            book.categories = [Category(
                id=row[0], name=row[1] or "", normalized_name=row[2] or "", parent_id=row[3],
                description=row[4], level=int(row[5] or 0), color=row[6], icon=row[7],
                aliases=row[8] or [], book_count=int(row[9] or 0), user_book_count=int(row[10] or 0),
                created_at=row[11] or datetime.now(timezone.utc), updated_at=row[12] or datetime.now(timezone.utc),
            ) for row in category_rows]

            cursor.execute(
                "SELECT p.id, p.name, p.normalized_name, p.founded_year, p.country, p.created_at "
                "FROM book_publishers bp JOIN publishers p ON p.id = bp.publisher_id "
                "WHERE bp.book_id = %s ORDER BY p.id LIMIT 1",
                (book.id,),
            )
            publisher = cursor.fetchone()
            if not publisher:
                cursor.execute(
                    "SELECT p.id, p.name, p.normalized_name, p.founded_year, p.country, p.created_at "
                    "FROM book_relationship_edges e JOIN publishers p ON p.id = "
                    "CASE WHEN e.source_label = 'Book' THEN e.target_id ELSE e.source_id END "
                    "WHERE e.book_id = %s AND e.relationship = 'PUBLISHED_BY' ORDER BY p.id LIMIT 1",
                    (book.id,),
                )
                publisher = cursor.fetchone()
            book.publisher = Publisher(
                id=publisher[0], name=publisher[1] or "", normalized_name=publisher[2] or "",
                founded_year=publisher[3], country=publisher[4], created_at=publisher[5] or datetime.now(timezone.utc),
            ) if publisher else None

            cursor.execute(
                "SELECT s.id, s.name, s.normalized_name, s.description, s.user_cover, s.cover_url, "
                "s.custom_cover, s.generated_placeholder, s.created_at FROM book_series bs "
                "JOIN series s ON s.id = bs.series_id WHERE bs.book_id = %s ORDER BY s.id LIMIT 1",
                (book.id,),
            )
            series = cursor.fetchone()
            if not series:
                cursor.execute(
                    "SELECT s.id, s.name, s.normalized_name, s.description, s.user_cover, s.cover_url, "
                    "s.custom_cover, s.generated_placeholder, s.created_at FROM book_relationship_edges e "
                    "JOIN series s ON s.id = CASE WHEN e.source_label = 'Book' THEN e.target_id ELSE e.source_id END "
                    "WHERE e.book_id = %s AND e.relationship = 'PART_OF_SERIES' ORDER BY s.id LIMIT 1",
                    (book.id,),
                )
                series = cursor.fetchone()
            if series:
                book.series = Series(
                    id=series[0], name=series[1] or "", normalized_name=series[2] or "",
                    description=series[3], user_cover=series[4], cover_url=series[5],
                    custom_cover=bool(series[6]), generated_placeholder=bool(series[7]),
                    created_at=series[8] or datetime.now(timezone.utc),
                )

    def get_book_by_id_sync(self, book_id: str) -> Book | None:
        return self._read_book("id", book_id)

    async def get_book_by_id(self, book_id: str) -> Book | None:
        return await asyncio.to_thread(self.get_book_by_id_sync, book_id)

    def get_book_by_isbn_sync(self, isbn: str) -> Book | None:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {', '.join(_BOOK_COLUMNS)} FROM books "
                "WHERE isbn13 = %s OR isbn10 = %s ORDER BY created_at NULLS LAST, id LIMIT 1",
                (isbn, isbn),
            )
            row = cursor.fetchone()
        if not row:
            return None
        book = _book_from_row(row)
        self._populate_relationships(book)
        return book

    def get_book_by_opds_source_id_sync(self, source_id: str) -> Book | None:
        return self._read_book("opds_source_id", str(source_id))

    def get_book_field_sync(self, book_id: str, field: str) -> Any:
        if field not in {"audio_duration_ms", "page_count", "audiobookshelf_id", "audiobookshelf_updated_at"}:
            raise ValueError("Unsupported book field")
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(f"SELECT {field} FROM books WHERE id=%s", (str(book_id),))
            row = cursor.fetchone()
        return row[0] if row else None

    async def get_book_by_isbn(self, isbn: str) -> Book | None:
        return await asyncio.to_thread(self.get_book_by_isbn_sync, isbn)

    def create_book_sync(
        self,
        domain_book: Book,
        user_id: str | None = None,
        *,
        reading_status: str = "",
        ownership_status: str = "owned",
    ) -> Book:
        now = datetime.now(timezone.utc)
        domain_book.id = str(domain_book.id or uuid.uuid4())
        domain_book.created_at = domain_book.created_at or now
        domain_book.updated_at = now
        values = _book_data(domain_book)
        values["created_at"] = domain_book.created_at
        values["updated_at"] = domain_book.updated_at
        values["extra"] = {}
        columns = tuple(_BOOK_COLUMNS)
        placeholders = ["%s::jsonb" if name in _BOOK_JSON_COLUMNS else "%s" for name in columns]
        payload = [
            json.dumps(values.get(name), ensure_ascii=False, separators=(",", ":"))
            if name in _BOOK_JSON_COLUMNS else values.get(name)
            for name in columns
        ]
        existing_book = None
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO books ({', '.join(columns)}) VALUES ({', '.join(placeholders)}) "
                "ON CONFLICT (id) DO NOTHING",
                payload,
            )
            if cursor.rowcount == 0:
                cursor.execute(
                    f"SELECT {', '.join(_BOOK_COLUMNS)} FROM books WHERE id = %s",
                    (domain_book.id,),
                )
                existing_row = cursor.fetchone()
                if existing_row is None:
                    raise RuntimeError("Book insert conflicted but the existing book could not be read")
                existing_book = _book_from_row(existing_row)
            else:
                self._write_contributors(connection, domain_book.id, domain_book.contributors)
                self._write_categories(connection, domain_book.id, domain_book.categories)
                self._write_series(connection, domain_book)
                self._write_publisher(connection, domain_book.id, domain_book.publisher)
            if user_id:
                cursor.execute(
                    "INSERT INTO user_books(user_id,book_id,reading_status,ownership_status,custom_metadata,created_at,updated_at) "
                    "VALUES (%s,%s,%s,%s,'{}'::jsonb,now(),now()) ON CONFLICT(user_id,book_id) DO NOTHING",
                    (str(user_id), domain_book.id, reading_status, ownership_status),
                )
        tags = ["postgres:books", "postgres:people"]
        if user_id:
            tags.append(f"postgres:user_books:{user_id}")
        redis_cache.invalidate_tags(*tags)
        return existing_book or domain_book

    async def create_book(self, domain_book: Book) -> Book:
        return await asyncio.to_thread(self.create_book_sync, domain_book)

    def find_or_create_book_sync(self, domain_book: Book) -> Book:
        if domain_book.isbn13:
            found = self.get_book_by_isbn_sync(domain_book.isbn13)
            if found:
                return found
        if domain_book.isbn10:
            found = self.get_book_by_isbn_sync(domain_book.isbn10)
            if found:
                return found
        found = self._read_book("lower(title)", domain_book.title or "")
        return found if found else self.create_book_sync(domain_book)

    def update_book_sync(self, book_id: str, updates: dict[str, Any]) -> Book | None:
        book_fields = {key: value for key, value in updates.items() if key in _BOOK_FIELDS}
        if not book_fields:
            return self.get_book_by_id_sync(book_id)
        with self.store.transaction() as connection, connection.cursor() as cursor:
            if "cover_url" in book_fields:
                candidate = book_fields["cover_url"]
                cursor.execute("SELECT cover_url FROM books WHERE id = %s FOR UPDATE", (book_id,))
                row = cursor.fetchone()
                if not row:
                    return None
                if (candidate is None or (isinstance(candidate, str) and not candidate.strip())) and row[0]:
                    book_fields.pop("cover_url")
            if "series" in book_fields:
                series = book_fields["series"]
                book_fields["series"] = getattr(series, "name", series) if series else None
            if "title" in book_fields and "normalized_title" not in book_fields:
                book_fields["normalized_title"] = Book._normalize_title(str(book_fields["title"] or ""))
            if not book_fields:
                return self.get_book_by_id_sync(book_id)
            assignments = [f"{name} = %s::jsonb" if name in _BOOK_JSON_COLUMNS else f"{name} = %s" for name in book_fields]
            params = [
                json.dumps(value, ensure_ascii=False, separators=(",", ":")) if name in _BOOK_JSON_COLUMNS else value
                for name, value in book_fields.items()
            ]
            cursor.execute(
                "UPDATE books SET " + ", ".join(assignments) + ", updated_at = now() WHERE id = %s",
                (*params, book_id),
            )
            if cursor.rowcount == 0:
                return None
            if "series" in book_fields or "series_volume" in book_fields or "series_order" in book_fields:
                cursor.execute("SELECT series, series_volume, series_order FROM books WHERE id = %s", (book_id,))
                series_name, volume, series_order = cursor.fetchone()
                cursor.execute("DELETE FROM book_series WHERE book_id = %s", (book_id,))
                if series_name:
                    series = Series(name=series_name)
                    self._write_series(connection, Book(id=book_id, title="", series=series,
                                                       series_volume=volume, series_order=series_order))
        redis_cache.invalidate_tags("postgres:books")
        return self.get_book_by_id_sync(book_id)

    def backfill_missing_media_type_sync(self, media_type: str) -> int:
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute(
                "UPDATE books SET media_type=%s,updated_at=now() "
                "WHERE media_type IS NULL OR btrim(media_type)=''",
                (media_type,),
            )
            updated = cursor.rowcount
        if updated:
            redis_cache.invalidate_tags("postgres:books")
        return int(updated)

    def replace_contributors_sync(self, book_id: str, contributions: list[Any] | None) -> bool:
        try:
            with self.store.transaction() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT 1 FROM books WHERE id = %s FOR UPDATE", (book_id,))
                if not cursor.fetchone():
                    return False
                self._write_contributors(connection, book_id, contributions, replace=True)
            redis_cache.invalidate_tags("postgres:books", "postgres:people")
            return True
        except Exception:
            return False

    def replace_categories_sync(self, book_id: str, categories: list[Any] | None) -> bool:
        try:
            with self.store.transaction() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT 1 FROM books WHERE id = %s FOR UPDATE", (book_id,))
                if not cursor.fetchone():
                    return False
                self._write_categories(connection, book_id, categories, replace=True)
            redis_cache.invalidate_tags("postgres:books", "postgres:categories")
            return True
        except Exception:
            return False

    async def update_book(self, book_id: str, updates: dict[str, Any]) -> Book | None:
        return await asyncio.to_thread(self.update_book_sync, book_id, updates)

    def _write_contributors(self, connection: Any, book_id: str, contributions: list[Any] | None, *, replace: bool = False) -> None:
        if replace:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM book_contributors WHERE book_id = %s", (book_id,))
        if not contributions:
            return
        with connection.cursor() as cursor:
            for index, contribution in enumerate(contributions):
                person = getattr(contribution, "person", None)
                name = getattr(person, "name", "") or ""
                person_id = str(getattr(person, "id", None) or getattr(contribution, "person_id", "") or "")
                if not person_id:
                    person_id = str(uuid.uuid4())
                cursor.execute("SELECT id FROM people WHERE id = %s", (person_id,))
                existing = cursor.fetchone()
                if not existing and name:
                    cursor.execute("SELECT id FROM people WHERE lower(name) = lower(%s) ORDER BY id LIMIT 1", (name,))
                    existing = cursor.fetchone()
                if existing:
                    person_id = str(existing[0])
                else:
                    cursor.execute(
                        "INSERT INTO people(id, name, normalized_name, extra) VALUES (%s, %s, %s, '{}'::jsonb)",
                        (person_id, name, Person._normalize_name(name) if name else ""),
                    )
                contribution_type = getattr(contribution, "contribution_type", ContributionType.AUTHORED)
                role = getattr(contribution_type, "value", contribution_type)
                order_index = getattr(contribution, "order", None)
                cursor.execute(
                    "INSERT INTO book_contributors(source_relationship, book_id, person_id, contribution_type, role, order_index, created_at, extra) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, '{}'::jsonb)",
                    (str(role).upper(), book_id, person_id, str(role), str(role), order_index if order_index is not None else index, datetime.now(timezone.utc)),
                )

    def _write_categories(self, connection: Any, book_id: str, categories: list[Any] | None, *, replace: bool = False) -> None:
        if replace:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM book_categories WHERE book_id = %s", (book_id,))
        if not categories:
            return
        with connection.cursor() as cursor:
            for category in categories:
                name = str(getattr(category, "name", category) or "").strip()
                if not name:
                    continue
                category_id = str(getattr(category, "id", None) or uuid.uuid4())
                cursor.execute(
                    "SELECT id FROM categories WHERE id = %s OR lower(name) = lower(%s) ORDER BY (id = %s) DESC LIMIT 1",
                    (category_id, name, category_id),
                )
                row = cursor.fetchone()
                if row:
                    category_id = str(row[0])
                else:
                    cursor.execute(
                        "INSERT INTO categories(id, name, normalized_name, extra) VALUES (%s, %s, %s, '{}'::jsonb)",
                        (category_id, name, Category._normalize_name(name)),
                    )
                cursor.execute(
                    "INSERT INTO book_categories(book_id, category_id, created_at) VALUES (%s, %s, now()) ON CONFLICT DO NOTHING",
                    (book_id, category_id),
                )

    def _write_series(self, connection: Any, book: Book) -> None:
        series = getattr(book, "series", None)
        name = getattr(series, "name", None) if series else None
        if not name or not book.id:
            return
        with connection.cursor() as cursor:
            cursor.execute("SELECT id FROM series WHERE lower(name) = lower(%s) ORDER BY id LIMIT 1", (name,))
            row = cursor.fetchone()
            if row:
                series_id = str(row[0])
            else:
                series_id = str(getattr(series, "id", None) or uuid.uuid4())
                cursor.execute(
                    "INSERT INTO series(id, name, normalized_name, extra) VALUES (%s, %s, %s, '{}'::jsonb)",
                    (series_id, name, name.strip().lower()),
                )
            cursor.execute(
                "INSERT INTO book_series(book_id, series_id, volume_number, series_order, created_at) "
                "VALUES (%s, %s, %s, %s, now()) ON CONFLICT (book_id, series_id) DO UPDATE SET "
                "volume_number = EXCLUDED.volume_number, series_order = EXCLUDED.series_order",
                (book.id, series_id, self._numeric_volume(book.series_volume), book.series_order),
            )

    def _write_publisher(self, connection: Any, book_id: str, publisher: Any) -> None:
        name = publisher if isinstance(publisher, str) else (getattr(publisher, "name", None) if publisher else None)
        if not name or not book_id:
            return
        with connection.cursor() as cursor:
            cursor.execute("SELECT id FROM publishers WHERE lower(name) = lower(%s) ORDER BY id LIMIT 1", (name,))
            row = cursor.fetchone()
            if row:
                publisher_id = str(row[0])
            else:
                publisher_id = str(getattr(publisher, "id", None) or uuid.uuid4())
                cursor.execute(
                    "INSERT INTO publishers(id, name, normalized_name, founded_year, country, extra) "
                    "VALUES (%s, %s, %s, %s, %s, '{}'::jsonb)",
                    (publisher_id, name, name.strip().lower(), getattr(publisher, "founded_year", None), getattr(publisher, "country", None)),
                )
            cursor.execute(
                "INSERT INTO book_publishers(book_id, publisher_id, created_at) VALUES (%s, %s, now()) ON CONFLICT DO NOTHING",
                (book_id, publisher_id),
            )

    def replace_publisher_sync(self, book_id: str, publisher: Any) -> bool:
        try:
            with self.store.transaction() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT 1 FROM books WHERE id = %s FOR UPDATE", (book_id,))
                if not cursor.fetchone():
                    return False
                cursor.execute("DELETE FROM book_publishers WHERE book_id = %s", (book_id,))
                self._write_publisher(connection, book_id, publisher)
            redis_cache.invalidate_tags("postgres:books")
            return True
        except Exception:
            return False

    @staticmethod
    def _numeric_volume(value: Any) -> int | None:
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def delete_book_sync(self, book_id: str) -> bool:
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT user_id FROM user_books WHERE book_id = %s", (book_id,))
            user_tags = [f"postgres:user_books:{row[0]}" for row in cursor.fetchall()]
            cursor.execute("DELETE FROM book_relationship_edges WHERE book_id = %s", (book_id,))
            cursor.execute("DELETE FROM book_contributors WHERE book_id = %s", (book_id,))
            cursor.execute("DELETE FROM book_authors WHERE book_id = %s", (book_id,))
            cursor.execute("DELETE FROM book_publishers WHERE book_id = %s", (book_id,))
            cursor.execute("DELETE FROM book_categories WHERE book_id = %s", (book_id,))
            cursor.execute("DELETE FROM book_series WHERE book_id = %s", (book_id,))
            cursor.execute("DELETE FROM book_locations WHERE book_id = %s", (book_id,))
            cursor.execute("DELETE FROM book_location_overrides WHERE book_id = %s", (book_id,))
            cursor.execute("DELETE FROM global_metadata_records WHERE book_id = %s", (book_id,))
            cursor.execute("DELETE FROM custom_fields WHERE book_id = %s", (book_id,))
            cursor.execute("DELETE FROM user_books WHERE book_id = %s", (book_id,))
            cursor.execute("UPDATE reading_logs SET book_id = NULL WHERE book_id = %s", (book_id,))
            cursor.execute("UPDATE reviews SET book_id = NULL WHERE book_id = %s", (book_id,))
            cursor.execute("DELETE FROM books WHERE id = %s", (book_id,))
            deleted = cursor.rowcount > 0
        if deleted:
            redis_cache.invalidate_tags("postgres:books", *user_tags)
        return deleted

    async def delete_book(self, book_id: str) -> bool:
        return await asyncio.to_thread(self.delete_book_sync, book_id)
