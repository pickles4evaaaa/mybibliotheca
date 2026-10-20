"""Application-facing PostgreSQL facade for book, library, and catalog data.

The persistence selector chooses this facade only after a verified Kuzu
snapshot has been activated. It composes all PostgreSQL services so requests
cannot accidentally split related records across database backends.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from ..domain.models import Book, Category, Series
from ..infrastructure.postgres_store import PostgresStore, get_postgres_store
from .postgres_book_service import PostgresBookService
from .postgres_catalog_service import PostgresCatalogService
from .postgres_custom_field_service import PostgresCustomFieldService
from .postgres_library_service import PostgresLibraryService
from .postgres_import_mapping_service import PostgresImportMappingService
from .postgres_location_service import PostgresLocationService
from .postgres_reading_log_service import PostgresReadingLogService
from .postgres_series_service import PostgresSeriesService
from .postgres_user_service import PostgresUserService


class PostgresServiceFacade:
    """Composition root matching the user-facing Kuzu book facade contract."""

    def __init__(self, store: PostgresStore | None = None):
        self.store = store or get_postgres_store()
        self.book_service = PostgresBookService(self.store)
        self.library_service = PostgresLibraryService(self.store)
        self.catalog_service = PostgresCatalogService(self.store)
        self.user_service = PostgresUserService(self.store)
        self.reading_log_service = PostgresReadingLogService(self.store)
        self.series_service = PostgresSeriesService(store=self.store)
        self.location_service = PostgresLocationService(self.store)
        self.import_mapping_service = PostgresImportMappingService(self.store)
        self.custom_field_service = PostgresCustomFieldService(self.store)

    def _overlay_to_book(self, row: dict[str, Any]) -> Book:
        series_name = row.get("series")
        custom_metadata = dict(row.get("book_custom_metadata") or {})
        custom_metadata.update(row.get("custom_metadata") or {})
        book = Book(
            id=str(row.get("book_id") or row.get("id") or ""),
            title=row.get("title") or "", subtitle=row.get("subtitle"),
            isbn13=row.get("isbn13"), isbn10=row.get("isbn10"),
            description=row.get("description"), published_date=row.get("published_date"),
            page_count=row.get("page_count"), language=row.get("language") or "en",
            cover_url=row.get("cover_url"), custom_metadata=custom_metadata,
            raw_categories=row.get("raw_categories") or [], media_type=row.get("book_media_type"),
            series=Series(name=series_name) if series_name else None,
            series_volume=row.get("series_volume"), series_order=row.get("series_order"),
            created_at=row.get("date_added") or row.get("created_at") or datetime.now(timezone.utc),
            updated_at=row.get("updated_at") or datetime.now(timezone.utc),
        )
        self.book_service._populate_relationships(book)
        personal = {
            "reading_status": row.get("reading_status"),
            "ownership_status": row.get("ownership_status") or "owned",
            "date_added": row.get("date_added") or book.created_at,
            "start_date": row.get("start_date"), "finish_date": row.get("finish_date"),
            "current_page": row.get("current_page"), "total_pages": row.get("total_pages"),
            "user_rating": row.get("user_rating"), "review": row.get("user_review"),
            "personal_notes": row.get("personal_notes"), "location_id": row.get("location_id"),
            "progress_ms": (row.get("custom_metadata") or {}).get("progress_ms"),
            "progress_percentage": (row.get("custom_metadata") or {}).get("progress_percentage"),
        }
        for name, value in personal.items():
            setattr(book, name, value)
        setattr(book, "want_to_read", personal["reading_status"] == "plan_to_read")
        setattr(book, "library_only", personal["reading_status"] == "library_only")
        return book

    def _books_from_overlay(self, user_id: str, *, limit: int = 1000, offset: int = 0) -> list[Book]:
        rows = self.library_service.get_books_with_user_overlay_paginated_sync(user_id, limit, offset)
        return [self._overlay_to_book(row) for row in rows]

    def create_book_sync(self, domain_book: Book, user_id: str) -> Book:
        return self.book_service.create_book_sync(domain_book, user_id, reading_status="")

    async def create_book(self, domain_book: Book, user_id: str) -> Book:
        return await asyncio.to_thread(self.create_book_sync, domain_book, user_id)

    def get_book_by_id_sync(self, book_id: str) -> Book | None:
        return self.book_service.get_book_by_id_sync(book_id)

    def get_book_sync(self, book_id: str) -> Book | None:
        """Compatibility alias used by CSV and bulk import routes."""
        return self.get_book_by_id_sync(book_id)

    async def get_book_by_id(self, book_id: str) -> Book | None:
        return await asyncio.to_thread(self.get_book_by_id_sync, book_id)

    def get_book_by_uid_sync(self, uid: str, user_id: str) -> Book | None:
        return self.get_book_by_id_for_user_sync(uid, user_id)

    def get_book_by_id_for_user_sync(self, book_id: str, user_id: str) -> Book | None:
        row = self.library_service.get_user_book_sync(str(user_id), str(book_id))
        return self._overlay_to_book(row) if row else None

    async def get_book_by_id_for_user(self, book_id: str, user_id: str) -> Book | None:
        return await asyncio.to_thread(self.get_book_by_id_for_user_sync, book_id, user_id)

    def get_user_book_sync(self, user_id: str, book_id: str) -> dict[str, Any] | None:
        return self.library_service.get_user_book_sync(user_id, book_id)

    def add_book_to_user_library_sync(
        self, user_id: str, book_id: str, reading_status: str = "library_only",
        ownership_status: str = "owned", locations: list[str] | None = None,
        custom_metadata: dict[str, Any] | None = None,
    ) -> bool:
        return self.library_service.add_book_to_user_library_sync(
            user_id, book_id, reading_status, ownership_status, locations, custom_metadata,
        )

    def update_book_sync(self, book_id: str, user_id: str, **updates: Any) -> Book | None:
        book_fields = {
            name: value for name, value in updates.items()
            if name in {
                "title", "normalized_title", "subtitle", "isbn13", "isbn10", "asin", "description",
                "published_date", "page_count", "language", "cover_url", "google_books_id",
                "openlibrary_id", "opds_source_id", "opds_source_updated_at", "opds_source_entry_hash",
                "average_rating", "rating_count", "series", "series_volume", "series_order",
                "raw_categories", "media_type", "quantity", "audio_length_minutes",
                "audio_is_abridged", "audio_narrators", "media_format", "audiobookshelf_id",
                "audio_duration_ms", "audiobookshelf_updated_at",
            }
        }
        personal = {
            "reading_status": updates.get("reading_status"),
            "ownership_status": updates.get("ownership_status"),
            "user_rating": updates.get("user_rating"),
            "personal_notes": updates.get("personal_notes"),
            "user_review": updates.get("review", updates.get("user_review")),
            "start_date": updates.get("start_date"),
            "finish_date": updates.get("finish_date"),
            "location_id": updates.get("location_id", updates.get("primary_location_id")),
        }
        personal = {name: value for name, value in personal.items() if name in updates or (name == "user_review" and "review" in updates)}
        if "custom_metadata" in updates:
            personal["custom_metadata"] = updates["custom_metadata"]
        updated_book = self.book_service.update_book_sync(book_id, book_fields) if book_fields else None
        if personal:
            self.library_service.update_user_book_sync(user_id, book_id, personal)
        if "contributors" in updates:
            self.book_service.replace_contributors_sync(book_id, updates["contributors"])
        if "categories" in updates or "raw_categories" in updates:
            categories = updates.get("categories", updates.get("raw_categories"))
            self.book_service.replace_categories_sync(book_id, categories)
        if "publisher" in updates:
            self.book_service.replace_publisher_sync(book_id, updates["publisher"])
        if updated_book:
            return updated_book
        if personal or "contributors" in updates or "categories" in updates or "raw_categories" in updates or "publisher" in updates:
            return self.get_book_by_id_for_user_sync(book_id, user_id)
        return self.get_book_by_id_sync(book_id)

    async def update_book(self, book_id: str, updates: dict[str, Any], user_id: str) -> Book | None:
        return await asyncio.to_thread(self.update_book_sync, book_id, user_id, **updates)

    def delete_book_sync(self, book_id: str, user_id: str) -> bool:
        return self.book_service.delete_book_sync(book_id)

    async def delete_book(self, book_id: str, user_id: str) -> bool:
        return await asyncio.to_thread(self.delete_book_sync, book_id, user_id)

    def find_or_create_book_sync(self, domain_book: Book) -> Book:
        return self.book_service.find_or_create_book_sync(domain_book)

    def get_book_by_isbn_sync(self, isbn: str) -> Book | None:
        return self.book_service.get_book_by_isbn_sync(isbn)

    def get_book_by_title_author_sync(self, title: str, author: str) -> Book | None:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT b.id FROM books b WHERE lower(b.title) = lower(%s) "
                "AND (%s = '' OR EXISTS (SELECT 1 FROM book_contributors c "
                "JOIN people p ON p.id=c.person_id WHERE c.book_id=b.id AND lower(p.name)=lower(%s))) "
                "ORDER BY b.created_at NULLS LAST,b.id LIMIT 1",
                (str(title or "").strip(), str(author or "").strip(), str(author or "").strip()),
            )
            row = cursor.fetchone()
        return self.book_service.get_book_by_id_sync(str(row[0])) if row else None

    def get_book_by_audiobookshelf_id_sync(self, external_id: str) -> Book | None:
        return self.book_service._read_book("audiobookshelf_id", str(external_id))

    def get_book_by_isbn_for_user_sync(self, isbn: str, user_id: str) -> Book | None:
        book = self.book_service.get_book_by_isbn_sync(isbn)
        return self.get_book_by_id_for_user_sync(book.id, user_id) if book and book.id else None

    async def get_book_by_isbn_for_user(self, isbn: str, user_id: str) -> Book | None:
        return await asyncio.to_thread(self.get_book_by_isbn_for_user_sync, isbn, user_id)

    def get_books_for_user(self, user_id: str, limit: int = 50, offset: int = 0) -> list[Book]:
        return self._books_from_overlay(user_id, limit=limit, offset=offset)

    def get_user_books_sync(self, user_id: str, limit: int = 50, offset: int = 0) -> list[Book]:
        return self.get_books_for_user(user_id, limit, offset)

    def get_all_books_with_user_overlay_sync(self, user_id: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        offset = 0
        while True:
            page = self.library_service.get_books_with_user_overlay_paginated_sync(user_id, 1000, offset)
            rows.extend(page)
            if len(page) < 1000:
                return rows
            offset += len(page)

    def get_all_books_with_user_overlay(self, user_id: str) -> list[dict[str, Any]]:
        return self.get_all_books_with_user_overlay_sync(user_id)

    async def get_all_books_with_user_overlay_async(self, user_id: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self.get_all_books_with_user_overlay_sync, user_id)

    def get_all_books_with_user_overlay_flat_sync(self, user_id: str) -> list[dict[str, Any]]:
        return self.get_all_books_with_user_overlay_sync(user_id)

    def get_books_with_user_overlay_paginated_sync(self, user_id: str, limit: int, offset: int, sort: str = "title_asc") -> list[dict[str, Any]]:
        return self.library_service.get_books_with_user_overlay_paginated_sync(user_id, limit, offset, sort)

    def get_total_book_count_sync(self) -> int:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM books")
            return int(cursor.fetchone()[0])

    def get_total_person_count_sync(self) -> int:
        return self.catalog_service.get_total_person_count_sync()

    def get_total_reading_log_count_sync(self) -> int:
        return self.reading_log_service.get_total_reading_log_count_sync()

    def backfill_missing_media_type_sync(self, media_type: str) -> int:
        return self.book_service.backfill_missing_media_type_sync(media_type)

    def get_repair_overview_sync(self) -> dict[str, Any]:
        spec = {
            "missing_media_type": "(b.media_type IS NULL OR btrim(b.media_type)='')",
            "missing_authors": "NOT EXISTS (SELECT 1 FROM book_contributors c WHERE c.book_id=b.id AND lower(COALESCE(c.source_relationship,c.contribution_type,c.role,'')) IN ('authored','written_by')) AND NOT EXISTS (SELECT 1 FROM book_relationship_edges e WHERE e.book_id=b.id AND e.relationship IN ('AUTHORED','WRITTEN_BY'))",
            "missing_locations": "NOT EXISTS (SELECT 1 FROM book_locations bl WHERE bl.book_id=b.id) AND NOT EXISTS (SELECT 1 FROM book_relationship_edges e WHERE e.book_id=b.id AND e.relationship='STORED_AT' AND NOT EXISTS (SELECT 1 FROM book_location_overrides o WHERE o.book_id=b.id))",
            "missing_isbn": "(b.isbn13 IS NULL OR btrim(b.isbn13)='') AND (b.isbn10 IS NULL OR btrim(b.isbn10)='')",
            "missing_covers": "(b.cover_url IS NULL OR btrim(b.cover_url)='')",
        }
        overview: dict[str, Any] = {"total_books": self.get_total_book_count_sync(), "issue_stats": {}, "issue_samples": {}}
        with self.store.connection() as connection, connection.cursor() as cursor:
            for key, predicate in spec.items():
                cursor.execute(f"SELECT count(*) FROM books b WHERE {predicate}")
                overview["issue_stats"][key] = int(cursor.fetchone()[0])
                cursor.execute(
                    f"SELECT b.id,b.title,b.updated_at FROM books b WHERE {predicate} "
                    "ORDER BY b.updated_at DESC NULLS LAST,b.id LIMIT 6"
                )
                overview["issue_samples"][key] = [
                    {"id": row[0], "title": row[1], "updated_at": row[2]} for row in cursor.fetchall()
                ]
        return overview

    def get_repair_candidates_sync(self, kind: str) -> list[dict[str, Any]]:
        if kind == "isbn":
            missing = "(b.isbn13 IS NULL OR btrim(b.isbn13)='') AND (b.isbn10 IS NULL OR btrim(b.isbn10)='')"
        elif kind == "cover":
            missing = "b.cover_url IS NULL OR btrim(b.cover_url)=''"
        else:
            raise ValueError(f"Unknown repair kind: {kind}")
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT b.id,b.title,b.isbn13,b.isbn10,b.cover_url,b.published_date, "
                "COALESCE(array_agg(DISTINCT p.name) FILTER (WHERE p.name IS NOT NULL "
                "AND lower(COALESCE(c.contribution_type,c.role,c.source_relationship,'')) "
                "IN ('authored','co_authored','written_by')), ARRAY[]::text[]) "
                "FROM books b LEFT JOIN book_contributors c ON c.book_id=b.id "
                "LEFT JOIN people p ON p.id=c.person_id "
                f"WHERE {missing} GROUP BY b.id ORDER BY b.title,b.id"
            )
            rows = cursor.fetchall()
        return [
            {
                "id": row[0], "title": row[1], "isbn13": row[2], "isbn10": row[3],
                "cover_url": row[4], "published_date": row[5], "authors": row[6] or [],
            }
            for row in rows
        ]

    def get_book_user_ids_sync(self, book_id: str) -> list[str]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT user_id FROM user_books WHERE book_id=%s ORDER BY user_id", (str(book_id),))
            return [str(row[0]) for row in cursor.fetchall()]

    def assign_all_books_without_location_sync(self, location_id: str) -> int:
        return self.location_service.assign_all_books_without_location(location_id)

    def get_library_status_counts_sync(self, user_id: str) -> dict[str, int]:
        return self.library_service.get_library_status_counts_sync(user_id)

    @property
    def user_repo(self):
        """Compatibility access to the PostgreSQL account service."""
        return self.user_service

    def search_user_books_sync(self, user_id: str, query: str, limit: int = 10) -> list[dict[str, Any]]:
        return self.library_service.search_user_books_sync(user_id, query, limit)

    def search_books_sync(self, query: str, user_id: str, limit: int = 50) -> list[Book]:
        rows = self.library_service.search_user_books_sync(user_id, query, limit)
        return [book for row in rows if (book := self.get_book_by_id_for_user_sync(row["book_id"], user_id))]

    def search_categories_sync(self, query: str, limit: int = 10, user_id: str | None = None) -> list[dict[str, Any]]:
        return self.catalog_service.search_categories_sync(query, limit, user_id)

    def list_all_categories_sync(self, user_id: str | None = None) -> list[dict[str, Any]]:
        return self.catalog_service.list_all_categories_sync(user_id)

    def get_all_categories_sync(self) -> list[dict[str, Any]]:
        return self.list_all_categories_sync()

    def get_category_by_id_sync(self, category_id: str, user_id: str | None = None) -> Category | None:
        return self.catalog_service.get_category_by_id_sync(category_id, user_id)

    def get_child_categories_sync(self, parent_id: str) -> list[dict[str, Any]]:
        return self.catalog_service.get_child_categories_sync(parent_id)

    def get_category_children_sync(self, category_id: str, user_id: str | None = None) -> list[dict[str, Any]]:
        return self.catalog_service.get_category_children_sync(category_id, user_id)

    def get_root_categories_sync(self, user_id: str | None = None) -> list[dict[str, Any]]:
        return self.catalog_service.get_root_categories_sync(user_id)

    def get_books_by_category_sync(self, category_id: str, user_id: str | None = None, include_subcategories: bool = False) -> list[dict[str, Any]]:
        return self.catalog_service.get_books_by_category_sync(category_id, user_id, include_subcategories)

    def create_category_sync(self, category_data: dict[str, Any]) -> Category | None:
        return self.catalog_service.create_category_sync(category_data)

    def update_category_sync(self, category: Category) -> Category | None:
        return self.catalog_service.update_category_sync(category)

    def delete_category_sync(self, category_id: str) -> bool:
        return self.catalog_service.delete_category_sync(category_id)

    def get_category_book_counts_sync(self) -> dict[str, int]:
        return self.catalog_service.get_category_book_counts_sync()

    def get_book_categories_sync(self, book_id: str) -> list[dict[str, Any]]:
        return self.catalog_service.get_book_categories_sync(book_id)

    def merge_categories_sync(self, primary_category_id: str, merge_category_ids: list[str]) -> bool:
        return self.catalog_service.merge_categories_sync(primary_category_id, merge_category_ids)

    def list_all_persons_sync(self, user_id: str | None = None) -> list[dict[str, Any]]:
        return self.catalog_service.list_all_persons_sync(user_id)

    def get_person_by_id_sync(self, person_id: str) -> dict[str, Any] | None:
        return self.catalog_service.get_person_by_id_sync(person_id)

    def get_person_by_name_sync(self, name: str) -> dict[str, Any] | None:
        return self.catalog_service.get_person_by_name_sync(name)

    def get_books_by_person_sync(
        self, person_id: str, user_id: str | None = None,
    ) -> dict[str, list[dict[str, Any]]] | list[dict[str, Any]]:
        return self.catalog_service.get_books_by_person_sync(person_id, user_id)

    def get_books_by_person_for_user_sync(self, person_id: str, user_id: str) -> dict[str, list[dict[str, Any]]]:
        return self.catalog_service.get_books_by_person_for_user_sync(person_id, user_id)

    def get_contribution_type_counts_sync(self) -> dict[str, int]:
        return self.catalog_service.get_contribution_type_counts_sync()

    def get_book_series_relationship_sync(self, book_id: str) -> dict[str, Any] | None:
        return self.series_service.get_book_series_relationship_sync(book_id)

    def set_book_publisher_sync(self, book_id: str, publisher_name: str | None, user_id: str | None = None) -> bool:
        return self.book_service.replace_publisher_sync(book_id, publisher_name)

    def get_raw_book_graph_sync(self, book_id: str, user_id: str) -> dict[str, Any]:
        """Read the lossless migrated graph snapshot for the raw diagnostic view."""
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT b.properties, "
                "COALESCE((SELECT jsonb_agg(jsonb_build_object('direction','OUT','relationship',"
                "jsonb_build_object('type',e.relationship,'properties',e.properties),'target',n.properties)) "
                "FROM app_graph_edges e LEFT JOIN app_graph_nodes n ON n.label=e.target_label AND n.node_id=e.target_id "
                "WHERE e.source_label='Book' AND e.source_id=b.node_id), '[]'::jsonb), "
                "COALESCE((SELECT jsonb_agg(jsonb_build_object('direction','IN','relationship',"
                "jsonb_build_object('type',e.relationship,'properties',e.properties),'source',n.properties)) "
                "FROM app_graph_edges e LEFT JOIN app_graph_nodes n ON n.label=e.source_label AND n.node_id=e.source_id "
                "WHERE e.target_label='Book' AND e.target_id=b.node_id), '[]'::jsonb), "
                "(SELECT e.properties FROM app_graph_edges e WHERE e.relationship='HAS_PERSONAL_METADATA' "
                "AND e.source_label='User' AND e.source_id=%s AND e.target_label='Book' AND e.target_id=b.node_id "
                "ORDER BY e.edge_id LIMIT 1) "
                "FROM app_graph_nodes b WHERE b.label='Book' AND b.node_id=%s LIMIT 1",
                (str(user_id), str(book_id)),
            )
            row = cursor.fetchone()
        if not row:
            return {}
        return {
            "book_node": row[0], "book_labels": ["Book"],
            "outgoing_rels_raw": row[1] or [], "incoming_rels_raw": row[2] or [],
            "personal_metadata": row[3],
        }

    def create_person_sync(self, person_data: Any) -> dict[str, Any] | None:
        return self.catalog_service.create_person_sync(person_data)

    def update_person_sync(self, person_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
        return self.catalog_service.update_person_sync(person_id, updates)

    def delete_person_sync(self, person_id: str, *, force: bool = False) -> bool:
        return self.catalog_service.delete_person_sync(person_id, force=force)

    def merge_persons_sync(self, primary_person_id: str, merge_person_ids: list[str]) -> int:
        return self.catalog_service.merge_persons_sync(primary_person_id, merge_person_ids)

    async def list_all_categories(self, user_id: str | None = None) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self.list_all_categories_sync, user_id)

    async def get_category_by_id(self, category_id: str, user_id: str | None = None) -> Category | None:
        return await asyncio.to_thread(self.get_category_by_id_sync, category_id, user_id)

    async def get_child_categories(self, parent_id: str) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self.get_child_categories_sync, parent_id)

    async def get_category_children(self, category_id: str, user_id: str | None = None) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self.get_category_children_sync, category_id, user_id)

    async def get_books_by_category(
        self, category_id: str, user_id: str | None = None, include_subcategories: bool = False,
    ) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self.get_books_by_category_sync, category_id, user_id, include_subcategories)

    async def list_all_persons(self, user_id: str | None = None) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self.list_all_persons_sync, user_id)

    async def get_person_by_id(self, person_id: str) -> dict[str, Any] | None:
        return await asyncio.to_thread(self.get_person_by_id_sync, person_id)

    async def search_books(self, query: str, user_id: str, limit: int = 50) -> list[Book]:
        return await asyncio.to_thread(self.search_books_sync, query, user_id, limit)

    def _get_all_descendant_categories(self, category_id: str) -> list[str]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "WITH RECURSIVE descendants(id) AS (SELECT id FROM categories WHERE parent_id=%s "
                "UNION ALL SELECT c.id FROM categories c JOIN descendants d ON c.parent_id=d.id) SELECT id FROM descendants",
                (category_id,),
            )
            return [str(row[0]) for row in cursor.fetchall()]

    def _build_category_with_hierarchy(self, category_data: dict[str, Any]) -> dict[str, Any]:
        item = dict(category_data)
        chain: list[dict[str, Any]] = []
        seen = {str(item.get("id"))}
        parent_id = item.get("parent_id")
        while parent_id and str(parent_id) not in seen:
            seen.add(str(parent_id))
            parent = self.catalog_service.get_category_by_id_sync(str(parent_id))
            if not parent:
                break
            parent_dict = parent.__dict__.copy()
            chain.append(parent_dict)
            parent_id = parent.parent_id
        item["hierarchy"] = list(reversed(chain))
        return item

    async def update_book_relationships(self, user_id: str, book_id: str, **updates: Any) -> bool:
        return await asyncio.to_thread(self.library_service.update_user_book_sync, user_id, book_id, updates)

    def get_recently_added_books_sync(self, limit: int = 5) -> list[Book]:
        limit = max(0, min(int(limit), 500))
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT id FROM books ORDER BY created_at DESC NULLS LAST, id LIMIT %s", (limit,))
            ids = [row[0] for row in cursor.fetchall()]
        return [book for book_id in ids if (book := self.book_service.get_book_by_id_sync(book_id))]

    def get_recently_added_want_to_read_books(self, user_id: str, limit: int = 5) -> list[Book]:
        rows = self.library_service.get_books_with_user_overlay_paginated_sync(user_id, min(limit * 4, 1000), 0, "date_added_desc")
        selected = [row for row in rows if row.get("reading_status") in ("plan_to_read", "want_to_read")][:limit]
        return [self._overlay_to_book(row) for row in selected]

    def get_recently_read_books_for_user(self, user_id: str, limit: int = 5) -> list[dict[str, Any]]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT b.id,b.title,b.cover_url,l.date,l.pages_read,l.minutes_read,l.notes "
                "FROM reading_logs l JOIN books b ON b.id=l.book_id WHERE l.user_id=%s "
                "ORDER BY l.date DESC NULLS LAST,l.created_at DESC NULLS LAST,l.id LIMIT %s",
                (user_id, max(0, min(int(limit), 1000))),
            )
            return [dict(zip(("id", "title", "cover_url", "date", "pages_read", "minutes_read", "notes"), row)) for row in cursor.fetchall()]

    def get_books_with_sharing_users_sync(self, days_back: int = 30, limit: int = 20) -> list[Book]:
        since = datetime.now(timezone.utc) - timedelta(days=max(0, int(days_back)))
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT DISTINCT b.id FROM books b JOIN user_books ub ON ub.book_id=b.id "
                "JOIN users u ON u.id=ub.user_id WHERE u.share_reading_activity AND ub.date_added >= %s "
                "ORDER BY b.id LIMIT %s",
                (since, max(0, min(int(limit), 1000))),
            )
            ids = [row[0] for row in cursor.fetchall()]
        return [book for book_id in ids if (book := self.book_service.get_book_by_id_sync(book_id))]

    def get_currently_reading_shared_sync(self, limit: int = 20) -> list[Book]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT DISTINCT b.id FROM books b JOIN user_books ub ON ub.book_id=b.id "
                "JOIN users u ON u.id=ub.user_id WHERE u.share_current_reading "
                "AND ub.reading_status IN ('currently_reading','reading','in_progress') "
                "ORDER BY b.id LIMIT %s",
                (max(0, min(int(limit), 1000)),),
            )
            ids = [row[0] for row in cursor.fetchall()]
        return [book for book_id in ids if (book := self.book_service.get_book_by_id_sync(book_id))]

    async def get_books_with_sharing_users(self, days_back: int = 30, limit: int = 20) -> list[Book]:
        return await asyncio.to_thread(self.get_books_with_sharing_users_sync, days_back, limit)

    async def get_currently_reading_shared(self, limit: int = 20) -> list[Book]:
        return await asyncio.to_thread(self.get_currently_reading_shared_sync, limit)
