"""PostgreSQL category and contributor queries for the application facade."""

from __future__ import annotations

import asyncio
import json
import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any

from ..domain.models import Category, Person
from ..infrastructure.postgres_store import PostgresStore, get_postgres_store
from ..infrastructure.postgres_migration import _stored_edge_hash


_CATEGORY_COLUMNS = (
    "id", "name", "normalized_name", "parent_id", "description", "level", "color", "icon",
    "aliases", "book_count", "user_book_count", "created_at", "updated_at",
)
_CATEGORY_LINKS = """(
    SELECT book_id, target_id AS category_id FROM book_relationship_edges
    WHERE relationship = 'CATEGORIZED_AS' AND source_label = 'Book'
    UNION
    SELECT book_id, source_id AS category_id FROM book_relationship_edges
    WHERE relationship = 'CATEGORIZED_AS' AND target_label = 'Book'
    UNION
    SELECT book_id, category_id FROM book_categories
)"""


def _person_book_record(row: tuple[Any, ...], *, user_scoped: bool) -> dict[str, Any]:
    keys = (
        ("id", "title", "subtitle", "isbn13", "isbn10", "cover_url", "contribution_type", "role")
        if user_scoped else
        ("id", "title", "subtitle", "isbn13", "isbn10", "cover_url", "relationship_type", "role")
    )
    record = dict(zip(keys, row))
    book_id = record.get("uid") or record.get("id") or record.get("book_id")
    if book_id:
        record["uid"] = str(book_id)
    return record


def _category_from_row(row: Any) -> Category:
    return Category(
        id=row[0], name=row[1] or "", normalized_name=row[2] or "", parent_id=row[3],
        description=row[4], level=int(row[5] or 0), color=row[6], icon=row[7],
        aliases=row[8] or [], book_count=int(row[9] or 0), user_book_count=int(row[10] or 0),
        created_at=row[11] or datetime.now(timezone.utc), updated_at=row[12] or row[11] or datetime.now(timezone.utc),
    )


class PostgresCatalogService:
    """Normalized category/person lookups and category mutations."""

    def __init__(self, store: PostgresStore | None = None):
        self.store = store or get_postgres_store()

    def list_all_categories_sync(self, user_id: str | None = None) -> list[dict[str, Any]]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT c.id, c.name, c.normalized_name, c.parent_id, c.description, c.level, c.color, "
                "c.icon, c.aliases, count(DISTINCT links.book_id), "
                "count(DISTINCT CASE WHEN ub.user_id = %s THEN links.book_id END), c.created_at, c.updated_at "
                "FROM categories c LEFT JOIN " + _CATEGORY_LINKS + " links ON links.category_id = c.id "
                "LEFT JOIN user_books ub ON ub.book_id = links.book_id "
                "GROUP BY c.id ORDER BY c.level, lower(c.name), c.id",
                (user_id,),
            )
            rows = cursor.fetchall()
        return [dict(zip(_CATEGORY_COLUMNS, row)) for row in rows]

    async def list_all_categories(self, user_id: str | None = None) -> list[dict[str, Any]]:
        return await asyncio.to_thread(self.list_all_categories_sync, user_id)

    def get_category_by_id_sync(self, category_id: str, user_id: str | None = None) -> Category | None:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT c.id, c.name, c.normalized_name, c.parent_id, c.description, c.level, c.color, "
                "c.icon, c.aliases, count(DISTINCT links.book_id), "
                "count(DISTINCT CASE WHEN ub.user_id = %s THEN links.book_id END), c.created_at, c.updated_at "
                "FROM categories c LEFT JOIN " + _CATEGORY_LINKS + " links ON links.category_id = c.id "
                "LEFT JOIN user_books ub ON ub.book_id = links.book_id WHERE c.id = %s GROUP BY c.id",
                (user_id, category_id),
            )
            row = cursor.fetchone()
        if not row:
            return None
        leaf = _category_from_row(row)
        current = leaf
        seen = {str(leaf.id)}
        parent_id = leaf.parent_id
        while parent_id and str(parent_id) not in seen:
            seen.add(str(parent_id))
            parent = self._get_category_without_hierarchy_sync(str(parent_id), user_id)
            if not parent:
                break
            current.parent = parent
            current = parent
            parent_id = parent.parent_id
        return leaf

    def _get_category_without_hierarchy_sync(self, category_id: str, user_id: str | None) -> Category | None:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT c.id, c.name, c.normalized_name, c.parent_id, c.description, c.level, c.color, "
                "c.icon, c.aliases, count(DISTINCT links.book_id), "
                "count(DISTINCT CASE WHEN ub.user_id = %s THEN links.book_id END), c.created_at, c.updated_at "
                "FROM categories c LEFT JOIN " + _CATEGORY_LINKS + " links ON links.category_id = c.id "
                "LEFT JOIN user_books ub ON ub.book_id = links.book_id WHERE c.id = %s GROUP BY c.id",
                (user_id, category_id),
            )
            row = cursor.fetchone()
        return _category_from_row(row) if row else None

    def get_child_categories_sync(self, parent_id: str) -> list[dict[str, Any]]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {', '.join(_CATEGORY_COLUMNS)} FROM categories WHERE parent_id = %s "
                "ORDER BY lower(name), id",
                (parent_id,),
            )
            return [dict(zip(_CATEGORY_COLUMNS, row)) for row in cursor.fetchall()]

    def get_category_children_sync(self, category_id: str, user_id: str | None = None) -> list[dict[str, Any]]:
        return self.get_child_categories_sync(category_id)

    def get_root_categories_sync(self, user_id: str | None = None) -> list[dict[str, Any]]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {', '.join(_CATEGORY_COLUMNS)} FROM categories WHERE parent_id IS NULL "
                "ORDER BY lower(name), id"
            )
            return [dict(zip(_CATEGORY_COLUMNS, row)) for row in cursor.fetchall()]

    def get_category_book_counts_sync(self) -> dict[str, int]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT c.id, count(DISTINCT links.book_id) FROM categories c LEFT JOIN "
                + _CATEGORY_LINKS + " links ON links.category_id=c.id GROUP BY c.id"
            )
            return {str(category_id): int(count) for category_id, count in cursor.fetchall()}

    def get_book_categories_sync(self, book_id: str) -> list[dict[str, Any]]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT c.id,c.name,c.normalized_name,c.parent_id,c.description,c.level,c.color,c.icon,"
                "c.aliases,c.book_count,c.user_book_count,c.created_at,c.updated_at FROM book_categories bc "
                "JOIN categories c ON c.id=bc.category_id WHERE bc.book_id=%s ORDER BY lower(c.name),c.id",
                (book_id,),
            )
            rows = cursor.fetchall()
            if not rows:
                cursor.execute(
                    "SELECT c.id,c.name,c.normalized_name,c.parent_id,c.description,c.level,c.color,c.icon,"
                    "c.aliases,c.book_count,c.user_book_count,c.created_at,c.updated_at FROM categories c "
                    "JOIN (SELECT DISTINCT CASE WHEN source_label='Book' THEN target_id ELSE source_id END AS category_id "
                    "FROM book_relationship_edges WHERE book_id=%s AND relationship='CATEGORIZED_AS') e ON e.category_id=c.id "
                    "ORDER BY lower(c.name),c.id",
                    (book_id,),
                )
                rows = cursor.fetchall()
        return [dict(zip(_CATEGORY_COLUMNS, row)) for row in rows]

    def search_categories_sync(self, query: str, limit: int = 10, user_id: str | None = None) -> list[dict[str, Any]]:
        limit = max(0, min(int(limit), 500))
        pattern = f"%{str(query or '').strip()}%"
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT c.id, c.name, c.normalized_name, c.parent_id, c.description, c.level, c.color, "
                "c.icon, c.aliases, count(DISTINCT links.book_id), "
                "count(DISTINCT CASE WHEN ub.user_id = %s THEN links.book_id END), c.created_at, c.updated_at "
                "FROM categories c LEFT JOIN " + _CATEGORY_LINKS + " links ON links.category_id = c.id "
                "LEFT JOIN user_books ub ON ub.book_id = links.book_id "
                "WHERE c.name ILIKE %s OR c.normalized_name ILIKE %s "
                "GROUP BY c.id ORDER BY lower(c.name), c.id LIMIT %s",
                (user_id, pattern, pattern, limit),
            )
            return [dict(zip(_CATEGORY_COLUMNS, row)) for row in cursor.fetchall()]

    def get_books_by_category_sync(
        self, category_id: str, user_id: str | None = None, include_subcategories: bool = False,
    ) -> list[dict[str, Any]]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            if include_subcategories:
                cursor.execute(
                    "WITH RECURSIVE descendants(id) AS (SELECT id FROM categories WHERE id = %s "
                    "UNION ALL SELECT c.id FROM categories c JOIN descendants d ON c.parent_id = d.id) "
                    "SELECT DISTINCT b.id, b.title, b.subtitle, b.isbn13, b.isbn10, b.cover_url, "
                    "ub.reading_status, ub.ownership_status, ub.user_rating, b.created_at "
                    "FROM descendants d JOIN " + _CATEGORY_LINKS + " links ON links.category_id = d.id "
                    "JOIN books b ON b.id = links.book_id LEFT JOIN user_books ub "
                    "ON ub.book_id = b.id AND ub.user_id = %s ORDER BY b.title, b.id",
                    (category_id, user_id),
                )
            else:
                cursor.execute(
                    "SELECT DISTINCT b.id, b.title, b.subtitle, b.isbn13, b.isbn10, b.cover_url, "
                    "ub.reading_status, ub.ownership_status, ub.user_rating, b.created_at "
                    "FROM " + _CATEGORY_LINKS + " links JOIN books b ON b.id = links.book_id "
                    "LEFT JOIN user_books ub ON ub.book_id = b.id AND ub.user_id = %s "
                    "WHERE links.category_id = %s ORDER BY b.title, b.id",
                    (user_id, category_id),
                )
            return [dict(zip(("id", "title", "subtitle", "isbn13", "isbn10", "cover_url", "reading_status", "ownership_status", "user_rating", "created_at"), row)) for row in cursor.fetchall()]

    def create_category_sync(self, category_data: dict[str, Any] | Category) -> Category | None:
        values = category_data.__dict__ if isinstance(category_data, Category) else dict(category_data)
        name = str(values.get("name") or "").strip()
        if not name:
            return None
        category_id = str(values.get("id") or uuid.uuid4())
        parent_id = values.get("parent_id")
        now = datetime.now(timezone.utc)
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO categories(id,name,normalized_name,parent_id,description,level,color,icon,aliases,created_at,updated_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s) RETURNING " + ", ".join(_CATEGORY_COLUMNS),
                (category_id, name, Category._normalize_name(name), parent_id, values.get("description"),
                 int(values.get("level") or 0), values.get("color"), values.get("icon"),
                 json.dumps(values.get("aliases") or []), now, now),
            )
            row = cursor.fetchone()
        return _category_from_row(row) if row else None

    def update_category_sync(self, category: Category) -> Category | None:
        if not category.id or not category.name.strip():
            return None
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute(
                "UPDATE categories SET name=%s, normalized_name=%s, parent_id=%s, description=%s, level=%s, "
                "color=%s, icon=%s, aliases=%s::jsonb, updated_at=now() WHERE id=%s RETURNING " + ", ".join(_CATEGORY_COLUMNS),
                (category.name, Category._normalize_name(category.name), category.parent_id, category.description,
                 category.level, category.color, category.icon, json.dumps(category.aliases or []), category.id),
            )
            row = cursor.fetchone()
        return _category_from_row(row) if row else None

    def delete_category_sync(self, category_id: str) -> bool:
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM categories WHERE parent_id=%s LIMIT 1", (category_id,))
            if cursor.fetchone():
                return False
            cursor.execute(
                "SELECT 1 FROM book_categories WHERE category_id=%s UNION ALL "
                "SELECT 1 FROM book_relationship_edges WHERE relationship='CATEGORIZED_AS' "
                "AND ((source_label='Book' AND target_id=%s) OR (target_label='Book' AND source_id=%s)) LIMIT 1",
                (category_id, category_id, category_id),
            )
            if cursor.fetchone():
                return False
            cursor.execute("DELETE FROM categories WHERE id=%s", (category_id,))
            return cursor.rowcount > 0

    def merge_categories_sync(self, primary_category_id: str, merge_category_ids: list[str]) -> bool:
        merge_ids = sorted({str(item) for item in merge_category_ids if str(item) != str(primary_category_id)})
        if not merge_ids:
            return True
        from ..infrastructure.postgres_migration import _stored_edge_hash

        try:
            with self.store.transaction() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT id, aliases FROM categories WHERE id=%s FOR UPDATE", (primary_category_id,))
                primary = cursor.fetchone()
                if not primary:
                    return False
                aliases = list(primary[1] or [])
                for old_id in merge_ids:
                    cursor.execute("SELECT id, aliases FROM categories WHERE id=%s FOR UPDATE", (old_id,))
                    old = cursor.fetchone()
                    if not old:
                        return False
                    aliases.extend(alias for alias in (old[1] or []) if alias not in aliases)
                    cursor.execute(
                        "INSERT INTO book_categories(book_id,category_id,created_at) "
                        "SELECT book_id,%s,created_at FROM book_categories WHERE category_id=%s "
                        "ON CONFLICT(book_id,category_id) DO NOTHING",
                        (primary_category_id, old_id),
                    )
                    cursor.execute("DELETE FROM book_categories WHERE category_id=%s", (old_id,))
                    cursor.execute(
                        "SELECT source_edge_id,relationship,source_label,source_id,target_label,target_id,properties "
                        "FROM book_relationship_edges WHERE relationship='CATEGORIZED_AS' AND "
                        "((source_label='Category' AND source_id=%s) OR (target_label='Category' AND target_id=%s)) FOR UPDATE",
                        (old_id, old_id),
                    )
                    edges = cursor.fetchall()
                    for edge_id, relationship, source_label, source_id, target_label, target_id, properties in edges:
                        source_id = primary_category_id if source_label == "Category" and source_id == old_id else source_id
                        target_id = primary_category_id if target_label == "Category" and target_id == old_id else target_id
                        digest = _stored_edge_hash(
                            str(relationship), str(source_label), str(source_id),
                            str(target_label), str(target_id), properties,
                        )
                        cursor.execute(
                            "UPDATE book_relationship_edges SET source_id=%s,target_id=%s,content_sha256=%s "
                            "WHERE source_edge_id=%s",
                            (source_id, target_id, digest, edge_id),
                        )
                    cursor.execute("UPDATE categories SET parent_id=%s WHERE parent_id=%s", (primary_category_id, old_id))
                    cursor.execute("DELETE FROM categories WHERE id=%s", (old_id,))
                cursor.execute(
                    "UPDATE categories SET aliases=%s::jsonb,updated_at=now() WHERE id=%s",
                    (json.dumps(aliases, ensure_ascii=False), primary_category_id),
                )
            return True
        except Exception:
            return False

    def list_all_persons_sync(self, user_id: str | None = None) -> list[dict[str, Any]]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT id,name,normalized_name,birth_date,death_date,birth_year,death_year,birth_place,bio,website,"
                "openlibrary_id,image_url,wikidata_id,imdb_id,alternate_names,fuller_name,title,official_links,created_at,updated_at "
                "FROM people ORDER BY lower(name),id"
            )
            columns = ("id", "name", "normalized_name", "birth_date", "death_date", "birth_year", "death_year",
                       "birth_place", "bio", "website", "openlibrary_id", "image_url", "wikidata_id", "imdb_id",
                       "alternate_names", "fuller_name", "title", "official_links", "created_at", "updated_at")
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_total_person_count_sync(self) -> int:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM people")
            return int(cursor.fetchone()[0])

    def get_person_by_id_sync(self, person_id: str) -> dict[str, Any] | None:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT id,name,normalized_name,birth_date,death_date,birth_year,death_year,birth_place,bio,website,"
                "openlibrary_id,image_url,wikidata_id,imdb_id,alternate_names,fuller_name,title,official_links,created_at,updated_at "
                "FROM people WHERE id=%s", (person_id,),
            )
            row = cursor.fetchone()
        columns = ("id", "name", "normalized_name", "birth_date", "death_date", "birth_year", "death_year",
                   "birth_place", "bio", "website", "openlibrary_id", "image_url", "wikidata_id", "imdb_id",
                   "alternate_names", "fuller_name", "title", "official_links", "created_at", "updated_at")
        return dict(zip(columns, row)) if row else None

    def get_person_by_name_sync(self, name: str) -> dict[str, Any] | None:
        normalized = Person._normalize_name(str(name or ""))
        if not normalized:
            return None
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT id,name,normalized_name,birth_date,death_date,birth_year,death_year,birth_place,bio,website,"
                "openlibrary_id,image_url,wikidata_id,imdb_id,alternate_names,fuller_name,title,official_links,created_at,updated_at "
                "FROM people WHERE normalized_name=%s OR lower(name)=lower(%s) ORDER BY id LIMIT 1",
                (normalized, name.strip()),
            )
            row = cursor.fetchone()
        columns = ("id", "name", "normalized_name", "birth_date", "death_date", "birth_year", "death_year",
                   "birth_place", "bio", "website", "openlibrary_id", "image_url", "wikidata_id", "imdb_id",
                   "alternate_names", "fuller_name", "title", "official_links", "created_at", "updated_at")
        return dict(zip(columns, row)) if row else None

    def create_person_sync(self, person_data: dict[str, Any] | Person) -> dict[str, Any] | None:
        values = dict(person_data.__dict__) if isinstance(person_data, Person) else dict(person_data)
        name = str(values.get("name") or "").strip()
        if not name:
            return None
        person_id = str(values.get("id") or uuid.uuid4())
        now = datetime.now(timezone.utc)
        json_fields = {"alternate_names", "official_links"}
        fields = (
            "id", "name", "normalized_name", "birth_date", "death_date", "birth_year", "death_year",
            "birth_place", "bio", "website", "openlibrary_id", "image_url", "wikidata_id", "imdb_id",
            "alternate_names", "fuller_name", "title", "official_links", "created_at", "updated_at",
        )
        normalized = dict(values)
        normalized.update(id=person_id, name=name, normalized_name=Person._normalize_name(name))
        normalized.setdefault("created_at", now)
        normalized["updated_at"] = now
        payload = []
        for field in fields:
            value = normalized.get(field)
            if field in json_fields and isinstance(value, str):
                try:
                    value = json.loads(value)
                except (TypeError, ValueError):
                    value = [] if field == "alternate_names" else []
            payload.append(json.dumps(value, ensure_ascii=False) if field in json_fields else value)
        placeholders = ["%s::jsonb" if field in json_fields else "%s" for field in fields]
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO people ({','.join(fields)}) VALUES ({','.join(placeholders)}) ON CONFLICT (id) DO NOTHING",
                payload,
            )
            created = cursor.rowcount == 1
        if not created:
            return None
        self._invalidate_people()
        return self.get_person_by_id_sync(person_id)

    def update_person_sync(self, person_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
        allowed = {
            "name", "normalized_name", "birth_date", "death_date", "birth_year", "death_year",
            "birth_place", "bio", "website", "openlibrary_id", "image_url", "wikidata_id", "imdb_id",
            "alternate_names", "fuller_name", "title", "official_links",
        }
        fields = [name for name in updates if name in allowed]
        if not fields:
            return self.get_person_by_id_sync(person_id)
        values = dict(updates)
        if "name" in values:
            values["name"] = str(values["name"] or "").strip()
            if not values["name"]:
                return None
            values["normalized_name"] = Person._normalize_name(values["name"])
            if "normalized_name" not in fields:
                fields.append("normalized_name")
        assignments = []
        params: list[Any] = []
        for name in fields:
            value = values.get(name)
            if name in {"alternate_names", "official_links"}:
                if isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except (TypeError, ValueError):
                        value = []
                assignments.append(f"{name}=%s::jsonb")
                params.append(json.dumps(value, ensure_ascii=False))
            else:
                assignments.append(f"{name}=%s")
                params.append(value)
        assignments.append("updated_at=%s")
        params.extend((datetime.now(timezone.utc), str(person_id)))
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute(f"UPDATE people SET {','.join(assignments)} WHERE id=%s", tuple(params))
            if cursor.rowcount != 1:
                return None
        self._invalidate_people()
        return self.get_person_by_id_sync(person_id)

    def delete_person_sync(self, person_id: str, *, force: bool = False) -> bool:
        try:
            with self.store.transaction() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT id FROM people WHERE id=%s FOR UPDATE", (str(person_id),))
                if not cursor.fetchone():
                    return False
                cursor.execute(
                    "SELECT EXISTS(SELECT 1 FROM book_contributors WHERE person_id=%s) OR EXISTS("
                    "SELECT 1 FROM book_relationship_edges WHERE "
                    "(source_label='Person' AND source_id=%s) OR (target_label='Person' AND target_id=%s))",
                    (str(person_id), str(person_id), str(person_id)),
                )
                linked = bool(cursor.fetchone()[0])
                if linked and not force:
                    return False
                if force:
                    cursor.execute("DELETE FROM book_contributors WHERE person_id=%s", (str(person_id),))
                    cursor.execute(
                        "DELETE FROM book_relationship_edges WHERE "
                        "(source_label='Person' AND source_id=%s) OR (target_label='Person' AND target_id=%s)",
                        (str(person_id), str(person_id)),
                    )
                cursor.execute("DELETE FROM people WHERE id=%s", (str(person_id),))
                deleted = cursor.rowcount == 1
            if deleted:
                self._invalidate_people()
            return deleted
        except Exception:
            return False

    def merge_persons_sync(self, primary_person_id: str, merge_person_ids: list[str]) -> int:
        ids = list(dict.fromkeys(str(value) for value in merge_person_ids if str(value) != str(primary_person_id)))
        if not ids:
            return 0
        merged = 0
        try:
            with self.store.transaction() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT id FROM people WHERE id=%s FOR UPDATE", (str(primary_person_id),))
                if not cursor.fetchone():
                    return 0
                for other_id in ids:
                    cursor.execute("SELECT id FROM people WHERE id=%s FOR UPDATE", (other_id,))
                    if not cursor.fetchone():
                        continue
                    cursor.execute(
                        """INSERT INTO book_contributors(source_relationship,book_id,person_id,contribution_type,role,order_index,created_at,extra)
                        SELECT source_relationship,book_id,%s,contribution_type,role,order_index,created_at,extra
                        FROM book_contributors old WHERE old.person_id=%s AND NOT EXISTS (
                            SELECT 1 FROM book_contributors current WHERE current.book_id=old.book_id AND current.person_id=%s
                            AND coalesce(current.contribution_type,'')=coalesce(old.contribution_type,'')
                            AND coalesce(current.role,'')=coalesce(old.role,''))""",
                        (str(primary_person_id), other_id, str(primary_person_id)),
                    )
                    cursor.execute("DELETE FROM book_contributors WHERE person_id=%s", (other_id,))
                    cursor.execute(
                        "SELECT source_edge_id,relationship,source_label,source_id,target_label,target_id,properties "
                        "FROM book_relationship_edges WHERE (source_label='Person' AND source_id=%s) "
                        "OR (target_label='Person' AND target_id=%s) FOR UPDATE",
                        (other_id, other_id),
                    )
                    for edge_id, relationship, source_label, source_id, target_label, target_id, properties in cursor.fetchall():
                        new_source = str(primary_person_id) if source_label == "Person" and source_id == other_id else source_id
                        new_target = str(primary_person_id) if target_label == "Person" and target_id == other_id else target_id
                        digest = _stored_edge_hash(str(relationship), str(source_label), str(new_source), str(target_label), str(new_target), properties)
                        cursor.execute(
                            "UPDATE book_relationship_edges SET source_id=%s,target_id=%s,content_sha256=%s WHERE source_edge_id=%s",
                            (new_source, new_target, digest, edge_id),
                        )
                    cursor.execute("DELETE FROM people WHERE id=%s", (other_id,))
                    if cursor.rowcount:
                        merged += 1
            if merged:
                self._invalidate_people()
            return merged
        except Exception:
            return 0

    def _invalidate_people(self) -> None:
        from ..infrastructure import redis_cache
        redis_cache.invalidate_tags("postgres:people", "postgres:books")

    def get_books_by_person_sync(
        self, person_id: str, user_id: str | None = None,
    ) -> dict[str, list[dict[str, Any]]] | list[dict[str, Any]]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            sql = (
                "SELECT b.id,b.title,b.subtitle,b.isbn13,b.isbn10,b.cover_url,c.contribution_type,c.role "
                "FROM book_contributors c JOIN books b ON b.id=c.book_id "
            )
            params: tuple[Any, ...]
            if user_id is None:
                sql += "WHERE c.person_id=%s ORDER BY c.order_index NULLS LAST,lower(b.title),b.id"
                params = (person_id,)
            else:
                sql += (
                    "JOIN user_books ub ON ub.book_id=b.id AND ub.user_id=%s "
                    "WHERE c.person_id=%s ORDER BY c.order_index NULLS LAST,lower(b.title),b.id"
                )
                params = (user_id, person_id)
            cursor.execute(sql, params)
            rows = cursor.fetchall()
        if user_id is None:
            return [
                _person_book_record(row, user_scoped=False)
                for row in rows
            ]
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            record = _person_book_record(row, user_scoped=True)
            grouped.setdefault(str(row[6] or row[7] or "AUTHORED"), []).append(record)
        return grouped

    def get_books_by_person_for_user_sync(self, person_id: str, user_id: str) -> dict[str, list[dict[str, Any]]]:
        result = self.get_books_by_person_sync(person_id, user_id)
        return result if isinstance(result, dict) else {}

    def get_contribution_type_counts_sync(self) -> dict[str, int]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT coalesce(contribution_type,role,'AUTHORED'),count(DISTINCT person_id) "
                "FROM book_contributors GROUP BY coalesce(contribution_type,role,'AUTHORED')"
            )
            return {str(row[0]): int(row[1]) for row in cursor.fetchall()}
