"""PostgreSQL implementation of custom field definitions and metadata values."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from ..infrastructure import redis_cache
from ..infrastructure.postgres_store import PostgresStore, get_postgres_store

RESERVED_CORE_BOOK_FIELDS = {"subtitle", "google_books_id", "openlibrary_id"}
_FIELD_TYPES = {"text", "textarea", "number", "date", "boolean", "rating_5", "rating_10", "tags"}
_DEFINITION_SOURCE = """(
    SELECT id,name,display_name,field_type,description,created_by_user_id,is_global,created_at,updated_at,usage_count,
           'custom_field_definitions'::text AS source_table
    FROM custom_field_definitions
    UNION ALL
    SELECT id,name,display_name,field_type,description,created_by_user_id,is_global,created_at,updated_at,usage_count,
           'legacy_custom_field_definitions'::text AS source_table
    FROM legacy_custom_field_definitions
) fields"""


def is_reserved_core_field(field_name: str) -> bool:
    return bool(field_name and field_name.lower() in RESERVED_CORE_BOOK_FIELDS)


class PostgresCustomFieldService:
    """Keep definitions relational and metadata values with their book scope."""

    def __init__(self, store: PostgresStore | None = None):
        self.store = store or get_postgres_store()

    @staticmethod
    def _definition(row: Any) -> dict[str, Any] | None:
        if not row:
            return None
        if not isinstance(row, dict):
            row = dict(zip((
                "id", "name", "display_name", "field_type", "description", "created_by_user_id",
                "is_global", "created_at", "updated_at", "usage_count", "source_table",
            ), row))
        if is_reserved_core_field(row.get("name")):
            return None
        return {
            "id": row.get("id"), "name": row.get("name"),
            "display_name": row.get("display_name"), "field_type": row.get("field_type") or "text",
            "description": row.get("description") or "", "is_global": bool(row.get("is_global")),
            "created_at": row.get("created_at"), "updated_at": row.get("updated_at"),
            "created_by_user_id": row.get("created_by_user_id"),
            "usage_count": int(row.get("usage_count") or 0),
            "source_table": row.get("source_table") or "custom_field_definitions",
        }

    def _list_definitions(self, *, owner_id: str | None = None, is_global: bool | None = None) -> list[dict[str, Any]]:
        where: list[str] = []
        params: list[Any] = []
        if owner_id is not None:
            where.append("(created_by_user_id = %s OR is_global = TRUE)")
            params.append(str(owner_id))
        if is_global is not None:
            where.append("is_global = %s")
            params.append(is_global)
        sql = "SELECT id,name,display_name,field_type,description,created_by_user_id,is_global,created_at,updated_at,usage_count,source_table FROM " + _DEFINITION_SOURCE
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at DESC NULLS LAST, id"
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(sql, tuple(params))
            values = [value for row in cursor.fetchall() if (value := self._definition(row)) is not None]
            deduplicated: dict[str, dict[str, Any]] = {}
            for value in values:
                deduplicated.setdefault(str(value.get("name") or "").lower(), value)
            return list(deduplicated.values())

    def get_user_fields_sync(self, user_id: str) -> list[dict[str, Any]]:
        # The legacy service exposed all definitions to the field editor.
        return self._list_definitions()

    def get_user_fields_with_calculated_usage_sync(self, user_id: str) -> list[dict[str, Any]]:
        fields = self._list_definitions(owner_id=str(user_id))
        for field in fields:
            field["usage_count"] = self.get_field_usage_count_sync(
                str(field.get("name") or ""), bool(field.get("is_global")),
            )
        return fields

    def get_available_fields_sync(self, user_id: str, is_global: bool | None = None) -> list[dict[str, Any]]:
        return self._list_definitions(owner_id=str(user_id), is_global=is_global)

    def _get_definition_by_name(self, name: str) -> dict[str, Any] | None:
        if is_reserved_core_field(name):
            return None
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT id,name,display_name,field_type,description,created_by_user_id,is_global,created_at,updated_at,usage_count,source_table "
                "FROM " + _DEFINITION_SOURCE + " WHERE lower(name)=lower(%s) ORDER BY is_global DESC,created_at,id LIMIT 1",
                (name,),
            )
            return self._definition(cursor.fetchone())

    def _get_field_definition(self, field_name: str) -> dict[str, Any] | None:
        return self._get_definition_by_name(field_name)

    def get_field_by_id_sync(self, field_id: str) -> dict[str, Any] | None:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT id,name,display_name,field_type,description,created_by_user_id,is_global,created_at,updated_at,usage_count,source_table "
                "FROM " + _DEFINITION_SOURCE + " WHERE id=%s", (field_id,),
            )
            field = self._definition(cursor.fetchone())
        if field:
            field["usage_count"] = self.get_field_usage_count_sync(field["name"], field["is_global"])
        return field

    def create_field_sync(self, user_id: str, field_data: dict[str, Any]) -> dict[str, Any] | None:
        name = str(field_data.get("name") or "unknown_field").strip()
        if not name or is_reserved_core_field(name):
            return None
        existing = self._get_definition_by_name(name)
        if existing:
            return existing
        now = datetime.now(timezone.utc)
        field_type = str(field_data.get("field_type") or "text")
        if field_type not in _FIELD_TYPES:
            field_type = "text"
        data = {
            "id": str(field_data.get("id") or f"field_{uuid.uuid4().hex}"),
            "name": name,
            "display_name": field_data.get("display_name") or name.replace("_", " ").title(),
            "field_type": field_type,
            "description": field_data.get("description") or "",
            "created_by_user_id": str(user_id),
            "is_global": bool(field_data.get("is_global", False)),
            "created_at": now, "updated_at": now,
        }
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO custom_field_definitions
                (id,name,display_name,field_type,description,created_by_user_id,is_global,created_at,updated_at)
                VALUES (%(id)s,%(name)s,%(display_name)s,%(field_type)s,%(description)s,%(created_by_user_id)s,%(is_global)s,%(created_at)s,%(updated_at)s)
                ON CONFLICT (id) DO NOTHING""",
                data,
            )
        self._invalidate()
        return self.get_field_by_id_sync(data["id"]) or self._get_definition_by_name(name)

    def update_field_sync(self, field_id: str, user_id: str, field_data: dict[str, Any]) -> dict[str, Any] | None:
        existing = self.get_field_by_id_sync(field_id)
        if not existing or existing.get("created_by_user_id") != str(user_id):
            return None
        name = str(field_data.get("name", existing["name"])).strip()
        if not name or is_reserved_core_field(name):
            return None
        field_type = str(field_data.get("field_type", existing["field_type"]))
        if field_type not in _FIELD_TYPES:
            field_type = "text"
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"""UPDATE {existing['source_table']} SET name=%s,display_name=%s,field_type=%s,description=%s,
                is_global=%s,updated_at=%s WHERE id=%s AND created_by_user_id=%s""",
                (name, field_data.get("display_name", existing["display_name"]), field_type,
                 field_data.get("description", existing["description"]),
                 bool(field_data.get("is_global", existing["is_global"])), datetime.now(timezone.utc),
                 field_id, str(user_id)),
            )
            if cursor.rowcount != 1:
                return None
        self._invalidate()
        return self.get_field_by_id_sync(field_id)

    def delete_field_sync(self, field_id: str, user_id: str) -> bool:
        existing = self.get_field_by_id_sync(field_id)
        if not existing or existing.get("created_by_user_id") != str(user_id):
            return False
        name = existing["name"]
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute("UPDATE books SET custom_metadata=custom_metadata-%s,updated_at=now() WHERE custom_metadata ? %s", (name, name))
            cursor.execute("UPDATE user_books SET custom_metadata=custom_metadata-%s,updated_at=now() WHERE custom_metadata ? %s", (name, name))
            cursor.execute("DELETE FROM custom_fields WHERE definition_id=%s", (field_id,))
            cursor.execute(f"DELETE FROM {existing['source_table']} WHERE id=%s AND created_by_user_id=%s", (field_id, str(user_id)))
            deleted = cursor.rowcount == 1
        if deleted:
            self._invalidate()
        return deleted

    def get_custom_metadata_sync(self, book_id: str, user_id: str) -> dict[str, Any]:
        tags = (
            "postgres:custom_fields", "postgres:books", "postgres:library",
            f"postgres:book_custom_metadata:{book_id}",
            f"postgres:user_custom_metadata:{user_id}:{book_id}",
        )
        key = f"postgres:custom_metadata:{user_id}:{book_id}"
        snapshot = redis_cache.snapshot_tag_versions(tags)
        hit, cached = redis_cache.get_json(key, tags=tags, snapshot=snapshot)
        if hit and isinstance(cached, dict):
            return cached
        result = self._read_custom_metadata(book_id, user_id)
        redis_cache.set_json(key, result, ttl_seconds=60, tags=tags, snapshot=snapshot)
        return result

    def _read_custom_metadata(self, book_id: str, user_id: str) -> dict[str, Any]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT custom_metadata FROM books WHERE id=%s", (str(book_id),))
            book_row = cursor.fetchone()
            if not book_row:
                return {}
            global_values = book_row[0] or {}
            if isinstance(global_values, str):
                try:
                    global_values = json.loads(global_values)
                except (TypeError, ValueError):
                    global_values = {}
            if not isinstance(global_values, dict):
                global_values = {}
            # Imported global metadata relationships remain available as a
            # fallback until they have been rewritten through the PG service.
            cursor.execute(
                "SELECT properties->>'global_custom_fields' FROM book_relationship_edges "
                "WHERE book_id=%s AND relationship='HAS_GLOBAL_METADATA' ORDER BY source_edge_id DESC LIMIT 1",
                (str(book_id),),
            )
            legacy = cursor.fetchone()
            if legacy and legacy[0]:
                try:
                    blob = json.loads(legacy[0]) if isinstance(legacy[0], str) else legacy[0]
                    if isinstance(blob, dict):
                        global_values = {**blob, **global_values}
                except (TypeError, ValueError):
                    pass
            cursor.execute(
                "SELECT field_name,field_value FROM custom_fields "
                "WHERE user_id=%s AND book_id=%s AND field_name IS NOT NULL "
                "ORDER BY created_at NULLS FIRST,id",
                (str(user_id), str(book_id)),
            )
            migrated_personal_values = {
                str(field_name): value
                for field_name, value in cursor.fetchall()
                if field_name is not None
            }
            cursor.execute("SELECT custom_metadata FROM user_books WHERE user_id=%s AND book_id=%s", (str(user_id), str(book_id)))
            user_row = cursor.fetchone()
            personal_values = (user_row[0] or {}) if user_row else {}
            if isinstance(personal_values, str):
                try:
                    personal_values = json.loads(personal_values)
                except (TypeError, ValueError):
                    personal_values = {}
            return {
                **global_values,
                **migrated_personal_values,
                **(personal_values if isinstance(personal_values, dict) else {}),
            }

    def save_custom_metadata_sync(self, book_id: str, user_id: str, custom_metadata: dict[str, Any]) -> bool:
        if not isinstance(custom_metadata, dict):
            return False
        global_values: dict[str, Any] = {}
        personal_values: dict[str, Any] = {}
        try:
            for name, value in custom_metadata.items():
                if value is None or value == "" or is_reserved_core_field(str(name)):
                    continue
                definition = self._get_definition_by_name(str(name))
                (global_values if definition and definition.get("is_global") else personal_values)[str(name)] = value
            with self.store.transaction() as connection, connection.cursor() as cursor:
                cursor.execute("SELECT id FROM books WHERE id=%s FOR UPDATE", (str(book_id),))
                if not cursor.fetchone():
                    return False
                if global_values:
                    cursor.execute(
                        "UPDATE books SET custom_metadata=custom_metadata || %s::jsonb,updated_at=now() WHERE id=%s",
                        (json.dumps(global_values, ensure_ascii=False), str(book_id)),
                    )
                if personal_values:
                    cursor.execute(
                        """UPDATE user_books SET custom_metadata=custom_metadata || %s::jsonb,updated_at=now()
                        WHERE user_id=%s AND book_id=%s""",
                        (json.dumps(personal_values, ensure_ascii=False), str(user_id), str(book_id)),
                    )
                    if cursor.rowcount == 0:
                        return False
            self._invalidate(book_id=str(book_id), user_id=str(user_id))
            return True
        except Exception:
            return False

    def get_custom_metadata_for_display(self, custom_metadata: dict[str, Any]) -> list[dict[str, Any]]:
        items = []
        for name, value in (custom_metadata or {}).items():
            if value is None or value == "":
                continue
            definition = self._get_definition_by_name(name)
            display_name = (definition or {}).get("display_name") or name.replace("_", " ").title()
            field_type = (definition or {}).get("field_type") or "text"
            items.append({"field_name": name, "display_name": display_name, "value": str(value), "display_value": str(value), "field_type": field_type})
        return items

    def ensure_custom_fields_exist(self, user_id: str, global_custom_metadata: dict[str, Any], personal_custom_metadata: dict[str, Any]) -> bool:
        try:
            for values, is_global in ((global_custom_metadata, True), (personal_custom_metadata, False)):
                for name, value in values.items():
                    if value is not None and value != "" and not is_reserved_core_field(name) and not self._get_definition_by_name(name):
                        if not self.create_field_sync(user_id, {"name": name, "is_global": is_global}):
                            return False
            return True
        except Exception:
            return False

    def get_field_usage_count_sync(self, field_name: str, is_global: bool) -> int:
        if is_reserved_core_field(field_name):
            return 0
        column = "books" if is_global else "user_books"
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(f"SELECT count(*) FROM {column} WHERE custom_metadata ? %s AND nullif(custom_metadata->>%s,'') IS NOT NULL", (field_name, field_name))
            return int(cursor.fetchone()[0])

    def _invalidate(self, *, book_id: str | None = None, user_id: str | None = None) -> None:
        tags = ["postgres:custom_fields", "postgres:books", "postgres:library"]
        if book_id:
            tags.extend((f"postgres:book:{book_id}", f"postgres:book_custom_metadata:{book_id}"))
        if user_id:
            tags.append(f"postgres:user_library:{user_id}")
            if book_id:
                tags.append(f"postgres:user_custom_metadata:{user_id}:{book_id}")
        redis_cache.invalidate_tags(*tags)

    # Legacy maintenance APIs retained with no-op semantics because metadata
    # is now normalized during migration and the immutable source archive is
    # retained for rollback.
    def migrate_custom_metadata_from_owns(self, book_id: str, user_id: str) -> bool:
        return True

    def migrate_all_custom_metadata_from_owns(self) -> bool:
        return True

    def cleanup_old_custom_field_nodes(self) -> bool:
        return True

    def cleanup_reserved_core_custom_fields(self) -> bool:
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM custom_fields WHERE definition_id IN "
                "(SELECT id FROM custom_field_definitions WHERE lower(name)=ANY(%s) UNION "
                "SELECT id FROM legacy_custom_field_definitions WHERE lower(name)=ANY(%s))",
                (list(RESERVED_CORE_BOOK_FIELDS), list(RESERVED_CORE_BOOK_FIELDS)),
            )
            cursor.execute("DELETE FROM custom_field_definitions WHERE lower(name)=ANY(%s)", (list(RESERVED_CORE_BOOK_FIELDS),))
            cursor.execute("DELETE FROM legacy_custom_field_definitions WHERE lower(name)=ANY(%s)", (list(RESERVED_CORE_BOOK_FIELDS),))
        return True
