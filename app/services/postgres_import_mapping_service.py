"""PostgreSQL implementation of saved CSV import mapping templates."""

from __future__ import annotations

import dataclasses
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from ..domain.models import ImportMappingTemplate
from ..infrastructure.postgres_store import PostgresStore, get_postgres_store


def _json_value(value: Any) -> Any:
    """Convert nested template values to JSON-safe values without dropping fields."""
    if dataclasses.is_dataclass(value):
        return {key: _json_value(item) for key, item in dataclasses.asdict(value).items()}
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "to_dict"):
        return _json_value(value.to_dict())
    return str(value)


class PostgresImportMappingService:
    """Persist and detect import templates using normalized PostgreSQL rows."""

    def __init__(self, store: PostgresStore | None = None):
        self.store = store or get_postgres_store()

    @staticmethod
    def _row_to_template(row: dict[str, Any] | None) -> ImportMappingTemplate | None:
        if not row:
            return None
        # The shared pool returns positional rows by default, matching the
        # established PostgreSQL services. Accept mappings for test adapters.
        if not isinstance(row, dict):
            row = dict(zip((
                "id", "user_id", "name", "description", "source_type", "sample_headers",
                "field_mappings", "times_used", "last_used", "created_at", "updated_at", "extra",
            ), row))
        return ImportMappingTemplate(
            id=str(row["id"]),
            user_id=str(row.get("user_id") or ""),
            name=str(row.get("name") or ""),
            description=row.get("description"),
            source_type=str(row.get("source_type") or ""),
            sample_headers=row.get("sample_headers") or [],
            field_mappings=row.get("field_mappings") or {},
            times_used=int(row.get("times_used") or 0),
            last_used=row.get("last_used"),
            created_at=row.get("created_at") or datetime.now(timezone.utc),
            updated_at=row.get("updated_at") or datetime.now(timezone.utc),
        )

    @staticmethod
    def _payload(template: ImportMappingTemplate) -> tuple[Any, ...]:
        return (
            template.id,
            template.user_id,
            template.name,
            template.description,
            template.source_type,
            json.dumps(_json_value(template.sample_headers)),
            json.dumps(_json_value(template.field_mappings)),
            max(0, int(template.times_used or 0)),
            template.last_used,
            template.created_at,
            template.updated_at,
        )

    def get_template_by_id_sync(self, template_id: str) -> ImportMappingTemplate | None:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT * FROM import_mapping_templates WHERE id = %s", (template_id,))
            return self._row_to_template(cursor.fetchone())

    def create_template_sync(self, template: ImportMappingTemplate) -> ImportMappingTemplate | None:
        now = datetime.now(timezone.utc)
        template.id = str(template.id or uuid.uuid4())
        template.created_at = template.created_at or now
        template.updated_at = now
        values = self._payload(template)
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT * FROM import_mapping_templates WHERE id = %s FOR UPDATE", (template.id,))
            existing = cursor.fetchone()
            if existing:
                return self._row_to_template(existing)
            cursor.execute(
                """INSERT INTO import_mapping_templates
                (id,user_id,name,description,source_type,sample_headers,field_mappings,times_used,last_used,created_at,updated_at)
                VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s) RETURNING *""",
                values,
            )
            return self._row_to_template(cursor.fetchone())

    def update_template_sync(self, template: ImportMappingTemplate) -> ImportMappingTemplate | None:
        if not template.id:
            return None
        template.updated_at = datetime.now(timezone.utc)
        values = self._payload(template)
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute(
                """UPDATE import_mapping_templates SET user_id=%s,name=%s,description=%s,source_type=%s,
                sample_headers=%s::jsonb,field_mappings=%s::jsonb,times_used=%s,last_used=%s,updated_at=%s
                WHERE id=%s RETURNING *""",
                (values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8], values[10], values[0]),
            )
            return self._row_to_template(cursor.fetchone())

    def get_user_templates_sync(self, user_id: str) -> list[ImportMappingTemplate]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """SELECT * FROM import_mapping_templates
                WHERE user_id = %s OR user_id = '__system__' ORDER BY created_at DESC, id""",
                (user_id,),
            )
            return [item for row in cursor.fetchall() if (item := self._row_to_template(row)) is not None]

    @staticmethod
    def _calculate_header_match_score(headers1: list[str], headers2: list[str]) -> float:
        if not headers1 or not headers2:
            return 0.0
        left = [str(header).lower().strip() for header in headers1]
        right = {str(header).lower().strip() for header in headers2}
        return sum(header in right for header in left) / max(len(left), len(right))

    def detect_template_sync(self, headers: list[str], user_id: str) -> ImportMappingTemplate | None:
        templates = self.get_user_templates_sync(user_id)
        if not templates:
            return None
        best = max(templates, key=lambda template: self._calculate_header_match_score(headers, template.sample_headers))
        return best if self._calculate_header_match_score(headers, best.sample_headers) > 0.5 else None

    def delete_template_sync(self, template_id: str, user_id: str) -> bool:
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM import_mapping_templates WHERE id = %s AND user_id = %s RETURNING id",
                (template_id, user_id),
            )
            return cursor.fetchone() is not None
