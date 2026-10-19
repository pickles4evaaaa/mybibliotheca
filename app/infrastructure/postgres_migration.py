"""Lossless, restartable Kuzu-to-PostgreSQL migration and cutover.

The source Kuzu database is never deleted or mutated. Migration writes an
immutable JSONB graph archive and normalized application tables in PostgreSQL,
then verifies counts and content hashes before the caller can activate the
PostgreSQL service layer. Unknown graph properties and relationship types stay
in the archive, and the untouched Kuzu files remain available for recovery.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
import base64
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Iterable

logger = logging.getLogger(__name__)

# Application-level paths are routed through the PostgreSQL facade, including
# imports, OPDS, Audiobookshelf, locations, and reading activity. Keep this
# explicit extension point for any newly discovered runtime incompatibility;
# source-specific schema/edge gaps are reported by the data audit below.
APPLICATION_CUTOVER_BLOCKERS: tuple[str, ...] = ()


# Legacy schema inventories retained for compatibility with migration tools.
# New snapshots use discover_kuzu_schema() so optional tables are not omitted.
NODE_LABELS = (
    "User",
    "Book",
    "Author",
    "Person",
    "Publisher",
    "Category",
    "Series",
    "ReadingLog",
    "Location",
    "ImportJob",
    "ImportMapping",
    "CustomFieldDefinition",
    "ImportTask",
    "CustomField",
)

RELATIONSHIPS = {
    "OWNS": ("User", "Book"),
    "WRITTEN_BY": ("Book", "Person"),
    "AUTHORED": ("Person", "Book"),
    "NARRATED": ("Person", "Book"),
    "EDITED": ("Person", "Book"),
    "TRANSLATED": ("Person", "Book"),
    "ILLUSTRATED": ("Person", "Book"),
    "PUBLISHED_BY": ("Book", "Publisher"),
    "CATEGORIZED_AS": ("Book", "Category"),
    "PART_OF_SERIES": ("Book", "Series"),
    "LOGGED": ("User", "ReadingLog"),
    "FOR_BOOK": ("ReadingLog", "Book"),
    "PARENT_CATEGORY": ("Category", "Category"),
    "STORED_AT": ("Book", "Location"),
    "HAS_CUSTOM_FIELD": ("User", "CustomField"),
}


DDL = (
    """
    CREATE TABLE IF NOT EXISTS bibliotheca_migration_runs (
        run_id TEXT PRIMARY KEY,
        source_kind TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('copying', 'verified')),
        manifest JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        verified_at TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bibliotheca_migrated_nodes (
        run_id TEXT NOT NULL REFERENCES bibliotheca_migration_runs(run_id),
        label TEXT NOT NULL,
        node_id TEXT NOT NULL,
        properties JSONB NOT NULL,
        content_sha256 TEXT NOT NULL,
        PRIMARY KEY (run_id, label, node_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bibliotheca_migrated_edges (
        run_id TEXT NOT NULL REFERENCES bibliotheca_migration_runs(run_id),
        edge_id BIGINT GENERATED ALWAYS AS IDENTITY,
        relationship TEXT NOT NULL,
        source_label TEXT NOT NULL,
        source_id TEXT NOT NULL,
        target_label TEXT NOT NULL,
        target_id TEXT NOT NULL,
        properties JSONB NOT NULL,
        content_sha256 TEXT NOT NULL,
        PRIMARY KEY (run_id, edge_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS app_graph_nodes (
        label TEXT NOT NULL,
        node_id TEXT NOT NULL,
        properties JSONB NOT NULL,
        content_sha256 TEXT NOT NULL,
        snapshot_run_id TEXT NOT NULL,
        PRIMARY KEY (label, node_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS app_graph_nodes_id_idx ON app_graph_nodes (node_id)",
    "CREATE INDEX IF NOT EXISTS app_graph_nodes_properties_gin_idx ON app_graph_nodes USING GIN (properties)",
    """
    CREATE TABLE IF NOT EXISTS app_graph_edges (
        edge_id BIGINT PRIMARY KEY,
        relationship TEXT NOT NULL,
        source_label TEXT NOT NULL,
        source_id TEXT NOT NULL,
        target_label TEXT NOT NULL,
        target_id TEXT NOT NULL,
        properties JSONB NOT NULL,
        content_sha256 TEXT NOT NULL,
        snapshot_run_id TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS app_graph_edges_source_idx ON app_graph_edges (source_label, source_id, relationship)",
    "CREATE INDEX IF NOT EXISTS app_graph_edges_target_idx ON app_graph_edges (target_label, target_id, relationship)",
    "CREATE INDEX IF NOT EXISTS app_graph_edges_properties_gin_idx ON app_graph_edges USING GIN (properties)",
    """
    CREATE TABLE IF NOT EXISTS bibliotheca_graph_imports (
        run_id TEXT PRIMARY KEY REFERENCES bibliotheca_migration_runs(run_id),
        status TEXT NOT NULL CHECK (status IN ('copying', 'verified')),
        manifest JSONB NOT NULL DEFAULT '{}'::jsonb,
        imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        verified_at TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bibliotheca_relational_imports (
        run_id TEXT NOT NULL REFERENCES bibliotheca_migration_runs(run_id),
        target_table TEXT NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('copying', 'verified')),
        manifest JSONB NOT NULL DEFAULT '{}'::jsonb,
        imported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        verified_at TIMESTAMPTZ,
        PRIMARY KEY (run_id, target_table)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bibliotheca_relational_imported_entities (
        run_id TEXT NOT NULL,
        entity_type TEXT NOT NULL,
        entity_id TEXT NOT NULL,
        source_sha256 TEXT NOT NULL,
        target_sha256 TEXT,
        PRIMARY KEY (run_id, entity_type, entity_id),
        FOREIGN KEY (run_id, entity_type) REFERENCES bibliotheca_relational_imports(run_id, target_table)
    )
    """,
    "ALTER TABLE bibliotheca_relational_imported_entities ADD COLUMN IF NOT EXISTS target_sha256 TEXT",
)


class MigrationError(RuntimeError):
    """Raised when the Kuzu snapshot cannot be copied and verified."""


class MigrationNotReady(MigrationError):
    """Raised when the lossless copy works but the app is not safe to cut over."""

    def __init__(self, audit: dict[str, Any]):
        self.audit = audit
        blockers = list(audit.get("cutover_blockers") or [])
        blockers.extend(audit.get("blockers") or [])
        blockers.extend(audit.get("preservation_blockers") or [])
        super().__init__("PostgreSQL cutover is not ready: " + "; ".join(map(str, blockers)))


class _SourceQuiescenceLease:
    """Unforgeable-by-accident capability for one live manager snapshot."""

    def __init__(self, manager: Any, kuzu_connection: Any):
        self._manager = manager
        self._connection = kuzu_connection
        self._active = True

    def validate(self, kuzu_connection: Any) -> None:
        if not self._active or kuzu_connection is not self._connection:
            raise MigrationError("Kuzu migration requires the active source-quiescence lease")
        lock = getattr(self._manager, "_lock", None)
        if lock is None:
            raise MigrationError("Kuzu migration lease has no live manager lock")
        with lock:
            if (
                not getattr(self._manager, "_writes_quiesced", False)
                or getattr(self._manager, "_pending_quiesce_reason", None)
                != "postgres_migration_snapshot"
            ):
                raise MigrationError("Kuzu source is no longer quiesced for PostgreSQL migration")

    def close(self) -> None:
        self._active = False


_USER_IMPORT_COLUMNS = (
    "id", "username", "email", "password_hash", "share_current_reading",
    "share_reading_activity", "share_library", "is_admin", "is_active",
    "password_must_change", "failed_login_attempts", "locked_until",
    "last_login", "password_changed_at", "reading_streak_offset", "timezone",
    "display_name", "bio", "location", "website", "created_at", "updated_at",
)
_BOOK_IMPORT_COLUMNS = (
    "id", "title", "normalized_title", "subtitle", "isbn13", "isbn10", "asin",
    "description", "published_date", "page_count", "language", "cover_url",
    "google_books_id", "openlibrary_id", "opds_source_id", "opds_source_updated_at",
    "opds_source_entry_hash", "average_rating", "rating_count", "series",
    "series_volume", "series_order", "custom_metadata", "raw_categories",
    "audio_length_minutes", "audio_is_abridged", "audio_narrators", "media_format",
    "audiobookshelf_id", "audio_duration_ms", "media_type", "audiobookshelf_updated_at",
    "quantity", "created_at", "updated_at",
)
_BOOK_JSON_COLUMNS = ("custom_metadata", "raw_categories", "audio_narrators")
_USER_BOOK_COLUMNS = (
    "user_id", "book_id", "reading_status", "date_added", "start_date", "finish_date",
    "current_page", "total_pages", "ownership_status", "media_type", "borrowed_from",
    "borrowed_from_user_id", "borrowed_date", "borrowed_due_date", "loaned_to",
    "loaned_to_user_id", "loaned_date", "loaned_due_date", "location_id", "user_rating",
    "rating_date", "user_review", "review_date", "is_review_spoiler", "personal_notes",
    "pace", "character_driven", "source", "custom_metadata", "created_at", "updated_at", "extra",
)
_USER_BOOK_JSON_COLUMNS = ("custom_metadata", "extra")
_CUSTOM_FIELD_PROJECTION_COLUMNS = (
    "id", "user_id", "definition_id", "book_id", "field_name", "field_value",
    "is_required", "is_active", "created_at", "updated_at", "extra",
)

# Independent reference data is safe to project before the relationship-bearing
# tables. Keep this list in sync with the normalized DDL in postgres_store.py.
# Unknown Kuzu properties are copied into ``extra`` as well as retained in the
# immutable graph archive.
_REFERENCE_ENTITY_IMPORTS = {
    "Person": {
        "table": "people",
        "columns": (
            "name", "normalized_name", "birth_date", "death_date", "birth_year",
            "death_year", "birth_place", "bio", "website", "openlibrary_id",
            "image_url", "wikidata_id", "imdb_id", "alternate_names", "fuller_name",
            "title", "official_links", "created_at", "updated_at",
        ),
        "json": ("alternate_names", "official_links"),
        "defaults": {"name": "", "alternate_names": [], "official_links": []},
    },
    "Author": {
        "table": "authors",
        "columns": ("name", "normalized_name", "birth_year", "death_year", "bio", "created_at", "updated_at"),
        "json": (),
        "defaults": {"name": ""},
    },
    "Publisher": {
        "table": "publishers",
        "columns": ("name", "normalized_name", "founded_year", "country", "created_at", "updated_at"),
        "json": (),
        "defaults": {"name": ""},
    },
    "Series": {
        "table": "series",
        "columns": ("name", "normalized_name", "description", "user_cover", "cover_url", "custom_cover", "generated_placeholder", "created_at"),
        "json": (),
        "defaults": {"name": ""},
    },
    "Library": {
        "table": "libraries",
        "columns": ("name", "description", "is_default", "created_at", "updated_at"),
        "json": (),
        "defaults": {"name": ""},
    },
    "LibraryShelf": {
        "table": "library_shelves",
        "columns": ("name", "description", "position", "created_at", "updated_at"),
        "json": (),
        "defaults": {"name": ""},
    },
    "ReadingSession": {
        "table": "reading_sessions",
        "columns": ("user_id", "book_id", "start_time", "end_time", "pages_read", "notes", "reading_speed", "created_at", "updated_at"),
        "json": (),
        "defaults": {},
        "requires": ("users", "books"),
    },
    "ReadingGoal": {
        "table": "reading_goals",
        "columns": ("user_id", "title", "description", "target_books", "target_pages", "target_reading_time_minutes", "deadline", "is_completed", "created_at", "updated_at"),
        "json": (),
        "defaults": {},
        "requires": ("users",),
    },
    "Review": {
        "table": "reviews",
        "columns": ("user_id", "book_id", "rating", "review_text", "created_at", "updated_at"),
        "json": (),
        "defaults": {},
        "requires": ("users", "books"),
    },
    "Category": {
        "table": "categories",
        "columns": ("name", "normalized_name", "parent_id", "description", "level", "color", "icon", "aliases", "book_count", "user_book_count", "created_at", "updated_at"),
        "json": ("aliases",),
        "defaults": {"name": "", "level": 0, "aliases": [], "book_count": 0, "user_book_count": 0},
    },
    "Location": {
        "table": "locations",
        "columns": ("owner_user_id", "name", "description", "location_type", "address", "is_default", "is_active", "created_at", "updated_at"),
        "json": (),
        "defaults": {"name": ""},
        "requires": ("users",),
        "aliases": {"owner_user_id": "user_id"},
    },
    "CustomFieldDefinition": {
        "table": "legacy_custom_field_definitions",
        "columns": ("name", "display_name", "field_type", "description", "created_by_user_id", "is_shareable", "is_global", "default_value", "placeholder_text", "help_text", "predefined_options", "allow_custom_options", "rating_min", "rating_max", "rating_labels", "usage_count", "created_at", "updated_at"),
        "json": ("predefined_options", "rating_labels"),
        "defaults": {"name": "", "predefined_options": [], "rating_labels": {}},
    },
    "CustomField": {
        # The current Kuzu service stores field *definitions* in CustomField
        # nodes. Per-book/per-user values live in metadata JSON blobs and
        # HAS_*_METADATA relationships, so treating these nodes as values
        # silently discarded their definition fields from the PG model.
        "table": "custom_field_definitions",
        "columns": ("name", "display_name", "field_type", "description", "created_by_user_id", "is_global", "created_at", "updated_at"),
        "json": (),
        "defaults": {"name": "", "field_type": "text", "is_global": False},
    },
    "UserCustomField": {
        "table": "user_custom_fields",
        "columns": ("field_name", "field_type", "is_required", "default_value", "created_at", "updated_at"),
        "json": (),
        "defaults": {},
    },
    "ImportMapping": {
        "table": "import_mappings",
        "columns": ("name", "description", "user_id", "is_system", "source_type", "field_mappings", "sample_headers", "is_shareable", "usage_count", "created_at", "updated_at"),
        "json": ("field_mappings", "sample_headers"),
        "defaults": {"field_mappings": {}, "sample_headers": []},
    },
    # Current Kuzu releases use ImportMappingTemplate, while older data may
    # also contain ImportMapping nodes. Keep both generations losslessly.
    "ImportMappingTemplate": {
        "table": "import_mapping_templates",
        "columns": ("user_id", "name", "description", "source_type", "sample_headers", "field_mappings", "times_used", "last_used", "created_at", "updated_at"),
        "json": ("sample_headers", "field_mappings"),
        "defaults": {"user_id": "", "name": "", "source_type": "", "sample_headers": [], "field_mappings": {}, "times_used": 0},
    },
    "ImportJob": {
        "table": "import_jobs",
        "columns": ("task_id", "user_id", "csv_file_path", "field_mappings", "default_reading_status", "duplicate_handling", "custom_fields_enabled", "status", "processed", "success", "errors", "total", "start_time", "end_time", "current_book", "error_messages", "recent_activity", "created_at", "updated_at", "expires_at"),
        "json": ("field_mappings", "error_messages", "recent_activity"),
        "defaults": {"field_mappings": {}, "error_messages": [], "recent_activity": []},
    },
    "ImportTask": {
        "table": "import_tasks",
        "columns": ("user_id", "task_type", "status", "progress", "total_items", "processed_items", "file_path", "parameters", "results", "error_message", "created_at", "started_at", "completed_at"),
        "json": ("parameters", "results"),
        "defaults": {"parameters": {}, "results": {}},
    },
    "GlobalMetadata": {
        "table": "global_metadata_records",
        "key": "book_id",
        "columns": ("book_id", "created_at", "updated_at"),
        "json": (),
        "defaults": {},
        "requires": ("books",),
    },
}


def _normalize_user_projection(node_id: str, properties: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(properties, dict):
        raise MigrationError(f"Kuzu User {node_id} properties are not a map")
    if str(properties.get("id", "")) != str(node_id):
        raise MigrationError(f"Kuzu User node id mismatch for {node_id}")
    for required in ("username", "email", "password_hash"):
        if properties.get(required) is None or str(properties.get(required)) == "":
            raise MigrationError(f"Kuzu User {node_id} is missing required field {required}")
    values = dict(properties)
    values["id"] = str(node_id)
    defaults = (
        ("share_current_reading", True), ("share_reading_activity", True),
        ("share_library", False), ("is_admin", False), ("is_active", True),
        ("password_must_change", False), ("failed_login_attempts", 0),
        ("reading_streak_offset", 0), ("timezone", "UTC"),
    )
    for name, default in defaults:
        if values.get(name) is None:
            values[name] = default
    known = set(_USER_IMPORT_COLUMNS)
    extra = {key: value for key, value in properties.items() if key not in known}
    return values, extra


def _normalize_book_projection(node_id: str, properties: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(properties, dict):
        raise MigrationError(f"Kuzu Book {node_id} properties are not a map")
    if str(properties.get("id", "")) != str(node_id):
        raise MigrationError(f"Kuzu Book node id mismatch for {node_id}")
    values = dict(properties)
    values["id"] = str(node_id)
    values["title"] = values.get("title") or ""
    json_defaults = {"custom_metadata": {}, "raw_categories": [], "audio_narrators": []}
    for name, default in json_defaults.items():
        raw = values.get(name)
        if raw is None:
            values[name] = default
        elif isinstance(raw, str):
            try:
                values[name] = json.loads(raw)
            except (TypeError, ValueError):
                # Preserve malformed legacy JSON as a string value; the exact
                # source encoding remains available in the typed graph archive.
                values[name] = raw
    known = set(_BOOK_IMPORT_COLUMNS)
    extra = {key: value for key, value in properties.items() if key not in known}
    return values, extra


def _parse_json_map(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return {"_legacy_raw_json": value}
    return value if isinstance(value, dict) else {}


def _parse_timestamp(value: Any) -> Any:
    if isinstance(value, date) and not isinstance(value, datetime):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
    return value


def _merge_user_book_edges(
    user_id: str,
    book_id: str,
    edges: list[tuple[int, str, dict[str, Any], str]],
) -> tuple[dict[str, Any], str, str]:
    """Resolve legacy ownership and universal-library overlays into one row.

    OWNS is applied first; READS and then HAS_PERSONAL_METADATA override the
    corresponding fields. Every original edge and hash is retained in `extra`
    and in the immutable graph snapshot, so projection precedence cannot erase
    source data.
    """
    values: dict[str, Any] = {"user_id": str(user_id), "book_id": str(book_id)}
    custom_metadata: dict[str, Any] = {}
    source_relationships: list[dict[str, Any]] = []
    source_hashes: list[str] = []
    priority = {"OWNS": 0, "READS": 1, "HAS_PERSONAL_METADATA": 2}
    ordered_edges = sorted(edges, key=lambda item: (priority.get(item[1], 3), item[0]))
    for edge_id, relationship, properties, source_hash in ordered_edges:
        source_hashes.append(source_hash)
        source_relationships.append({
            "edge_id": edge_id,
            "relationship": relationship,
            "properties": json.loads(_canonical_json(properties)),
            "source_sha256": source_hash,
        })
        if relationship == "OWNS":
            direct_fields = (
                "reading_status", "date_added", "start_date", "finish_date", "current_page",
                "total_pages", "ownership_status", "media_type", "borrowed_from",
                "borrowed_from_user_id", "borrowed_date", "borrowed_due_date", "loaned_to",
                "loaned_to_user_id", "loaned_date", "loaned_due_date", "location_id", "user_rating",
                "rating_date", "user_review", "review_date", "is_review_spoiler", "personal_notes",
                "pace", "character_driven", "source", "created_at", "updated_at",
            )
            for name in direct_fields:
                if properties.get(name) is not None:
                    values[name] = properties[name]
            custom_metadata.update(_parse_json_map(properties.get("custom_metadata")))
        elif relationship == "READS":
            aliases = {
                "status": "reading_status", "started_at": "start_date", "finished_at": "finish_date",
                "current_page": "current_page",
            }
            for source_name, target_name in aliases.items():
                if properties.get(source_name) is not None:
                    values[target_name] = properties[source_name]
            if properties.get("progress_percentage") is not None:
                custom_metadata["progress_percentage"] = properties["progress_percentage"]
        elif relationship == "HAS_PERSONAL_METADATA":
            blob = _parse_json_map(properties.get("personal_custom_fields"))
            for name in ("personal_notes", "start_date", "finish_date"):
                if properties.get(name) is not None:
                    values[name] = properties[name]
                elif blob.get(name) is not None:
                    values[name] = blob[name]
            if blob.get("user_review") is not None:
                values["user_review"] = blob["user_review"]
            column_fields = set(_USER_BOOK_COLUMNS) | {"user_id", "book_id", "personal_custom_fields"}
            for name, value in blob.items():
                if name == "custom_metadata":
                    custom_metadata.update(_parse_json_map(value))
                elif name in _USER_BOOK_COLUMNS and name != "extra":
                    if value is not None:
                        values[name] = value
                elif name not in column_fields:
                    custom_metadata[name] = value
            # Also retain fields from newer relationship schemas that are not
            # included in the standard personal metadata JSON blob.
            for name in ("created_at", "updated_at"):
                if properties.get(name) is not None:
                    values[name] = properties[name]
        else:
            # Unknown User -> Book edges still create a library pair while
            # their original values remain recoverable from source_relationships.
            continue

    for name in ("start_date", "finish_date", "date_added", "borrowed_date", "borrowed_due_date", "loaned_date", "loaned_due_date", "rating_date", "review_date", "created_at", "updated_at"):
        if name in values:
            values[name] = _parse_timestamp(values[name])
    if values.get("user_rating") is not None:
        values["user_rating"] = float(values["user_rating"])
    values["custom_metadata"] = custom_metadata
    pair_key = hashlib.sha256(f"{user_id}\0{book_id}".encode("utf-8")).hexdigest()
    values["extra"] = {"_migration_pair_key": pair_key, "_source_relationships": source_relationships}
    source_digest = _aggregate_hash(source_hashes)
    return values, pair_key, source_digest


def _jsonb_projection(value: Any) -> str:
    """Serialize an app-facing JSONB value; the typed source stays archived."""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=_json_default)


_WIRE_MARKER = "$mybibliotheca_kuzu_value_v1"


def _wire_value(value: Any) -> list[Any]:
    """Encode Python/Kuzu values as an unambiguous JSON-only typed tree.

    Every value is tagged, including maps and arrays. That avoids collisions
    between a user map that resembles a type marker and a real binary/date
    value, and preserves arbitrary bytes without lossy text decoding.
    """
    if value is None:
        return [_WIRE_MARKER, "none"]
    if isinstance(value, bool):
        return [_WIRE_MARKER, "bool", value]
    if isinstance(value, str):
        return [_WIRE_MARKER, "str", value]
    if isinstance(value, int):
        return [_WIRE_MARKER, "int", str(value)]
    if isinstance(value, float):
        return [_WIRE_MARKER, "float", repr(value)]
    if isinstance(value, bytes):
        return [_WIRE_MARKER, "bytes", base64.b64encode(value).decode("ascii")]
    if isinstance(value, datetime):
        return [_WIRE_MARKER, "datetime", value.isoformat()]
    if isinstance(value, date):
        return [_WIRE_MARKER, "date", value.isoformat()]
    if isinstance(value, Decimal):
        return [_WIRE_MARKER, "decimal", str(value)]
    if isinstance(value, uuid.UUID):
        return [_WIRE_MARKER, "uuid", str(value)]
    if isinstance(value, dict):
        pairs = [(_wire_value(key), _wire_value(item)) for key, item in value.items()]
        pairs.sort(key=lambda pair: json.dumps(pair[0], ensure_ascii=False, separators=(",", ":")))
        return [_WIRE_MARKER, "map", [[key, item] for key, item in pairs]]
    if isinstance(value, list):
        return [_WIRE_MARKER, "list", [_wire_value(item) for item in value]]
    if isinstance(value, tuple):
        return [_WIRE_MARKER, "tuple", [_wire_value(item) for item in value]]
    raise TypeError(f"Unsupported migration value: {type(value).__name__}")


def decode_migration_value(value: Any) -> Any:
    """Decode a value returned from the PostgreSQL JSONB archive."""
    if not isinstance(value, list) or len(value) < 2 or value[0] != _WIRE_MARKER:
        raise MigrationError("Invalid typed value in PostgreSQL migration archive")
    kind = value[1]
    payload = value[2] if len(value) > 2 else None
    if kind == "none":
        return None
    if kind == "bool":
        return bool(payload)
    if kind == "str":
        return str(payload)
    if kind == "int":
        return int(payload)
    if kind == "float":
        return float(payload)
    if kind == "bytes":
        return base64.b64decode(payload, validate=True)
    if kind == "datetime":
        return datetime.fromisoformat(payload)
    if kind == "date":
        return date.fromisoformat(payload)
    if kind == "decimal":
        return Decimal(payload)
    if kind == "uuid":
        return uuid.UUID(payload)
    if kind == "map":
        return {decode_migration_value(key): decode_migration_value(item) for key, item in payload}
    if kind == "list":
        return [decode_migration_value(item) for item in payload]
    if kind == "tuple":
        return tuple(decode_migration_value(item) for item in payload)
    raise MigrationError(f"Unknown typed value {kind!r} in PostgreSQL migration archive")


def _stored_json(value: Any) -> str:
    """Canonicalize already typed JSONB values without encoding them twice."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _canonical_manifest_json(value: Any) -> str:
    """Serialize migration bookkeeping as ordinary JSON, not typed graph data."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _canonical_json(value: Any) -> str:
    return _stored_json(_wire_value(value))


def _json_default(value: Any) -> Any:
    # Kept for backwards-compatible imports by downstream tests/tools.
    return _wire_value(value)


def _content_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _stored_content_hash(value: Any) -> str:
    return hashlib.sha256(_stored_json(value).encode("utf-8")).hexdigest()


def _postgres_value(value: Any) -> Any:
    """Normalize values to the representation returned by PostgreSQL types."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, dict):
        return {key: _postgres_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_postgres_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_postgres_value(item) for item in value)
    return value


def _result_rows(result: Any) -> Iterable[tuple[Any, ...]]:
    """Yield Kuzu rows for both QueryResult and list[QueryResult] APIs."""
    if isinstance(result, list):
        for part in result:
            yield from _result_rows(part)
        return
    if result is None:
        return
    if hasattr(result, "has_next") and hasattr(result, "get_next"):
        while result.has_next():
            row = result.get_next()
            yield tuple(row) if isinstance(row, (tuple, list)) else (row,)
        return
    raise MigrationError(f"Unexpected Kuzu query result type: {type(result).__name__}")


_SAFE_TABLE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def discover_kuzu_schema(kuzu_connection: Any) -> tuple[tuple[str, ...], dict[str, tuple[str, str]]]:
    """Read every user node and relationship table from Kuzu's catalog.

    A hard-coded allowlist silently loses data when older releases or optional
    services add graph tables. Discover tables at runtime and fail closed on
    identifiers or table types that this archive format cannot represent.
    """
    tables: list[tuple[str, str]] = []
    for row in _result_rows(kuzu_connection.execute("CALL show_tables() RETURN *")):
        if len(row) < 3:
            raise MigrationError("Kuzu show_tables() returned an unexpected row shape")
        name, kind = str(row[1]), str(row[2]).upper()
        if not _SAFE_TABLE_NAME.fullmatch(name):
            raise MigrationError(f"Cannot safely migrate Kuzu table name {name!r}")
        if kind in ("NODE", "REL"):
            tables.append((name, kind))
        else:
            raise MigrationError(f"Unsupported Kuzu table kind {kind!r} for {name}")

    if not tables:
        raise MigrationError("Kuzu catalog returned no user tables; refusing an empty migration")

    nodes = tuple(sorted(name for name, kind in tables if kind == "NODE"))
    # Fail early for node tables whose primary key cannot be represented by
    # the archive's stable string node identifier.
    for label in nodes:
        _node_primary_key_column(kuzu_connection, label)
    relationships: dict[str, tuple[str, str]] = {}
    for name, kind in sorted(tables):
        if kind != "REL":
            continue
        rows = list(_result_rows(kuzu_connection.execute(f"CALL show_connection('{name}') RETURN *")))
        endpoints = {(str(item[0]), str(item[1])) for item in rows if len(item) >= 2}
        if not endpoints:
            raise MigrationError(f"Kuzu catalog did not provide endpoints for relationship {name}")
        if any(not _SAFE_TABLE_NAME.fullmatch(label) for pair in endpoints for label in pair):
            raise MigrationError(f"Kuzu returned an unsafe endpoint label for relationship {name}")
        if len(endpoints) != 1:
            raise MigrationError(f"Relationship {name} has unsupported endpoint pairs: {sorted(endpoints)}")
        relationships[name] = next(iter(endpoints))
    return nodes, relationships


def _node_primary_key_column(kuzu_connection: Any, label: str) -> str:
    """Discover the sole declared Kuzu primary-key property for a node table."""
    if not _SAFE_TABLE_NAME.fullmatch(label):
        raise MigrationError(f"Cannot safely inspect Kuzu node table {label!r}")
    columns = list(_result_rows(kuzu_connection.execute(f"CALL table_info('{label}') RETURN *")))
    primary_keys = [str(row[1]) for row in columns if len(row) >= 5 and bool(row[4])]
    if len(primary_keys) != 1 or not _SAFE_TABLE_NAME.fullmatch(primary_keys[0]):
        raise MigrationError(f"Kuzu node table {label} must expose exactly one safe primary-key property")
    return primary_keys[0]


def _node_properties(node: Any, expected_label: str, primary_key: str = "id") -> tuple[str, dict[str, Any]]:
    if not isinstance(node, dict):
        raise MigrationError(f"Kuzu returned an unexpected node value for {expected_label}: {type(node).__name__}")
    label = str(node.get("_label") or expected_label)
    if label != expected_label:
        raise MigrationError(f"Expected node label {expected_label}, received {label}")
    node_id = node.get(primary_key)
    if node_id is None or str(node_id) == "":
        if primary_key == "id":
            raise MigrationError(f"Kuzu {expected_label} node is missing its id")
        raise MigrationError(f"Kuzu {expected_label} node is missing its primary key {primary_key}")
    # Preserve application properties while storing Kuzu's internal row id in
    # separate columns; internal IDs are database-local and not portable.
    properties = {str(k): v for k, v in node.items() if k not in ("_id", "_label")}
    return str(node_id), properties


def _edge_properties(edge: Any) -> dict[str, Any]:
    if not isinstance(edge, dict):
        raise MigrationError(f"Kuzu returned an unexpected relationship value: {type(edge).__name__}")
    internal_keys = {"_id", "_label", "_src", "_dst", "_from", "_to"}
    return {str(k): v for k, v in edge.items() if str(k) not in internal_keys}


def _edge_hash(
    relationship: str,
    source_label: str,
    source_id: str,
    target_label: str,
    target_id: str,
    properties: dict[str, Any],
) -> str:
    # Store/hash properties through the typed encoder and include that
    # canonical representation in the outer digest, so PostgreSQL JSONB
    # verification can reproduce it exactly.
    return _content_hash([
        relationship, source_label, source_id, target_label, target_id,
        _canonical_json(properties),
    ])


def _stored_edge_hash(
    relationship: str,
    source_label: str,
    source_id: str,
    target_label: str,
    target_id: str,
    properties: Any,
) -> str:
    return _content_hash([
        relationship, source_label, source_id, target_label, target_id,
        _stored_json(properties),
    ])


def _fetch_nodes(kuzu_connection: Any, label: str) -> list[tuple[str, dict[str, Any], str]]:
    rows: list[tuple[str, dict[str, Any], str]] = []
    primary_key = _node_primary_key_column(kuzu_connection, label)
    for row in _result_rows(kuzu_connection.execute(f"MATCH (n:{label}) RETURN n ORDER BY n.{primary_key}")):
        if len(row) != 1:
            raise MigrationError(f"Unexpected node row width in {label}: {len(row)}")
        node_id, properties = _node_properties(row[0], label, primary_key)
        rows.append((node_id, properties, _content_hash(properties)))
    rows.sort(key=lambda item: item[0])
    if len({item[0] for item in rows}) != len(rows):
        raise MigrationError(f"Duplicate primary key encountered in Kuzu node table {label}")
    return rows


def _fetch_edges(
    kuzu_connection: Any,
    relationship: str,
    source_label: str,
    target_label: str,
) -> list[tuple[str, str, dict[str, Any], str]]:
    rows: list[tuple[str, str, dict[str, Any], str]] = []
    source_key = _node_primary_key_column(kuzu_connection, source_label)
    target_key = _node_primary_key_column(kuzu_connection, target_label)
    query = (
        f"MATCH (a:{source_label})-[r:{relationship}]->(b:{target_label}) "
        f"RETURN a.{source_key}, b.{target_key}, r"
    )
    for row in _result_rows(kuzu_connection.execute(query)):
        if len(row) != 3:
            raise MigrationError(f"Unexpected relationship row width in {relationship}: {len(row)}")
        source_id, target_id, edge = row
        if source_id is None or target_id is None:
            raise MigrationError(f"Relationship {relationship} has an endpoint without an id")
        properties = _edge_properties(edge)
        source_id = str(source_id)
        target_id = str(target_id)
        rows.append((source_id, target_id, properties, _edge_hash(
            relationship, source_label, source_id, target_label, target_id, properties
        )))
    # Preserve parallel edges.  Their properties distinguish otherwise
    # identical endpoints; exact duplicate edges remain separate list items.
    rows.sort(key=lambda item: (item[0], item[1], item[3]))
    return rows


def _aggregate_hash(hashes: Iterable[str]) -> str:
    digest = hashlib.sha256()
    for item in hashes:
        digest.update(item.encode("ascii"))
        digest.update(b"\n")
    return digest.hexdigest()


class _DigestAccumulator:
    """Incremental equivalent of _aggregate_hash for large migrations."""

    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self.count = 0

    def add(self, row_hash: str) -> None:
        self._digest.update(row_hash.encode("ascii"))
        self._digest.update(b"\n")
        self.count += 1

    def hexdigest(self) -> str:
        return self._digest.hexdigest()


def _iter_nodes(kuzu_connection: Any, label: str) -> Iterable[tuple[str, dict[str, Any], str]]:
    """Stream source nodes in primary-key order to bound migration memory."""
    previous_id: str | None = None
    primary_key = _node_primary_key_column(kuzu_connection, label)
    for row in _result_rows(kuzu_connection.execute(f"MATCH (n:{label}) RETURN n ORDER BY n.{primary_key}")):
        if len(row) != 1:
            raise MigrationError(f"Unexpected node row width in {label}: {len(row)}")
        node_id, properties = _node_properties(row[0], label, primary_key)
        if previous_id is not None and node_id == previous_id:
            raise MigrationError(f"Duplicate primary key encountered in Kuzu node table {label}")
        previous_id = node_id
        yield node_id, properties, _content_hash(properties)


def _iter_edges(
    kuzu_connection: Any,
    relationship: str,
    source_label: str,
    target_label: str,
) -> Iterable[tuple[str, str, dict[str, Any], str]]:
    """Stream every relationship, including parallel and duplicate edges."""
    source_key = _node_primary_key_column(kuzu_connection, source_label)
    target_key = _node_primary_key_column(kuzu_connection, target_label)
    query = (
        f"MATCH (a:{source_label})-[r:{relationship}]->(b:{target_label}) "
        f"RETURN a.{source_key}, b.{target_key}, r"
    )
    for row in _result_rows(kuzu_connection.execute(query)):
        if len(row) != 3:
            raise MigrationError(f"Unexpected relationship row width in {relationship}: {len(row)}")
        source_id, target_id, edge = row
        if source_id is None or target_id is None:
            raise MigrationError(f"Relationship {relationship} has an endpoint without an id")
        properties = _edge_properties(edge)
        source_id = str(source_id)
        target_id = str(target_id)
        yield source_id, target_id, properties, _edge_hash(
            relationship, source_label, source_id, target_label, target_id, properties
        )


def migrate_kuzu_to_postgres(
    kuzu_connection: Any,
    postgres_connection: Any,
    *,
    quiescence_lease: _SourceQuiescenceLease | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Copy a complete Kuzu graph into PostgreSQL and verify the archive.

    The Kuzu database is read-only throughout.  The Postgres transaction is
    rolled back on any query, insert, count, or hash mismatch.  Existing
    migration runs are immutable; a new call creates a separate run.
    """
    if quiescence_lease is None:
        raise MigrationError("Pause application writes through the Kuzu manager before migration")
    quiescence_lease.validate(kuzu_connection)
    node_labels, relationships = discover_kuzu_schema(kuzu_connection)
    run_id = run_id or str(uuid.uuid4())
    manifest: dict[str, Any] = {
        "format_version": 2,
        "value_encoding": "typed-json-v1",
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "nodes": {},
        "relationships": {},
        "schema": {"node_labels": list(node_labels), "relationships": relationships},
    }

    try:
        # Psycopg connections default to transactional mode.  The `with`
        # context commits only if every write and verification succeeds.
        with postgres_connection.transaction():
            with postgres_connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('mybibliotheca:kuzu-to-postgres'))")
                for statement in DDL:
                    cursor.execute(statement)
                cursor.execute(
                    "INSERT INTO bibliotheca_migration_runs(run_id, source_kind, status, manifest) "
                    "VALUES (%s, 'kuzu', 'copying', %s::jsonb)",
                    (run_id, _canonical_manifest_json(manifest)),
                )

                for label in node_labels:
                    digest = _DigestAccumulator()
                    batch: list[tuple[Any, ...]] = []
                    for node_id, properties, content_hash in _iter_nodes(kuzu_connection, label):
                        digest.add(content_hash)
                        batch.append((run_id, label, node_id, _canonical_json(properties), content_hash))
                        if len(batch) >= 500:
                            cursor.executemany(
                                "INSERT INTO bibliotheca_migrated_nodes "
                                "(run_id, label, node_id, properties, content_sha256) "
                                "VALUES (%s, %s, %s, %s::jsonb, %s)",
                                batch,
                            )
                            batch.clear()
                    if batch:
                        cursor.executemany(
                            "INSERT INTO bibliotheca_migrated_nodes "
                            "(run_id, label, node_id, properties, content_sha256) "
                            "VALUES (%s, %s, %s, %s::jsonb, %s)",
                            batch,
                        )
                    manifest["nodes"][label] = {"count": digest.count, "sha256": digest.hexdigest()}

                for relationship, (source_label, target_label) in relationships.items():
                    digest = _DigestAccumulator()
                    batch = []
                    for source_id, target_id, properties, content_hash in _iter_edges(
                        kuzu_connection, relationship, source_label, target_label
                    ):
                        digest.add(content_hash)
                        batch.append(
                            (
                                run_id, relationship, source_label, source_id,
                                target_label, target_id, _canonical_json(properties), content_hash,
                            )
                        )
                        if len(batch) >= 500:
                            cursor.executemany(
                                "INSERT INTO bibliotheca_migrated_edges "
                                "(run_id, relationship, source_label, source_id, target_label, target_id, properties, content_sha256) "
                                "VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)",
                                batch,
                            )
                            batch.clear()
                    if batch:
                        cursor.executemany(
                            "INSERT INTO bibliotheca_migrated_edges "
                            "(run_id, relationship, source_label, source_id, target_label, target_id, properties, content_sha256) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)",
                            batch,
                        )
                    manifest["relationships"][relationship] = {
                        "count": digest.count,
                        "sha256": digest.hexdigest(),
                    }

                # Verify the persisted copy itself. Counts and ordered
                # property hashes detect truncation, accidental field loss,
                # and serialization drift before the archive is marked verified.
                for label in node_labels:
                    persisted_digest = _DigestAccumulator()
                    with postgres_connection.cursor(name=f"verify_nodes_{uuid.uuid4().hex}") as verify_cursor:
                        verify_cursor.execute(
                            "SELECT node_id, properties, content_sha256 FROM bibliotheca_migrated_nodes "
                            "WHERE run_id = %s AND label = %s ORDER BY node_id",
                            (run_id, label),
                        )
                        for node_id, properties, stored_hash in verify_cursor:
                            actual_hash = _stored_content_hash(properties)
                            if actual_hash != str(stored_hash):
                                raise MigrationError(f"PostgreSQL property verification failed for {label}:{node_id}")
                            persisted_digest.add(actual_hash)
                    expected = manifest["nodes"][label]
                    if persisted_digest.count != expected["count"] or persisted_digest.hexdigest() != expected["sha256"]:
                        raise MigrationError(f"PostgreSQL verification failed for node table {label}")

                for relationship in relationships:
                    persisted_digest = _DigestAccumulator()
                    with postgres_connection.cursor(name=f"verify_edges_{uuid.uuid4().hex}") as verify_cursor:
                        source_label, target_label = relationships[relationship]
                        verify_cursor.execute(
                            "SELECT source_id, target_id, properties, content_sha256 FROM bibliotheca_migrated_edges "
                            "WHERE run_id = %s AND relationship = %s ORDER BY edge_id",
                            (run_id, relationship),
                        )
                        for source_id, target_id, properties, stored_hash in verify_cursor:
                            actual_hash = _stored_edge_hash(
                                relationship, source_label, str(source_id), target_label, str(target_id), properties
                            )
                            if actual_hash != str(stored_hash):
                                raise MigrationError(f"PostgreSQL property verification failed for relationship {relationship}")
                            persisted_digest.add(actual_hash)
                    expected = manifest["relationships"][relationship]
                    if persisted_digest.count != expected["count"] or persisted_digest.hexdigest() != expected["sha256"]:
                        raise MigrationError(f"PostgreSQL verification failed for relationship {relationship}")

                cursor.execute(
                    "UPDATE bibliotheca_migration_runs SET status = 'verified', verified_at = now(), manifest = %s::jsonb "
                    "WHERE run_id = %s",
                    (_canonical_manifest_json(manifest), run_id),
                )
    except Exception as exc:
        if isinstance(exc, MigrationError):
            raise
        logger.exception("Kuzu to PostgreSQL migration failed; source Kuzu data was not modified")
        raise MigrationError("Kuzu to PostgreSQL migration failed; PostgreSQL transaction rolled back") from exc

    return manifest


def import_verified_graph_snapshot(postgres_connection: Any, run_id: str) -> dict[str, Any]:
    """Materialize a verified archive into indexed PostgreSQL graph tables.

    This preserves the full source graph, including optional tables, unknown
    properties, orphan endpoints, and parallel edges. The archive remains
    untouched. Importing this snapshot prepares PostgreSQL for service ports;
    it does not switch application traffic by itself.
    """
    try:
        with postgres_connection.transaction():
            with postgres_connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('mybibliotheca:graph-snapshot-import'))")
                cursor.execute(
                    "SELECT status, manifest FROM bibliotheca_migration_runs WHERE run_id = %s FOR SHARE",
                    (run_id,),
                )
                source_run = cursor.fetchone()
                if not source_run or source_run[0] != "verified":
                    raise MigrationError(f"Kuzu archive run {run_id} is missing or not verified")
                source_manifest = source_run[1]
                if isinstance(source_manifest, str):
                    source_manifest = json.loads(source_manifest)

                cursor.execute(
                    "SELECT DISTINCT snapshot_run_id FROM app_graph_nodes "
                    "UNION SELECT DISTINCT snapshot_run_id FROM app_graph_edges"
                )
                existing_runs = {row[0] for row in cursor.fetchall()}
                if existing_runs - {run_id}:
                    raise MigrationError(
                        "PostgreSQL graph tables already contain a different snapshot; "
                        "refusing to replace it automatically"
                    )

                cursor.execute(
                    "INSERT INTO bibliotheca_graph_imports(run_id, status, manifest) "
                    "VALUES (%s, 'copying', %s::jsonb) "
                    "ON CONFLICT (run_id) DO UPDATE SET status = 'copying', manifest = EXCLUDED.manifest",
                    (run_id, _canonical_manifest_json(source_manifest)),
                )
                # Re-running a failed/verified import of this same immutable
                # snapshot replaces only its own rows, inside this transaction.
                cursor.execute("DELETE FROM app_graph_edges WHERE snapshot_run_id = %s", (run_id,))
                cursor.execute("DELETE FROM app_graph_nodes WHERE snapshot_run_id = %s", (run_id,))
                cursor.execute(
                    "INSERT INTO app_graph_nodes(label, node_id, properties, content_sha256, snapshot_run_id) "
                    "SELECT label, node_id, properties, content_sha256, %s "
                    "FROM bibliotheca_migrated_nodes WHERE run_id = %s",
                    (run_id, run_id),
                )
                cursor.execute(
                    "INSERT INTO app_graph_edges(edge_id, relationship, source_label, source_id, "
                    "target_label, target_id, properties, content_sha256, snapshot_run_id) "
                    "SELECT edge_id, relationship, source_label, source_id, target_label, target_id, "
                    "properties, content_sha256, %s "
                    "FROM bibliotheca_migrated_edges WHERE run_id = %s",
                    (run_id, run_id),
                )

                verified_manifest = {
                    "format_version": 1,
                    "source_run_id": run_id,
                    "nodes": {},
                    "relationships": {},
                    "schema": source_manifest.get("schema", {}),
                }
                for label, expected in source_manifest.get("nodes", {}).items():
                    digest = _DigestAccumulator()
                    with postgres_connection.cursor(name=f"verify_graph_nodes_{uuid.uuid4().hex}") as verify_cursor:
                        verify_cursor.execute(
                            "SELECT node_id, properties, content_sha256 FROM app_graph_nodes "
                            "WHERE snapshot_run_id = %s AND label = %s ORDER BY node_id",
                            (run_id, label),
                        )
                        for node_id, properties, stored_hash in verify_cursor:
                            actual_hash = _stored_content_hash(properties)
                            if actual_hash != str(stored_hash):
                                raise MigrationError(f"Graph snapshot verification failed for {label}:{node_id}")
                            digest.add(actual_hash)
                    if digest.count != expected["count"] or digest.hexdigest() != expected["sha256"]:
                        raise MigrationError(f"Graph snapshot count/hash mismatch for node table {label}")
                    verified_manifest["nodes"][label] = {"count": digest.count, "sha256": digest.hexdigest()}

                relationships = source_manifest.get("schema", {}).get("relationships", {})
                for relationship, expected in source_manifest.get("relationships", {}).items():
                    source_label, target_label = relationships[relationship]
                    digest = _DigestAccumulator()
                    with postgres_connection.cursor(name=f"verify_graph_edges_{uuid.uuid4().hex}") as verify_cursor:
                        verify_cursor.execute(
                            "SELECT source_id, target_id, properties, content_sha256 FROM app_graph_edges "
                            "WHERE snapshot_run_id = %s AND relationship = %s ORDER BY edge_id",
                            (run_id, relationship),
                        )
                        for source_id, target_id, properties, stored_hash in verify_cursor:
                            actual_hash = _stored_edge_hash(
                                relationship, source_label, str(source_id), target_label, str(target_id), properties
                            )
                            if actual_hash != str(stored_hash):
                                raise MigrationError(f"Graph snapshot edge property verification failed for {relationship}")
                            digest.add(actual_hash)
                    if digest.count != expected["count"] or digest.hexdigest() != expected["sha256"]:
                        raise MigrationError(f"Graph snapshot count/hash mismatch for relationship {relationship}")
                    verified_manifest["relationships"][relationship] = {
                        "count": digest.count, "sha256": digest.hexdigest()
                    }

                cursor.execute(
                    "UPDATE bibliotheca_graph_imports SET status = 'verified', manifest = %s::jsonb, verified_at = now() "
                    "WHERE run_id = %s",
                    (_canonical_manifest_json(verified_manifest), run_id),
                )
        return verified_manifest
    except Exception as exc:
        if isinstance(exc, MigrationError):
            raise
        logger.exception("Verified PostgreSQL graph snapshot import failed; archive was retained")
        raise MigrationError("PostgreSQL graph snapshot import failed; transaction rolled back") from exc


def import_verified_users(postgres_connection: Any, run_id: str) -> dict[str, Any]:
    """Project archived User nodes into the normalized PostgreSQL users table.

    This stage requires a clean destination user table, runs atomically, and
    records a source hash per imported identity. Unknown account properties
    remain in ``users.extra`` and in the immutable typed graph snapshot.
    """
    column_names = _USER_IMPORT_COLUMNS
    json_columns = ("extra",)
    try:
        with postgres_connection.transaction():
            with postgres_connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('mybibliotheca:relational-user-import'))")
                cursor.execute(
                    "SELECT status FROM bibliotheca_graph_imports WHERE run_id = %s FOR SHARE", (run_id,)
                )
                graph_run = cursor.fetchone()
                if not graph_run or graph_run[0] != "verified":
                    raise MigrationError(f"PostgreSQL graph snapshot {run_id} is missing or not verified")
                cursor.execute(
                    "SELECT status, manifest FROM bibliotheca_relational_imports "
                    "WHERE run_id = %s AND target_table = 'users' FOR UPDATE",
                    (run_id,),
                )
                prior = cursor.fetchone()
                if prior and prior[0] == "verified":
                    prior_manifest = prior[1]
                    prior_manifest = json.loads(prior_manifest) if isinstance(prior_manifest, str) else prior_manifest
                    expected_count = int(prior_manifest.get("count", -1))
                    cursor.execute(
                        "SELECT count(*) FROM bibliotheca_relational_imported_entities e "
                        "JOIN users u ON u.id = e.entity_id "
                        "WHERE e.run_id = %s AND e.entity_type = 'users'",
                        (run_id,),
                    )
                    present_count = int(cursor.fetchone()[0])
                    cursor.execute(
                        "SELECT count(*) FROM bibliotheca_relational_imported_entities "
                        "WHERE run_id = %s AND entity_type = 'users'",
                        (run_id,),
                    )
                    provenance_count = int(cursor.fetchone()[0])
                    if expected_count != provenance_count or provenance_count != present_count:
                        raise MigrationError("Previously imported PostgreSQL users failed retry verification")
                    selected = (*column_names, "extra")
                    cursor.execute(
                        "SELECT e.target_sha256, " + ", ".join(f"u.{name}" for name in selected) +
                        " FROM bibliotheca_relational_imported_entities e JOIN users u ON u.id = e.entity_id "
                        "WHERE e.run_id = %s AND e.entity_type = 'users' ORDER BY e.entity_id",
                        (run_id,),
                    )
                    for stored_hash, *row in cursor.fetchall():
                        if not stored_hash or _content_hash(_postgres_value(dict(zip(selected, row)))) != str(stored_hash):
                            raise MigrationError("Previously imported PostgreSQL user failed content verification")
                    return prior_manifest

                cursor.execute("SELECT count(*) FROM users")
                if int(cursor.fetchone()[0]) != 0:
                    raise MigrationError("Refusing to import Kuzu users into a non-empty PostgreSQL users table")
                cursor.execute(
                    "INSERT INTO bibliotheca_relational_imports(run_id, target_table, status, manifest) "
                    "VALUES (%s, 'users', 'copying', '{}'::jsonb) "
                    "ON CONFLICT (run_id, target_table) DO UPDATE SET status = 'copying', manifest = '{}'::jsonb",
                    (run_id,),
                )
                cursor.execute("DELETE FROM bibliotheca_relational_imported_entities WHERE run_id = %s AND entity_type = 'users'", (run_id,))

            digest = _DigestAccumulator()
            count = 0
            with postgres_connection.cursor(name=f"import_users_{uuid.uuid4().hex}") as source_cursor:
                source_cursor.execute(
                    "SELECT node_id, properties, content_sha256 FROM app_graph_nodes "
                    "WHERE snapshot_run_id = %s AND label = 'User' ORDER BY node_id",
                    (run_id,),
                )
                insert_columns = (*column_names, "extra")
                placeholders = ["%s::jsonb" if name in json_columns else "%s" for name in insert_columns]
                insert_sql = (
                    f"INSERT INTO users ({', '.join(insert_columns)}) "
                    f"VALUES ({', '.join(placeholders)}) RETURNING {', '.join(insert_columns)}"
                )
                while True:
                    batch = source_cursor.fetchmany(250)
                    if not batch:
                        break
                    for node_id, stored_properties, source_hash in batch:
                        properties = decode_migration_value(stored_properties)
                        values, extra = _normalize_user_projection(str(node_id), properties)
                        insert_values = [values.get(name) for name in column_names]
                        insert_values.append(_jsonb_projection(extra))
                        cursor = postgres_connection.cursor()
                        try:
                            cursor.execute(insert_sql, insert_values)
                            inserted = dict(zip(insert_columns, cursor.fetchone()))
                            expected = {name: values.get(name) for name in column_names}
                            expected["extra"] = extra
                            if _content_hash(_postgres_value(inserted)) != _content_hash(_postgres_value(expected)):
                                raise MigrationError(f"PostgreSQL user field verification failed for {node_id}")
                            target_hash = _content_hash(_postgres_value(expected))
                            cursor.execute(
                                "INSERT INTO bibliotheca_relational_imported_entities "
                                "(run_id, entity_type, entity_id, source_sha256, target_sha256) "
                                "VALUES (%s, 'users', %s, %s, %s)",
                                (run_id, str(node_id), str(source_hash), target_hash),
                            )
                        finally:
                            cursor.close()
                        digest.add(str(source_hash))
                        count += 1

            with postgres_connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM bibliotheca_relational_imported_entities "
                    "WHERE run_id = %s AND entity_type = 'users'",
                    (run_id,),
                )
                provenance_count = int(cursor.fetchone()[0])
                cursor.execute("SELECT count(*) FROM users")
                user_count = int(cursor.fetchone()[0])
                if provenance_count != count or user_count != count:
                    raise MigrationError("PostgreSQL user projection row-count verification failed")
                manifest = {"count": count, "sha256": digest.hexdigest()}
                cursor.execute(
                    "UPDATE bibliotheca_relational_imports SET status = 'verified', manifest = %s::jsonb, verified_at = now() "
                    "WHERE run_id = %s AND target_table = 'users'",
                    (_canonical_manifest_json(manifest), run_id),
                )
        return manifest
    except Exception as exc:
        if isinstance(exc, MigrationError):
            raise
        logger.exception("Normalized PostgreSQL user import failed; graph snapshot was retained")
        raise MigrationError("Normalized PostgreSQL user import failed; transaction rolled back") from exc


def import_verified_books(postgres_connection: Any, run_id: str) -> dict[str, Any]:
    """Project archived Book nodes into normalized PostgreSQL books rows."""
    column_names = _BOOK_IMPORT_COLUMNS
    try:
        with postgres_connection.transaction():
            with postgres_connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('mybibliotheca:relational-book-import'))")
                cursor.execute(
                    "SELECT status FROM bibliotheca_graph_imports WHERE run_id = %s FOR SHARE", (run_id,)
                )
                graph_run = cursor.fetchone()
                if not graph_run or graph_run[0] != "verified":
                    raise MigrationError(f"PostgreSQL graph snapshot {run_id} is missing or not verified")
                cursor.execute(
                    "SELECT status, manifest FROM bibliotheca_relational_imports "
                    "WHERE run_id = %s AND target_table = 'books' FOR UPDATE",
                    (run_id,),
                )
                prior = cursor.fetchone()
                if prior and prior[0] == "verified":
                    prior_manifest = json.loads(prior[1]) if isinstance(prior[1], str) else prior[1]
                    expected_count = int(prior_manifest.get("count", -1))
                    cursor.execute(
                        "SELECT count(*) FROM bibliotheca_relational_imported_entities e "
                        "JOIN books b ON b.id = e.entity_id "
                        "WHERE e.run_id = %s AND e.entity_type = 'books'",
                        (run_id,),
                    )
                    present_count = int(cursor.fetchone()[0])
                    cursor.execute(
                        "SELECT count(*) FROM bibliotheca_relational_imported_entities "
                        "WHERE run_id = %s AND entity_type = 'books'",
                        (run_id,),
                    )
                    provenance_count = int(cursor.fetchone()[0])
                    if expected_count != provenance_count or provenance_count != present_count:
                        raise MigrationError("Previously imported PostgreSQL books failed retry verification")
                    selected = (*column_names, "extra")
                    cursor.execute(
                        "SELECT e.target_sha256, " + ", ".join(f"b.{name}" for name in selected) +
                        " FROM bibliotheca_relational_imported_entities e JOIN books b ON b.id = e.entity_id "
                        "WHERE e.run_id = %s AND e.entity_type = 'books' ORDER BY e.entity_id",
                        (run_id,),
                    )
                    for stored_hash, *row in cursor.fetchall():
                        if not stored_hash or _content_hash(_postgres_value(dict(zip(selected, row)))) != str(stored_hash):
                            raise MigrationError("Previously imported PostgreSQL book failed content verification")
                    return prior_manifest
                cursor.execute("SELECT count(*) FROM books")
                if int(cursor.fetchone()[0]) != 0:
                    raise MigrationError("Refusing to import Kuzu books into a non-empty PostgreSQL books table")
                cursor.execute(
                    "INSERT INTO bibliotheca_relational_imports(run_id, target_table, status, manifest) "
                    "VALUES (%s, 'books', 'copying', '{}'::jsonb) "
                    "ON CONFLICT (run_id, target_table) DO UPDATE SET status = 'copying', manifest = '{}'::jsonb",
                    (run_id,),
                )
                cursor.execute(
                    "DELETE FROM bibliotheca_relational_imported_entities WHERE run_id = %s AND entity_type = 'books'",
                    (run_id,),
                )

            digest = _DigestAccumulator()
            count = 0
            insert_columns = (*column_names, "extra")
            placeholders = ["%s::jsonb" if name in (*_BOOK_JSON_COLUMNS, "extra") else "%s" for name in insert_columns]
            insert_sql = (
                f"INSERT INTO books ({', '.join(insert_columns)}) "
                f"VALUES ({', '.join(placeholders)}) RETURNING {', '.join(insert_columns)}"
            )
            with postgres_connection.cursor(name=f"import_books_{uuid.uuid4().hex}") as source_cursor:
                source_cursor.execute(
                    "SELECT node_id, properties, content_sha256 FROM app_graph_nodes "
                    "WHERE snapshot_run_id = %s AND label = 'Book' ORDER BY node_id",
                    (run_id,),
                )
                while True:
                    batch = source_cursor.fetchmany(250)
                    if not batch:
                        break
                    for node_id, stored_properties, source_hash in batch:
                        properties = decode_migration_value(stored_properties)
                        values, extra = _normalize_book_projection(str(node_id), properties)
                        insert_values: list[Any] = []
                        for name in column_names:
                            value = values.get(name)
                            insert_values.append(_jsonb_projection(value) if name in _BOOK_JSON_COLUMNS else value)
                        insert_values.append(_jsonb_projection(extra))
                        with postgres_connection.cursor() as cursor:
                            cursor.execute(insert_sql, insert_values)
                            inserted = dict(zip(insert_columns, cursor.fetchone()))
                            expected = {name: values.get(name) for name in column_names}
                            for name in _BOOK_JSON_COLUMNS:
                                expected[name] = json.loads(_jsonb_projection(expected[name]))
                            expected["extra"] = extra
                            if _content_hash(_postgres_value(inserted)) != _content_hash(_postgres_value(expected)):
                                raise MigrationError(f"PostgreSQL book field verification failed for {node_id}")
                            target_hash = _content_hash(_postgres_value(expected))
                            cursor.execute(
                                "INSERT INTO bibliotheca_relational_imported_entities "
                                "(run_id, entity_type, entity_id, source_sha256, target_sha256) "
                                "VALUES (%s, 'books', %s, %s, %s)",
                                (run_id, str(node_id), str(source_hash), target_hash),
                            )
                        digest.add(str(source_hash))
                        count += 1

            with postgres_connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM bibliotheca_relational_imported_entities "
                    "WHERE run_id = %s AND entity_type = 'books'",
                    (run_id,),
                )
                provenance_count = int(cursor.fetchone()[0])
                cursor.execute("SELECT count(*) FROM books")
                book_count = int(cursor.fetchone()[0])
                if provenance_count != count or book_count != count:
                    raise MigrationError("PostgreSQL book projection row-count verification failed")
                manifest = {"count": count, "sha256": digest.hexdigest()}
                cursor.execute(
                    "UPDATE bibliotheca_relational_imports SET status = 'verified', manifest = %s::jsonb, verified_at = now() "
                    "WHERE run_id = %s AND target_table = 'books'",
                    (_canonical_manifest_json(manifest), run_id),
                )
        return manifest
    except Exception as exc:
        if isinstance(exc, MigrationError):
            raise
        logger.exception("Normalized PostgreSQL book import failed; graph snapshot was retained")
        raise MigrationError("Normalized PostgreSQL book import failed; transaction rolled back") from exc


def import_verified_user_books(postgres_connection: Any, run_id: str) -> dict[str, Any]:
    """Project User -> Book edges into one PostgreSQL row per library pair.

    Legacy OWNS edges populate the base row. READS data overlays that base,
    followed by HAS_PERSONAL_METADATA as the current user-specific layer.
    Parallel or duplicate graph edges remain losslessly recorded in both the
    graph snapshot and user_books.extra._source_relationships.
    """
    try:
        with postgres_connection.transaction():
            with postgres_connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('mybibliotheca:relational-user-books-import'))")
                cursor.execute(
                    "SELECT status FROM bibliotheca_graph_imports WHERE run_id = %s FOR SHARE", (run_id,)
                )
                graph_run = cursor.fetchone()
                if not graph_run or graph_run[0] != "verified":
                    raise MigrationError(f"PostgreSQL graph snapshot {run_id} is missing or not verified")
                cursor.execute(
                    "SELECT target_table, status FROM bibliotheca_relational_imports "
                    "WHERE run_id = %s AND target_table IN ('users', 'books')",
                    (run_id,),
                )
                required_imports = {table: status for table, status in cursor.fetchall()}
                if required_imports != {"users": "verified", "books": "verified"}:
                    raise MigrationError("PostgreSQL users and books must be verified before importing library links")
                cursor.execute(
                    "SELECT status, manifest FROM bibliotheca_relational_imports "
                    "WHERE run_id = %s AND target_table = 'user_books' FOR UPDATE",
                    (run_id,),
                )
                prior = cursor.fetchone()
                if prior and prior[0] == "verified":
                    prior_manifest = json.loads(prior[1]) if isinstance(prior[1], str) else prior[1]
                    expected_count = int(prior_manifest.get("pair_count", -1))
                    cursor.execute(
                        "SELECT count(*) FROM bibliotheca_relational_imported_entities e "
                        "JOIN user_books ub ON ub.extra ->> '_migration_pair_key' = e.entity_id "
                        "WHERE e.run_id = %s AND e.entity_type = 'user_books'",
                        (run_id,),
                    )
                    present_count = int(cursor.fetchone()[0])
                    cursor.execute(
                        "SELECT count(*) FROM bibliotheca_relational_imported_entities "
                        "WHERE run_id = %s AND entity_type = 'user_books'",
                        (run_id,),
                    )
                    provenance_count = int(cursor.fetchone()[0])
                    if expected_count != provenance_count or provenance_count != present_count:
                        raise MigrationError("Previously imported PostgreSQL library links failed retry verification")
                    cursor.execute(
                        "SELECT e.target_sha256, " + ", ".join(f"ub.{name}" for name in _USER_BOOK_COLUMNS) +
                        " FROM bibliotheca_relational_imported_entities e "
                        "JOIN user_books ub ON ub.extra ->> '_migration_pair_key' = e.entity_id "
                        "WHERE e.run_id = %s AND e.entity_type = 'user_books' ORDER BY e.entity_id",
                        (run_id,),
                    )
                    for stored_hash, *row in cursor.fetchall():
                        if not stored_hash or _content_hash(_postgres_value(dict(zip(_USER_BOOK_COLUMNS, row)))) != str(stored_hash):
                            raise MigrationError("Previously imported PostgreSQL library link failed content verification")
                    return prior_manifest
                cursor.execute("SELECT count(*) FROM user_books")
                if int(cursor.fetchone()[0]) != 0:
                    raise MigrationError("Refusing to import Kuzu library links into a non-empty PostgreSQL table")
                cursor.execute(
                    "INSERT INTO bibliotheca_relational_imports(run_id, target_table, status, manifest) "
                    "VALUES (%s, 'user_books', 'copying', '{}'::jsonb) "
                    "ON CONFLICT (run_id, target_table) DO UPDATE SET status = 'copying', manifest = '{}'::jsonb",
                    (run_id,),
                )
                cursor.execute(
                    "DELETE FROM bibliotheca_relational_imported_entities "
                    "WHERE run_id = %s AND entity_type = 'user_books'",
                    (run_id,),
                )

            source_edge_count = 0
            pair_count = 0
            pair_digest = _DigestAccumulator()
            insert_columns = _USER_BOOK_COLUMNS
            placeholders = [
                "%s::jsonb" if column in _USER_BOOK_JSON_COLUMNS else "%s"
                for column in insert_columns
            ]
            insert_sql = (
                f"INSERT INTO user_books ({', '.join(insert_columns)}) "
                f"VALUES ({', '.join(placeholders)}) RETURNING {', '.join(insert_columns)}"
            )

            def flush_pair(pair_user_id: str, pair_book_id: str, pair_edges: list[tuple[int, str, dict[str, Any], str]]) -> None:
                nonlocal source_edge_count, pair_count
                values, pair_key, source_digest = _merge_user_book_edges(pair_user_id, pair_book_id, pair_edges)
                source_edge_count += len(pair_edges)
                insert_values = []
                for column in insert_columns:
                    value = values.get(column)
                    if column in _USER_BOOK_JSON_COLUMNS:
                        value = _jsonb_projection(value)
                    insert_values.append(value)
                with postgres_connection.cursor() as cursor:
                    cursor.execute(insert_sql, insert_values)
                    inserted = dict(zip(insert_columns, cursor.fetchone()))
                    expected = {column: values.get(column) for column in insert_columns}
                    for column in _USER_BOOK_JSON_COLUMNS:
                        expected[column] = json.loads(_jsonb_projection(expected[column]))
                    if _content_hash(_postgres_value(inserted)) != _content_hash(_postgres_value(expected)):
                        raise MigrationError(
                            f"PostgreSQL user-book field verification failed for {pair_user_id}/{pair_book_id}"
                        )
                    target_hash = _content_hash(_postgres_value(expected))
                    cursor.execute(
                        "INSERT INTO bibliotheca_relational_imported_entities "
                        "(run_id, entity_type, entity_id, source_sha256, target_sha256) "
                        "VALUES (%s, 'user_books', %s, %s, %s)",
                        (run_id, pair_key, source_digest, target_hash),
                    )
                pair_digest.add(_content_hash([pair_key, source_digest]))
                pair_count += 1

            with postgres_connection.cursor(name=f"import_user_books_{uuid.uuid4().hex}") as source_cursor:
                source_cursor.execute(
                    "SELECT edge_id, relationship, source_id, target_id, properties, content_sha256 "
                    "FROM app_graph_edges WHERE snapshot_run_id = %s AND source_label = 'User' "
                    "AND target_label = 'Book' ORDER BY source_id, target_id, edge_id",
                    (run_id,),
                )
                current_pair: tuple[str, str] | None = None
                pair_edges: list[tuple[int, str, dict[str, Any], str]] = []
                while True:
                    batch = source_cursor.fetchmany(500)
                    if not batch:
                        break
                    for edge_id, relationship, source_id, target_id, stored_properties, source_hash in batch:
                        pair = (str(source_id), str(target_id))
                        if current_pair is not None and pair != current_pair:
                            flush_pair(current_pair[0], current_pair[1], pair_edges)
                            pair_edges = []
                        current_pair = pair
                        properties = decode_migration_value(stored_properties)
                        if not isinstance(properties, dict):
                            raise MigrationError(f"User -> Book edge {edge_id} properties are not a map")
                        pair_edges.append((int(edge_id), str(relationship), properties, str(source_hash)))
                if current_pair is not None:
                    flush_pair(current_pair[0], current_pair[1], pair_edges)

            with postgres_connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM bibliotheca_relational_imported_entities "
                    "WHERE run_id = %s AND entity_type = 'user_books'",
                    (run_id,),
                )
                provenance_count = int(cursor.fetchone()[0])
                cursor.execute("SELECT count(*) FROM user_books")
                projected_count = int(cursor.fetchone()[0])
                if provenance_count != pair_count or projected_count != pair_count:
                    raise MigrationError("PostgreSQL user-book projection row-count verification failed")
                manifest = {
                    "source_edge_count": source_edge_count,
                    "pair_count": pair_count,
                    "sha256": pair_digest.hexdigest(),
                    "precedence": ["OWNS", "READS", "HAS_PERSONAL_METADATA"],
                }
                cursor.execute(
                    "UPDATE bibliotheca_relational_imports SET status = 'verified', manifest = %s::jsonb, verified_at = now() "
                    "WHERE run_id = %s AND target_table = 'user_books'",
                    (_canonical_manifest_json(manifest), run_id),
                )
        return manifest
    except Exception as exc:
        if isinstance(exc, MigrationError):
            raise
        logger.exception("Normalized PostgreSQL library-link import failed; graph snapshot was retained")
        raise MigrationError("Normalized PostgreSQL library-link import failed; transaction rolled back") from exc


def import_verified_reference_entities(postgres_connection: Any, run_id: str) -> dict[str, dict[str, Any]]:
    """Project remaining supported Kuzu node types into normalized tables.

    Import users and books first. The importer enforces the configured foreign
    key dependencies, copies unknown properties into ``extra``, and leaves the
    immutable typed graph archive intact for rollback and audit.
    """
    results: dict[str, dict[str, Any]] = {}
    try:
        for label, spec in _REFERENCE_ENTITY_IMPORTS.items():
            table = spec["table"]
            columns = tuple(spec["columns"])
            json_columns = set(spec["json"])
            known = {"id", *columns}
            insert_columns = ("id", *columns, "extra")
            placeholders = ["%s::jsonb" if name in json_columns | {"extra"} else "%s" for name in insert_columns]
            insert_sql = (
                f"INSERT INTO {table} ({', '.join(insert_columns)}) "
                f"VALUES ({', '.join(placeholders)}) RETURNING {', '.join(insert_columns)}"
            )

            with postgres_connection.transaction():
                with postgres_connection.cursor() as cursor:
                    cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"mybibliotheca:relational:{table}",))
                    cursor.execute("SELECT status FROM bibliotheca_graph_imports WHERE run_id = %s FOR SHARE", (run_id,))
                    graph_run = cursor.fetchone()
                    if not graph_run or graph_run[0] != "verified":
                        raise MigrationError(f"PostgreSQL graph snapshot {run_id} is missing or not verified")
                    required_tables = tuple(spec.get("requires", ()))
                    if required_tables:
                        placeholders_for_dependencies = ", ".join("%s" for _ in required_tables)
                        cursor.execute(
                            "SELECT target_table, status FROM bibliotheca_relational_imports "
                            f"WHERE run_id = %s AND target_table IN ({placeholders_for_dependencies})",
                            (run_id, *required_tables),
                        )
                        imported_dependencies = {name: status for name, status in cursor.fetchall()}
                        if any(imported_dependencies.get(name) != "verified" for name in required_tables):
                            raise MigrationError(
                                f"PostgreSQL dependencies {required_tables} must be imported before {table}"
                            )
                    cursor.execute(
                        "SELECT status, manifest FROM bibliotheca_relational_imports "
                        "WHERE run_id = %s AND target_table = %s FOR UPDATE",
                        (run_id, table),
                    )
                    prior = cursor.fetchone()
                    if prior and prior[0] == "verified":
                        prior_manifest = json.loads(prior[1]) if isinstance(prior[1], str) else prior[1]
                        expected_count = int(prior_manifest.get("count", -1))
                        cursor.execute(
                            "SELECT count(*) FROM bibliotheca_relational_imported_entities e "
                            f"JOIN {table} t ON t.id = e.entity_id "
                            "WHERE e.run_id = %s AND e.entity_type = %s",
                            (run_id, table),
                        )
                        present_count = int(cursor.fetchone()[0])
                        cursor.execute(
                            "SELECT count(*) FROM bibliotheca_relational_imported_entities "
                            "WHERE run_id = %s AND entity_type = %s",
                            (run_id, table),
                        )
                        provenance_count = int(cursor.fetchone()[0])
                        if expected_count != provenance_count or provenance_count != present_count:
                            raise MigrationError(f"Previously imported PostgreSQL {table} failed retry verification")
                        selected = ("id", *columns, "extra")
                        cursor.execute(
                            "SELECT e.target_sha256, " + ", ".join(f"t.{name}" for name in selected) +
                            f" FROM bibliotheca_relational_imported_entities e JOIN {table} t ON t.id = e.entity_id "
                            "WHERE e.run_id = %s AND e.entity_type = %s ORDER BY e.entity_id",
                            (run_id, table),
                        )
                        for stored_target_hash, *row in cursor.fetchall():
                            if not stored_target_hash or _content_hash(_postgres_value(dict(zip(selected, row)))) != str(stored_target_hash):
                                raise MigrationError(f"Previously imported PostgreSQL {table} row failed content verification")
                        results[table] = prior_manifest
                        continue
                    cursor.execute(f"SELECT count(*) FROM {table}")
                    if int(cursor.fetchone()[0]) != 0:
                        raise MigrationError(f"Refusing to import Kuzu {label} nodes into non-empty PostgreSQL {table}")
                    cursor.execute(
                        "INSERT INTO bibliotheca_relational_imports(run_id, target_table, status, manifest) "
                        "VALUES (%s, %s, 'copying', '{}'::jsonb) "
                        "ON CONFLICT (run_id, target_table) DO UPDATE SET status = 'copying', manifest = '{}'::jsonb",
                        (run_id, table),
                    )
                    cursor.execute(
                        "DELETE FROM bibliotheca_relational_imported_entities WHERE run_id = %s AND entity_type = %s",
                        (run_id, table),
                    )

                digest = _DigestAccumulator()
                count = 0
                with postgres_connection.cursor(name=f"import_{table}_{uuid.uuid4().hex}") as source_cursor:
                    source_cursor.execute(
                        "SELECT node_id, properties, content_sha256 FROM app_graph_nodes "
                        "WHERE snapshot_run_id = %s AND label = %s ORDER BY node_id",
                        (run_id, label),
                    )
                    while True:
                        batch = source_cursor.fetchmany(250)
                        if not batch:
                            break
                        for node_id, stored_properties, source_hash in batch:
                            properties = decode_migration_value(stored_properties)
                            source_key = str(spec.get("key", "id"))
                            if not isinstance(properties, dict) or str(properties.get(source_key, "")) != str(node_id):
                                raise MigrationError(f"Kuzu {label} node id mismatch for {node_id}")
                            values = {name: properties.get(name) for name in columns}
                            values["id"] = str(node_id)
                            for target_name, source_name in spec.get("aliases", {}).items():
                                if values.get(target_name) is None and properties.get(source_name) is not None:
                                    values[target_name] = properties[source_name]
                            if values.get("parent_id") == "":
                                values["parent_id"] = None
                            for name, default in spec["defaults"].items():
                                if values.get(name) is None:
                                    values[name] = default
                            for name in json_columns:
                                value = values.get(name)
                                if isinstance(value, str):
                                    try:
                                        values[name] = json.loads(value)
                                    except (TypeError, ValueError):
                                        # Keep invalid legacy JSON as a string, losslessly.
                                        pass
                            extra = {key: value for key, value in properties.items() if key not in known}
                            insert_values = [
                                _jsonb_projection(extra) if name == "extra" else
                                _jsonb_projection(values.get(name)) if name in json_columns else
                                values.get(name)
                                for name in insert_columns
                            ]
                            with postgres_connection.cursor() as cursor:
                                cursor.execute(insert_sql, insert_values)
                                returned = dict(zip(insert_columns, cursor.fetchone()))
                                expected = {name: values.get(name) for name in ("id", *columns)}
                                for name in json_columns:
                                    expected[name] = json.loads(_jsonb_projection(expected.get(name)))
                                expected["extra"] = extra
                                if _content_hash(_postgres_value(returned)) != _content_hash(_postgres_value(expected)):
                                    raise MigrationError(f"PostgreSQL {table} field verification failed for {node_id}")
                                cursor.execute(
                                    "INSERT INTO bibliotheca_relational_imported_entities "
                                    "(run_id, entity_type, entity_id, source_sha256, target_sha256) "
                                    "VALUES (%s, %s, %s, %s, %s)",
                                    (run_id, table, str(node_id), str(source_hash), _content_hash(_postgres_value(expected))),
                                )
                            digest.add(str(source_hash))
                            count += 1

                with postgres_connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT count(*) FROM bibliotheca_relational_imported_entities "
                        "WHERE run_id = %s AND entity_type = %s",
                        (run_id, table),
                    )
                    provenance_count = int(cursor.fetchone()[0])
                    cursor.execute(f"SELECT count(*) FROM {table}")
                    target_count = int(cursor.fetchone()[0])
                    if provenance_count != count or target_count != count:
                        raise MigrationError(f"PostgreSQL {table} projection row-count verification failed")
                    manifest = {"count": count, "sha256": digest.hexdigest(), "source_label": label}
                    cursor.execute(
                        "UPDATE bibliotheca_relational_imports SET status = 'verified', manifest = %s::jsonb, verified_at = now() "
                        "WHERE run_id = %s AND target_table = %s",
                        (_canonical_manifest_json(manifest), run_id, table),
                    )
                results[table] = manifest
        return results
    except Exception as exc:
        if isinstance(exc, MigrationError):
            raise
        logger.exception("Normalized PostgreSQL reference entity import failed; graph snapshot was retained")
        raise MigrationError("Normalized PostgreSQL reference entity import failed; transaction rolled back") from exc


def import_verified_custom_field_edges(postgres_connection: Any, run_id: str) -> dict[str, Any]:
    """Project legacy User-[:HAS_CUSTOM_FIELD]->CustomField values.

    These edges carry user/book-scoped values in older Kuzu databases. Keep
    their typed value in ``custom_fields`` and retain every original edge in
    the immutable graph archive. A deterministic target id makes retries
    idempotent without conflating parallel source edges.
    """
    target_columns = _CUSTOM_FIELD_PROJECTION_COLUMNS
    target_id_prefix = f"kuzu-edge:{run_id}:"
    try:
        with postgres_connection.transaction():
            with postgres_connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('mybibliotheca:relational:custom-field-edges'))")
                cursor.execute(
                    "SELECT status FROM bibliotheca_relational_imports WHERE run_id=%s AND target_table='users'",
                    (run_id,),
                )
                users_run = cursor.fetchone()
                cursor.execute(
                    "SELECT status FROM bibliotheca_relational_imports WHERE run_id=%s AND target_table='custom_field_definitions'",
                    (run_id,),
                )
                definitions_run = cursor.fetchone()
                cursor.execute(
                    "SELECT status FROM bibliotheca_relational_imports WHERE run_id=%s AND target_table='books'",
                    (run_id,),
                )
                books_run = cursor.fetchone()
                if (
                    not users_run or users_run[0] != "verified"
                    or not definitions_run or definitions_run[0] != "verified"
                    or not books_run or books_run[0] != "verified"
                ):
                    raise MigrationError("Users, books, and custom field definitions must be imported before custom field values")
                cursor.execute(
                    "SELECT status,manifest FROM bibliotheca_relational_imports "
                    "WHERE run_id=%s AND target_table='custom_fields' FOR UPDATE",
                    (run_id,),
                )
                prior = cursor.fetchone()
                if prior and prior[0] == "verified":
                    manifest = json.loads(prior[1]) if isinstance(prior[1], str) else prior[1]
                    expected = int(manifest.get("edge_count", -1))
                    cursor.execute(
                        "SELECT count(*) FROM bibliotheca_relational_imported_entities e "
                        "JOIN custom_fields f ON f.id=%s || e.entity_id "
                        "WHERE e.run_id=%s AND e.entity_type='custom_fields'",
                        (target_id_prefix, run_id),
                    )
                    present = int(cursor.fetchone()[0])
                    cursor.execute(
                        "SELECT count(*) FROM bibliotheca_relational_imported_entities "
                        "WHERE run_id=%s AND entity_type='custom_fields'", (run_id,),
                    )
                    provenance = int(cursor.fetchone()[0])
                    cursor.execute("SELECT count(*) FROM custom_fields WHERE id LIKE %s", (target_id_prefix + "%",))
                    targets = int(cursor.fetchone()[0])
                    if expected != present or present != provenance or provenance != targets:
                        raise MigrationError("Previously imported custom field values failed retry verification")
                    cursor.execute(
                        "SELECT e.entity_id,e.source_sha256,e.target_sha256,g.content_sha256,g.relationship,g.source_label,"
                        "g.source_id,g.target_label,g.target_id,g.properties,f.user_id,f.definition_id,f.book_id,"
                        "f.field_name,f.field_value,f.is_required,f.is_active,f.created_at,f.updated_at,f.extra,f.id "
                        "FROM bibliotheca_relational_imported_entities e "
                        "JOIN app_graph_edges g ON g.snapshot_run_id=e.run_id AND g.edge_id=e.entity_id::bigint "
                        "JOIN custom_fields f ON f.id=%s || e.entity_id "
                        "WHERE e.run_id=%s AND e.entity_type='custom_fields' ORDER BY g.edge_id",
                        (target_id_prefix, run_id),
                    )
                    for row in cursor.fetchall():
                        (entity_id, source_hash, target_hash, edge_hash, relationship, source_label, source_id,
                         target_label, target_id, properties, user_id, definition_id, book_id,
                         field_name, field_value, is_required, is_active, created_at, updated_at, extra, row_id) = row
                        source_actual = _stored_edge_hash(
                            str(relationship), str(source_label), str(source_id),
                            str(target_label), str(target_id), properties,
                        )
                        target_actual = _content_hash(_postgres_value(dict(zip(target_columns, (
                            row_id, user_id, definition_id, book_id, field_name, field_value,
                            is_required, is_active, created_at, updated_at, extra,
                        )))))
                        if str(source_hash) != str(edge_hash) or str(source_hash) != source_actual:
                            raise MigrationError("Previously imported custom field edge failed source verification")
                        if str(row_id) != target_id_prefix + str(entity_id) or str(target_hash) != target_actual:
                            raise MigrationError("Previously imported custom field value failed content verification")
                    return manifest

                cursor.execute("SELECT count(*) FROM custom_fields")
                if int(cursor.fetchone()[0]) != 0:
                    raise MigrationError("Refusing to import Kuzu custom field edges into a non-empty custom_fields table")
                cursor.execute(
                    "INSERT INTO bibliotheca_relational_imports(run_id,target_table,status,manifest) "
                    "VALUES (%s,'custom_fields','copying','{}'::jsonb) "
                    "ON CONFLICT(run_id,target_table) DO UPDATE SET status='copying',manifest='{}'::jsonb",
                    (run_id,),
                )
                cursor.execute(
                    "DELETE FROM bibliotheca_relational_imported_entities WHERE run_id=%s AND entity_type='custom_fields'",
                    (run_id,),
                )

            digest = _DigestAccumulator()
            with postgres_connection.cursor(name=f"import_custom_field_edges_{uuid.uuid4().hex}") as edge_cursor:
                edge_cursor.execute(
                    "SELECT edge_id,source_id,target_id,properties,content_sha256 FROM app_graph_edges "
                    "WHERE snapshot_run_id=%s AND relationship='HAS_CUSTOM_FIELD' "
                    "AND source_label='User' AND target_label='CustomField' ORDER BY edge_id",
                    (run_id,),
                )
                while True:
                    batch = edge_cursor.fetchmany(250)
                    if not batch:
                        break
                    for edge_id, user_id, definition_id, stored_properties, source_hash in batch:
                        properties = decode_migration_value(stored_properties)
                        if not isinstance(properties, dict):
                            raise MigrationError(f"HAS_CUSTOM_FIELD edge {edge_id} properties are not a map")
                        with postgres_connection.cursor() as cursor:
                            cursor.execute(
                                "SELECT properties FROM app_graph_nodes WHERE snapshot_run_id=%s "
                                "AND label='CustomField' AND node_id=%s",
                                (run_id, str(definition_id)),
                            )
                            definition_row = cursor.fetchone()
                            if not definition_row:
                                raise MigrationError(f"HAS_CUSTOM_FIELD edge {edge_id} references missing field definition {definition_id}")
                            definition = decode_migration_value(definition_row[0])
                            if not isinstance(definition, dict):
                                raise MigrationError(f"CustomField definition {definition_id} properties are not a map")
                            field_name = properties.get("field_name") or definition.get("name")
                            book_id = properties.get("book_id") or None
                            mapped_properties = {"book_id", "field_name", "field_value", "created_at", "updated_at"}
                            extra = {key: value for key, value in properties.items() if key not in mapped_properties}
                            row_id = target_id_prefix + str(edge_id)
                            expected = {
                                "id": row_id, "user_id": str(user_id), "definition_id": str(definition_id),
                                "book_id": str(book_id) if book_id is not None else None,
                                "field_name": str(field_name) if field_name is not None else None,
                                "field_value": properties.get("field_value"),
                                "is_required": definition.get("is_required"),
                                "is_active": definition.get("is_active"),
                                "created_at": properties.get("created_at"),
                                "updated_at": properties.get("updated_at"), "extra": extra,
                            }
                            source_actual = _stored_edge_hash(
                                "HAS_CUSTOM_FIELD", "User", str(user_id), "CustomField", str(definition_id), stored_properties,
                            )
                            if source_actual != str(source_hash):
                                raise MigrationError(f"HAS_CUSTOM_FIELD edge {edge_id} failed source hash verification")
                            cursor.execute(
                                "INSERT INTO custom_fields(id,user_id,definition_id,book_id,field_name,field_value,"
                                "is_required,is_active,created_at,updated_at,extra) "
                                "VALUES (%(id)s,%(user_id)s,%(definition_id)s,%(book_id)s,%(field_name)s,%(field_value)s::jsonb,"
                                "%(is_required)s,%(is_active)s,%(created_at)s,%(updated_at)s,%(extra)s::jsonb) "
                                "RETURNING " + ",".join(target_columns),
                                {**expected,
                                 "field_value": _jsonb_projection(expected["field_value"]),
                                 "extra": _jsonb_projection(expected["extra"])},
                            )
                            returned = dict(zip(target_columns, cursor.fetchone()))
                            expected_db = dict(expected)
                            expected_db["field_value"] = json.loads(_jsonb_projection(expected["field_value"])) if expected["field_value"] is not None else None
                            expected_db["extra"] = json.loads(_jsonb_projection(expected["extra"]))
                            target_hash = _content_hash(_postgres_value(returned))
                            if _content_hash(_postgres_value(returned)) != _content_hash(_postgres_value(expected_db)):
                                raise MigrationError(f"PostgreSQL custom field edge projection failed for {edge_id}")
                            cursor.execute(
                                "INSERT INTO bibliotheca_relational_imported_entities "
                                "(run_id,entity_type,entity_id,source_sha256,target_sha256) "
                                "VALUES (%s,'custom_fields',%s,%s,%s)",
                                (run_id, str(edge_id), str(source_hash), target_hash),
                            )
                        digest.add(str(source_hash))

            with postgres_connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM bibliotheca_relational_imported_entities WHERE run_id=%s AND entity_type='custom_fields'",
                    (run_id,),
                )
                provenance_count = int(cursor.fetchone()[0])
                cursor.execute("SELECT count(*) FROM custom_fields WHERE id LIKE %s", (target_id_prefix + "%",))
                target_count = int(cursor.fetchone()[0])
                if provenance_count != digest.count or target_count != digest.count:
                    raise MigrationError("PostgreSQL custom field edge projection count verification failed")
                manifest = {"edge_count": digest.count, "sha256": digest.hexdigest(), "relationship": "HAS_CUSTOM_FIELD"}
                cursor.execute(
                    "UPDATE bibliotheca_relational_imports SET status='verified',manifest=%s::jsonb,verified_at=now() "
                    "WHERE run_id=%s AND target_table='custom_fields'",
                    (_canonical_manifest_json(manifest), run_id),
                )
            return manifest
    except Exception as exc:
        if isinstance(exc, MigrationError):
            raise
        logger.exception("Normalized PostgreSQL custom field edge import failed; graph snapshot was retained")
        raise MigrationError("Normalized PostgreSQL custom field edge import failed; transaction rolled back") from exc


def import_verified_reading_logs(postgres_connection: Any, run_id: str) -> dict[str, Any]:
    """Project ReadingLog nodes and their ownership/book links into PostgreSQL.

    Scalar ``user_id``/``book_id`` values and LOGGED/FOR_BOOK edges are
    reconciled. Conflicting endpoints abort the transaction; logs with a
    genuinely absent endpoint remain representable because the normalized
    foreign-key columns are nullable. Every original link is also kept in
    ``extra._source_relationships`` and in the typed graph archive.
    """
    columns = ("id", "user_id", "book_id", "date", "pages_read", "minutes_read", "notes", "created_at", "updated_at", "extra")
    json_columns = {"extra"}
    insert_sql = (
        "INSERT INTO reading_logs (" + ", ".join(columns) + ") VALUES (" +
        ", ".join("%s::jsonb" if name in json_columns else "%s" for name in columns) +
        ") RETURNING " + ", ".join(columns)
    )
    try:
        with postgres_connection.transaction():
            with postgres_connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('mybibliotheca:relational:reading_logs'))")
                cursor.execute("SELECT status FROM bibliotheca_graph_imports WHERE run_id = %s FOR SHARE", (run_id,))
                graph_run = cursor.fetchone()
                if not graph_run or graph_run[0] != "verified":
                    raise MigrationError(f"PostgreSQL graph snapshot {run_id} is missing or not verified")
                cursor.execute(
                    "SELECT target_table, status FROM bibliotheca_relational_imports "
                    "WHERE run_id = %s AND target_table IN ('users', 'books')",
                    (run_id,),
                )
                dependencies = {table: status for table, status in cursor.fetchall()}
                if dependencies.get("users") != "verified" or dependencies.get("books") != "verified":
                    raise MigrationError("PostgreSQL users and books must be imported before reading logs")
                cursor.execute(
                    "SELECT status, manifest FROM bibliotheca_relational_imports "
                    "WHERE run_id = %s AND target_table = 'reading_logs' FOR UPDATE",
                    (run_id,),
                )
                prior = cursor.fetchone()
                if prior and prior[0] == "verified":
                    prior_manifest = json.loads(prior[1]) if isinstance(prior[1], str) else prior[1]
                    expected_count = int(prior_manifest.get("count", -1))
                    cursor.execute(
                        "SELECT count(*) FROM bibliotheca_relational_imported_entities e "
                        "JOIN reading_logs t ON t.id = e.entity_id "
                        "WHERE e.run_id = %s AND e.entity_type = 'reading_logs'",
                        (run_id,),
                    )
                    present_count = int(cursor.fetchone()[0])
                    cursor.execute(
                        "SELECT count(*) FROM bibliotheca_relational_imported_entities "
                        "WHERE run_id = %s AND entity_type = 'reading_logs'",
                        (run_id,),
                    )
                    provenance_count = int(cursor.fetchone()[0])
                    if expected_count != provenance_count or provenance_count != present_count:
                        raise MigrationError("Previously imported PostgreSQL reading logs failed retry verification")
                    cursor.execute(
                        "SELECT e.target_sha256, " + ", ".join(f"t.{name}" for name in columns) +
                        " FROM bibliotheca_relational_imported_entities e "
                        "JOIN reading_logs t ON t.id = e.entity_id "
                        "WHERE e.run_id = %s AND e.entity_type = 'reading_logs' ORDER BY e.entity_id",
                        (run_id,),
                    )
                    for stored_hash, *row in cursor.fetchall():
                        if not stored_hash or _content_hash(_postgres_value(dict(zip(columns, row)))) != str(stored_hash):
                            raise MigrationError("Previously imported PostgreSQL reading log failed content verification")
                    return prior_manifest
                cursor.execute("SELECT count(*) FROM reading_logs")
                if int(cursor.fetchone()[0]) != 0:
                    raise MigrationError("Refusing to import Kuzu reading logs into a non-empty PostgreSQL table")
                cursor.execute(
                    "INSERT INTO bibliotheca_relational_imports(run_id, target_table, status, manifest) "
                    "VALUES (%s, 'reading_logs', 'copying', '{}'::jsonb) "
                    "ON CONFLICT (run_id, target_table) DO UPDATE SET status = 'copying', manifest = '{}'::jsonb",
                    (run_id,),
                )
                cursor.execute(
                    "DELETE FROM bibliotheca_relational_imported_entities WHERE run_id = %s AND entity_type = 'reading_logs'",
                    (run_id,),
                )

            digest = _DigestAccumulator()
            count = 0
            relationship_count = 0
            query = """
                SELECT n.node_id, n.properties, n.content_sha256,
                    COALESCE((
                        SELECT jsonb_agg(jsonb_build_object(
                            'edge_id', e.edge_id, 'relationship', e.relationship,
                            'source_label', e.source_label, 'source_id', e.source_id,
                            'target_label', e.target_label, 'target_id', e.target_id,
                            'properties', e.properties, 'content_sha256', e.content_sha256
                        ) ORDER BY e.edge_id)
                        FROM app_graph_edges e
                        WHERE e.snapshot_run_id = n.snapshot_run_id
                          AND ((e.relationship = 'LOGGED' AND e.target_label = 'ReadingLog' AND e.target_id = n.node_id)
                            OR (e.relationship = 'FOR_BOOK' AND e.source_label = 'ReadingLog' AND e.source_id = n.node_id))
                    ), '[]'::jsonb) AS source_edges
                FROM app_graph_nodes n
                WHERE n.snapshot_run_id = %s AND n.label = 'ReadingLog'
                ORDER BY n.node_id
            """
            with postgres_connection.cursor(name=f"import_reading_logs_{uuid.uuid4().hex}") as source_cursor:
                source_cursor.execute(query, (run_id,))
                while True:
                    batch = source_cursor.fetchmany(200)
                    if not batch:
                        break
                    for node_id, stored_properties, source_hash, source_edges in batch:
                        properties = decode_migration_value(stored_properties)
                        if not isinstance(properties, dict) or str(properties.get("id", "")) != str(node_id):
                            raise MigrationError(f"Kuzu ReadingLog node id mismatch for {node_id}")
                        edges = source_edges if isinstance(source_edges, list) else json.loads(source_edges or "[]")
                        normalized_edges: list[dict[str, Any]] = []
                        user_ids = {str(properties["user_id"])} if properties.get("user_id") is not None else set()
                        book_ids = {str(properties["book_id"])} if properties.get("book_id") is not None else set()
                        for edge in edges:
                            edge_properties = decode_migration_value(edge.get("properties"))
                            if not isinstance(edge_properties, dict):
                                raise MigrationError(f"ReadingLog relationship {edge.get('edge_id')} has invalid properties")
                            relationship = str(edge["relationship"])
                            if relationship == "LOGGED":
                                user_ids.add(str(edge["source_id"]))
                                if edge_properties.get("book_id") is not None:
                                    book_ids.add(str(edge_properties["book_id"]))
                            elif relationship == "FOR_BOOK":
                                book_ids.add(str(edge["target_id"]))
                            normalized_edges.append({
                                "edge_id": int(edge["edge_id"]),
                                "relationship": relationship,
                                "source_label": edge["source_label"],
                                "source_id": str(edge["source_id"]),
                                "target_label": edge["target_label"],
                                "target_id": str(edge["target_id"]),
                                "properties": edge_properties,
                                "content_sha256": str(edge["content_sha256"]),
                            })
                        if len(user_ids) > 1 or len(book_ids) > 1:
                            raise MigrationError(f"ReadingLog {node_id} has conflicting user/book endpoints")
                        values = {name: properties.get(name) for name in columns if name != "extra"}
                        values["id"] = str(node_id)
                        values["user_id"] = next(iter(user_ids), None)
                        values["book_id"] = next(iter(book_ids), None)
                        if isinstance(values.get("date"), datetime):
                            values["date"] = values["date"].date()
                        extra = {key: value for key, value in properties.items() if key not in set(columns)}
                        extra["_source_relationships"] = normalized_edges
                        values["extra"] = extra
                        insert_values = [
                            _jsonb_projection(values.get(name)) if name in json_columns else values.get(name)
                            for name in columns
                        ]
                        with postgres_connection.cursor() as cursor:
                            cursor.execute(insert_sql, insert_values)
                            returned = dict(zip(columns, cursor.fetchone()))
                            expected = dict(values)
                            expected["extra"] = json.loads(_jsonb_projection(extra))
                            if _content_hash(_postgres_value(returned)) != _content_hash(_postgres_value(expected)):
                                raise MigrationError(f"PostgreSQL reading log field verification failed for {node_id}")
                            target_hash = _content_hash(_postgres_value(expected))
                            cursor.execute(
                                "INSERT INTO bibliotheca_relational_imported_entities "
                                "(run_id, entity_type, entity_id, source_sha256, target_sha256) "
                                "VALUES (%s, 'reading_logs', %s, %s, %s)",
                                (run_id, str(node_id), str(source_hash), target_hash),
                            )
                        digest.add(str(source_hash))
                        count += 1
                        relationship_count += len(normalized_edges)

            with postgres_connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM bibliotheca_relational_imported_entities "
                    "WHERE run_id = %s AND entity_type = 'reading_logs'",
                    (run_id,),
                )
                provenance_count = int(cursor.fetchone()[0])
                cursor.execute("SELECT count(*) FROM reading_logs")
                target_count = int(cursor.fetchone()[0])
                if provenance_count != count or target_count != count:
                    raise MigrationError("PostgreSQL reading log projection row-count verification failed")
                manifest = {
                    "count": count,
                    "relationship_count": relationship_count,
                    "sha256": digest.hexdigest(),
                }
                cursor.execute(
                    "UPDATE bibliotheca_relational_imports SET status = 'verified', manifest = %s::jsonb, verified_at = now() "
                    "WHERE run_id = %s AND target_table = 'reading_logs'",
                    (_canonical_manifest_json(manifest), run_id),
                )
            return manifest
    except Exception as exc:
        if isinstance(exc, MigrationError):
            raise
        logger.exception("Normalized PostgreSQL reading log import failed; graph snapshot was retained")
        raise MigrationError("Normalized PostgreSQL reading log import failed; transaction rolled back") from exc


def import_verified_book_contributors(postgres_connection: Any, run_id: str) -> dict[str, Any]:
    """Project every person-to-book contribution edge without deduplicating it."""
    relationship_names = (
        "WRITTEN_BY", "AUTHORED", "EDITED", "ILLUSTRATED", "TRANSLATED", "NARRATED", "CONTRIBUTED",
    )
    columns = (
        "source_edge_id", "source_relationship", "book_id", "person_id", "contribution_type",
        "role", "order_index", "created_at", "extra",
    )
    insert_sql = (
        "INSERT INTO book_contributors (" + ", ".join(columns) + ") VALUES (" +
        ", ".join("%s::jsonb" if name == "extra" else "%s" for name in columns) +
        ") RETURNING " + ", ".join(columns)
    )
    try:
        with postgres_connection.transaction():
            with postgres_connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('mybibliotheca:relational:book_contributors'))")
                cursor.execute("SELECT status FROM bibliotheca_graph_imports WHERE run_id = %s FOR SHARE", (run_id,))
                graph_run = cursor.fetchone()
                if not graph_run or graph_run[0] != "verified":
                    raise MigrationError(f"PostgreSQL graph snapshot {run_id} is missing or not verified")
                cursor.execute(
                    "SELECT target_table, status FROM bibliotheca_relational_imports "
                    "WHERE run_id = %s AND target_table IN ('books', 'people')",
                    (run_id,),
                )
                dependencies = {table: status for table, status in cursor.fetchall()}
                if dependencies.get("books") != "verified" or dependencies.get("people") != "verified":
                    raise MigrationError("PostgreSQL books and people must be imported before contributor links")
                cursor.execute(
                    "SELECT status, manifest FROM bibliotheca_relational_imports "
                    "WHERE run_id = %s AND target_table = 'book_contributors' FOR UPDATE",
                    (run_id,),
                )
                prior = cursor.fetchone()
                if prior and prior[0] == "verified":
                    prior_manifest = json.loads(prior[1]) if isinstance(prior[1], str) else prior[1]
                    expected_count = int(prior_manifest.get("count", -1))
                    cursor.execute(
                        "SELECT count(*) FROM bibliotheca_relational_imported_entities e "
                        "JOIN book_contributors t ON t.source_edge_id::text = e.entity_id "
                        "WHERE e.run_id = %s AND e.entity_type = 'book_contributors'",
                        (run_id,),
                    )
                    present_count = int(cursor.fetchone()[0])
                    cursor.execute(
                        "SELECT count(*) FROM bibliotheca_relational_imported_entities "
                        "WHERE run_id = %s AND entity_type = 'book_contributors'",
                        (run_id,),
                    )
                    provenance_count = int(cursor.fetchone()[0])
                    if expected_count != provenance_count or provenance_count != present_count:
                        raise MigrationError("Previously imported PostgreSQL contributor links failed retry verification")
                    cursor.execute(
                        "SELECT e.target_sha256, " + ", ".join(f"t.{name}" for name in columns) +
                        " FROM bibliotheca_relational_imported_entities e "
                        "JOIN book_contributors t ON t.source_edge_id::text = e.entity_id "
                        "WHERE e.run_id = %s AND e.entity_type = 'book_contributors' ORDER BY e.entity_id",
                        (run_id,),
                    )
                    for stored_hash, *row in cursor.fetchall():
                        if not stored_hash or _content_hash(_postgres_value(dict(zip(columns, row)))) != str(stored_hash):
                            raise MigrationError("Previously imported PostgreSQL contributor link failed content verification")
                    return prior_manifest
                cursor.execute("SELECT count(*) FROM book_contributors")
                if int(cursor.fetchone()[0]) != 0:
                    raise MigrationError("Refusing to import Kuzu contributor edges into a non-empty PostgreSQL table")
                cursor.execute(
                    "INSERT INTO bibliotheca_relational_imports(run_id, target_table, status, manifest) "
                    "VALUES (%s, 'book_contributors', 'copying', '{}'::jsonb) "
                    "ON CONFLICT (run_id, target_table) DO UPDATE SET status = 'copying', manifest = '{}'::jsonb",
                    (run_id,),
                )
                cursor.execute(
                    "DELETE FROM bibliotheca_relational_imported_entities WHERE run_id = %s AND entity_type = 'book_contributors'",
                    (run_id,),
                )

            digest = _DigestAccumulator()
            count = 0
            source_edge_ids = ", ".join("%s" for _ in relationship_names)
            with postgres_connection.cursor(name=f"import_book_contributors_{uuid.uuid4().hex}") as source_cursor:
                source_cursor.execute(
                    "SELECT edge_id, relationship, source_label, source_id, target_label, target_id, properties, content_sha256 "
                    "FROM app_graph_edges WHERE snapshot_run_id = %s AND relationship IN (" + source_edge_ids + ") "
                    "ORDER BY edge_id",
                    (run_id, *relationship_names),
                )
                while True:
                    batch = source_cursor.fetchmany(300)
                    if not batch:
                        break
                    for edge_id, relationship, source_label, source_id, target_label, target_id, stored_properties, source_hash in batch:
                        if relationship == "WRITTEN_BY":
                            book_id, person_id = str(source_id), str(target_id)
                            if (source_label, target_label) != ("Book", "Person"):
                                raise MigrationError(f"WRITTEN_BY edge {edge_id} has invalid endpoint labels")
                        else:
                            person_id, book_id = str(source_id), str(target_id)
                            if (source_label, target_label) != ("Person", "Book"):
                                raise MigrationError(f"{relationship} edge {edge_id} has invalid endpoint labels")
                        properties = decode_migration_value(stored_properties)
                        if not isinstance(properties, dict):
                            raise MigrationError(f"Contributor edge {edge_id} properties are not a map")
                        known = {"contribution_type", "role", "order_index", "created_at"}
                        extra = {key: value for key, value in properties.items() if key not in known}
                        values = {
                            "source_edge_id": int(edge_id),
                            "source_relationship": str(relationship),
                            "book_id": book_id,
                            "person_id": person_id,
                            "contribution_type": properties.get("contribution_type"),
                            "role": properties.get("role"),
                            "order_index": properties.get("order_index"),
                            "created_at": properties.get("created_at"),
                            "extra": extra,
                        }
                        with postgres_connection.cursor() as cursor:
                            cursor.execute(insert_sql, [
                                _jsonb_projection(values[name]) if name == "extra" else values[name]
                                for name in columns
                            ])
                            returned = dict(zip(columns, cursor.fetchone()))
                            expected = dict(values)
                            expected["extra"] = json.loads(_jsonb_projection(extra))
                            if _content_hash(_postgres_value(returned)) != _content_hash(_postgres_value(expected)):
                                raise MigrationError(f"PostgreSQL contributor field verification failed for edge {edge_id}")
                            target_hash = _content_hash(_postgres_value(expected))
                            cursor.execute(
                                "INSERT INTO bibliotheca_relational_imported_entities "
                                "(run_id, entity_type, entity_id, source_sha256, target_sha256) "
                                "VALUES (%s, 'book_contributors', %s, %s, %s)",
                                (run_id, str(edge_id), str(source_hash), target_hash),
                            )
                        digest.add(str(source_hash))
                        count += 1

            with postgres_connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM bibliotheca_relational_imported_entities "
                    "WHERE run_id = %s AND entity_type = 'book_contributors'",
                    (run_id,),
                )
                provenance_count = int(cursor.fetchone()[0])
                cursor.execute("SELECT count(*) FROM book_contributors")
                target_count = int(cursor.fetchone()[0])
                if provenance_count != count or target_count != count:
                    raise MigrationError("PostgreSQL contributor projection row-count verification failed")
                manifest = {"count": count, "sha256": digest.hexdigest(), "source_relationships": list(relationship_names)}
                cursor.execute(
                    "UPDATE bibliotheca_relational_imports SET status = 'verified', manifest = %s::jsonb, verified_at = now() "
                    "WHERE run_id = %s AND target_table = 'book_contributors'",
                    (_canonical_manifest_json(manifest), run_id),
                )
            return manifest
    except Exception as exc:
        if isinstance(exc, MigrationError):
            raise
        logger.exception("Normalized PostgreSQL contributor import failed; graph snapshot was retained")
        raise MigrationError("Normalized PostgreSQL contributor import failed; transaction rolled back") from exc


def import_verified_book_relationship_edges(postgres_connection: Any, run_id: str) -> dict[str, Any]:
    """Project every Book-incident source edge as a typed PostgreSQL row.

    This relation preserves parallel edges and arbitrary Kuzu relationship
    properties while indexing book adjacency for PostgreSQL-backed services.
    The source edge identifier is retained as the primary key.
    """
    try:
        with postgres_connection.transaction():
            with postgres_connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('mybibliotheca:relational:book-edges'))")
                cursor.execute("SELECT status FROM bibliotheca_graph_imports WHERE run_id = %s FOR SHARE", (run_id,))
                graph_run = cursor.fetchone()
                if not graph_run or graph_run[0] != "verified":
                    raise MigrationError(f"PostgreSQL graph snapshot {run_id} is missing or not verified")
                cursor.execute(
                    "SELECT status FROM bibliotheca_relational_imports "
                    "WHERE run_id = %s AND target_table = 'books'",
                    (run_id,),
                )
                books_run = cursor.fetchone()
                if not books_run or books_run[0] != "verified":
                    raise MigrationError("PostgreSQL books must be imported before book relationship edges")
                cursor.execute(
                    "SELECT status, manifest FROM bibliotheca_relational_imports "
                    "WHERE run_id = %s AND target_table = 'book_relationship_edges' FOR UPDATE",
                    (run_id,),
                )
                prior = cursor.fetchone()
                if prior and prior[0] == "verified":
                    prior_manifest = json.loads(prior[1]) if isinstance(prior[1], str) else prior[1]
                    expected_count = int(prior_manifest.get("edge_count", -1))
                    cursor.execute(
                        "SELECT count(*) FROM bibliotheca_relational_imported_entities e "
                        "JOIN book_relationship_edges b ON b.source_edge_id::text = e.entity_id "
                        "WHERE e.run_id = %s AND e.entity_type = 'book_relationship_edges'",
                        (run_id,),
                    )
                    present_count = int(cursor.fetchone()[0])
                    cursor.execute(
                        "SELECT count(*) FROM bibliotheca_relational_imported_entities "
                        "WHERE run_id = %s AND entity_type = 'book_relationship_edges'",
                        (run_id,),
                    )
                    provenance_count = int(cursor.fetchone()[0])
                    cursor.execute("SELECT count(*) FROM book_relationship_edges")
                    target_count = int(cursor.fetchone()[0])
                    if expected_count != provenance_count or provenance_count != present_count or target_count != present_count:
                        raise MigrationError("Previously imported PostgreSQL book relationship edges failed retry verification")
                    cursor.execute(
                        "SELECT e.source_sha256, e.target_sha256, b.source_edge_id, b.relationship, "
                        "b.source_label, b.source_id, b.target_label, b.target_id, b.properties, b.content_sha256 "
                        "FROM bibliotheca_relational_imported_entities e "
                        "JOIN book_relationship_edges b ON b.source_edge_id::text = e.entity_id "
                        "WHERE e.run_id = %s AND e.entity_type = 'book_relationship_edges' ORDER BY b.source_edge_id",
                        (run_id,),
                    )
                    for source_hash, target_hash, edge_id, relationship, source_label, source_id, target_label, target_id, stored_properties, content_hash in cursor.fetchall():
                        computed = _stored_edge_hash(
                            str(relationship), str(source_label), str(source_id),
                            str(target_label), str(target_id), stored_properties,
                        )
                        if str(source_hash) != str(content_hash) or str(target_hash) != computed or str(content_hash) != computed:
                            raise MigrationError(f"Previously imported PostgreSQL book edge {edge_id} failed content verification")
                    return prior_manifest
                cursor.execute("SELECT count(*) FROM book_relationship_edges")
                if int(cursor.fetchone()[0]) != 0:
                    raise MigrationError("Refusing to import Kuzu book edges into a non-empty PostgreSQL table")
                cursor.execute(
                    "INSERT INTO bibliotheca_relational_imports(run_id, target_table, status, manifest) "
                    "VALUES (%s, 'book_relationship_edges', 'copying', '{}'::jsonb) "
                    "ON CONFLICT (run_id, target_table) DO UPDATE SET status = 'copying', manifest = '{}'::jsonb",
                    (run_id,),
                )
                cursor.execute(
                    "DELETE FROM bibliotheca_relational_imported_entities "
                    "WHERE run_id = %s AND entity_type = 'book_relationship_edges'",
                    (run_id,),
                )

            digest = _DigestAccumulator()
            counts: dict[str, int] = {}
            with postgres_connection.cursor(name=f"import_book_edges_{uuid.uuid4().hex}") as source_cursor:
                source_cursor.execute(
                    "SELECT edge_id, relationship, source_label, source_id, target_label, target_id, properties, content_sha256 "
                    "FROM app_graph_edges WHERE snapshot_run_id = %s "
                    "AND (source_label = 'Book' OR target_label = 'Book') ORDER BY edge_id",
                    (run_id,),
                )
                while True:
                    batch = source_cursor.fetchmany(500)
                    if not batch:
                        break
                    for edge_id, relationship, source_label, source_id, target_label, target_id, stored_properties, source_hash in batch:
                        properties = decode_migration_value(stored_properties)
                        if not isinstance(properties, dict):
                            raise MigrationError(f"Book relationship edge {edge_id} properties are not a map")
                        book_id = str(source_id) if source_label == "Book" else str(target_id)
                        if source_label != "Book" and target_label != "Book":
                            raise MigrationError(f"Book relationship edge {edge_id} has no Book endpoint")
                        with postgres_connection.cursor() as cursor:
                            cursor.execute(
                                "INSERT INTO book_relationship_edges "
                                "(source_edge_id, relationship, source_label, source_id, target_label, target_id, book_id, properties, content_sha256) "
                                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s) "
                                "RETURNING source_edge_id, relationship, source_label, source_id, target_label, target_id, book_id, properties, content_sha256",
                                (int(edge_id), str(relationship), str(source_label), str(source_id), str(target_label), str(target_id), book_id, _canonical_json(properties), str(source_hash)),
                            )
                            returned = cursor.fetchone()
                            computed = _stored_edge_hash(
                                str(returned[1]), str(returned[2]), str(returned[3]),
                                str(returned[4]), str(returned[5]), returned[7],
                            )
                            if (
                                int(returned[0]) != int(edge_id)
                                or str(returned[6]) != book_id
                                or computed != str(source_hash)
                                or str(returned[8]) != str(source_hash)
                            ):
                                raise MigrationError(f"PostgreSQL book edge verification failed for source edge {edge_id}")
                            cursor.execute(
                                "INSERT INTO bibliotheca_relational_imported_entities "
                                "(run_id, entity_type, entity_id, source_sha256, target_sha256) "
                                "VALUES (%s, 'book_relationship_edges', %s, %s, %s)",
                                (run_id, str(edge_id), str(source_hash), computed),
                            )
                        digest.add(str(source_hash))
                        counts[str(relationship)] = counts.get(str(relationship), 0) + 1

            with postgres_connection.cursor() as cursor:
                cursor.execute(
                    "SELECT count(*) FROM bibliotheca_relational_imported_entities "
                    "WHERE run_id = %s AND entity_type = 'book_relationship_edges'",
                    (run_id,),
                )
                provenance_count = int(cursor.fetchone()[0])
                cursor.execute("SELECT count(*) FROM book_relationship_edges")
                target_count = int(cursor.fetchone()[0])
                if provenance_count != digest.count or target_count != digest.count:
                    raise MigrationError("PostgreSQL book relationship edge projection count verification failed")
                manifest = {"edge_count": digest.count, "sha256": digest.hexdigest(), "relationship_counts": counts}
                cursor.execute(
                    "UPDATE bibliotheca_relational_imports SET status = 'verified', manifest = %s::jsonb, verified_at = now() "
                    "WHERE run_id = %s AND target_table = 'book_relationship_edges'",
                    (_canonical_manifest_json(manifest), run_id),
                )
            return manifest
    except Exception as exc:
        if isinstance(exc, MigrationError):
            raise
        logger.exception("Normalized PostgreSQL book relationship edge import failed; graph snapshot was retained")
        raise MigrationError("Normalized PostgreSQL book relationship edge import failed; transaction rolled back") from exc


def audit_relational_projection(postgres_connection: Any, run_id: str) -> dict[str, Any]:
    """Audit normalized projection coverage without mutating either database.

    This is intended to be the final data gate used by the first-upgrade
    migration flow. Every source node needs a matching typed projection.
    Application-critical relationships also need typed projections; legacy
    relationships without an active service are accepted only when the exact
    edge archive passes its count and content-hash checks.
    """
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            "SELECT status, manifest FROM bibliotheca_migration_runs WHERE run_id = %s",
            (run_id,),
        )
        source_run = cursor.fetchone()
        cursor.execute(
            "SELECT status, manifest FROM bibliotheca_graph_imports WHERE run_id = %s",
            (run_id,),
        )
        graph_run = cursor.fetchone()
        if not source_run or source_run[0] != "verified":
            raise MigrationError(f"PostgreSQL Kuzu archive {run_id} is missing or not verified")
        if not graph_run or graph_run[0] != "verified":
            raise MigrationError(f"PostgreSQL graph snapshot {run_id} is missing or not verified")
        source_manifest = json.loads(source_run[1]) if isinstance(source_run[1], str) else source_run[1]
        graph_manifest = json.loads(graph_run[1]) if isinstance(graph_run[1], str) else graph_run[1]
        cursor.execute(
            "SELECT target_table, status, manifest FROM bibliotheca_relational_imports WHERE run_id = %s",
            (run_id,),
        )
        imports = {
            table: (status, json.loads(manifest) if isinstance(manifest, str) else manifest)
            for table, status, manifest in cursor.fetchall()
        }

        node_tables = {"User": "users", "Book": "books", "ReadingLog": "reading_logs"}
        node_tables.update({label: spec["table"] for label, spec in _REFERENCE_ENTITY_IMPORTS.items()})
        source_nodes = source_manifest.get("nodes", {})
        node_report: dict[str, Any] = {}
        blockers: list[str] = []
        preservation_blockers: list[str] = []
        for label, expected_info in sorted(source_nodes.items()):
            expected_count = int(expected_info.get("count", -1))
            expected_hash = str(expected_info.get("sha256", ""))
            graph_digest = _DigestAccumulator()
            with postgres_connection.cursor(name=f"audit_graph_nodes_{uuid.uuid4().hex}") as graph_cursor:
                graph_cursor.execute(
                    "SELECT node_id, properties, content_sha256 FROM app_graph_nodes "
                    "WHERE snapshot_run_id = %s AND label = %s ORDER BY node_id",
                    (run_id, label),
                )
                for node_id, properties, stored_hash in graph_cursor:
                    actual_hash = _stored_content_hash(properties)
                    if actual_hash != str(stored_hash):
                        graph_digest.add("0" * 64)
                    else:
                        graph_digest.add(actual_hash)
            graph_verified = graph_digest.count == expected_count and graph_digest.hexdigest() == expected_hash
            if not graph_verified:
                preservation_blockers.append(f"archived PostgreSQL nodes for {label} differ from the Kuzu snapshot")
            table = node_tables.get(label)
            if not table:
                node_report[label] = {
                    "source_count": expected_count,
                    "graph_count": graph_digest.count,
                    "graph_verified": graph_verified,
                    "status": "graph_only" if graph_verified else "archive_mismatch",
                }
                blockers.append(f"node label {label} has no normalized PostgreSQL projection")
                continue
            imported = imports.get(table)
            if not imported or imported[0] != "verified":
                node_report[label] = {
                    "table": table,
                    "source_count": expected_count,
                    "graph_count": graph_digest.count,
                    "graph_verified": graph_verified,
                    "status": "not_verified",
                }
                blockers.append(f"node label {label} has no verified {table} projection")
                continue
            projection_manifest = imported[1]
            projected_count = int(projection_manifest.get("count", -1))
            cursor.execute(f"SELECT count(*) FROM {table}")
            target_count = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT count(*) FROM bibliotheca_relational_imported_entities "
                "WHERE run_id = %s AND entity_type = %s",
                (run_id, table),
            )
            provenance_count = int(cursor.fetchone()[0])
            okay = graph_verified and expected_count == projected_count == target_count == provenance_count
            if label == "User":
                target_columns = (*_USER_IMPORT_COLUMNS, "extra")
            elif label == "Book":
                target_columns = (*_BOOK_IMPORT_COLUMNS, "extra")
            elif label == "ReadingLog":
                target_columns = ("id", "user_id", "book_id", "date", "pages_read", "minutes_read", "notes", "created_at", "updated_at", "extra")
            else:
                target_columns = ("id", *tuple(_REFERENCE_ENTITY_IMPORTS[label]["columns"]), "extra")
            content_verified = True
            with postgres_connection.cursor(name=f"audit_{table}_{uuid.uuid4().hex}") as verify_cursor:
                verify_cursor.execute(
                    "SELECT e.source_sha256, n.content_sha256, e.target_sha256, " +
                    ", ".join(f"t.{name}" for name in target_columns) +
                    " FROM bibliotheca_relational_imported_entities e "
                    f"JOIN {table} t ON t.id = e.entity_id "
                    "JOIN app_graph_nodes n ON n.snapshot_run_id = %s AND n.label = %s AND n.node_id = e.entity_id "
                    "WHERE e.run_id = %s AND e.entity_type = %s ORDER BY e.entity_id",
                    (run_id, label, run_id, table),
                )
                for source_hash, archived_hash, target_hash, *row in verify_cursor:
                    if (
                        str(source_hash) != str(archived_hash)
                        or not target_hash
                        or _content_hash(_postgres_value(dict(zip(target_columns, row)))) != str(target_hash)
                    ):
                        content_verified = False
                        break
            okay = okay and content_verified
            node_report[label] = {
                "table": table,
                "source_count": expected_count,
                "projected_count": projected_count,
                "target_count": target_count,
                "provenance_count": provenance_count,
                "graph_count": graph_digest.count,
                "graph_verified": graph_verified,
                "content_verified": content_verified,
                "status": "verified" if okay else "count_mismatch",
            }
            if not okay:
                blockers.append(f"node label {label} source/projection/target counts or content do not match")

        source_relationships = source_manifest.get("relationships", {})
        source_schema_relationships = source_manifest.get("schema", {}).get("relationships", {})
        relationship_report: dict[str, Any] = {}
        book_edge_import = imports.get("book_relationship_edges")
        book_edge_counts = (
            book_edge_import[1].get("relationship_counts", {})
            if book_edge_import and book_edge_import[0] == "verified" else {}
        )
        book_edge_integrity = False
        if book_edge_import and book_edge_import[0] == "verified":
            expected_book_edges = int(book_edge_import[1].get("edge_count", -1))
            cursor.execute("SELECT count(*) FROM book_relationship_edges")
            target_book_edges = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT count(*) FROM bibliotheca_relational_imported_entities "
                "WHERE run_id = %s AND entity_type = 'book_relationship_edges'",
                (run_id,),
            )
            provenance_book_edges = int(cursor.fetchone()[0])
            book_edge_integrity = expected_book_edges == target_book_edges == provenance_book_edges
            if book_edge_integrity:
                with postgres_connection.cursor(name=f"audit_book_edges_{uuid.uuid4().hex}") as edge_cursor:
                    edge_cursor.execute(
                        "SELECT p.source_sha256, p.target_sha256, g.relationship, g.source_label, g.source_id, "
                        "g.target_label, g.target_id, g.properties, g.content_sha256, "
                        "b.relationship, b.source_label, b.source_id, b.target_label, b.target_id, b.book_id, "
                        "b.properties, b.content_sha256 "
                        "FROM bibliotheca_relational_imported_entities p "
                        "JOIN book_relationship_edges b ON b.source_edge_id::text = p.entity_id "
                        "JOIN app_graph_edges g ON g.snapshot_run_id = %s AND g.edge_id = b.source_edge_id "
                        "WHERE p.run_id = %s AND p.entity_type = 'book_relationship_edges' ORDER BY b.source_edge_id",
                        (run_id, run_id),
                    )
                    verified_edge_count = 0
                    for source_hash, target_hash, relationship, source_label, source_id, target_label, target_id, source_properties, content_hash, projected_relationship, projected_source_label, projected_source_id, projected_target_label, projected_target_id, projected_book_id, projected_properties, projected_content_hash in edge_cursor:
                        source_content_hash = _stored_edge_hash(
                            str(relationship), str(source_label), str(source_id),
                            str(target_label), str(target_id), source_properties,
                        )
                        target_content_hash = _stored_edge_hash(
                            str(projected_relationship), str(projected_source_label), str(projected_source_id),
                            str(projected_target_label), str(projected_target_id), projected_properties,
                        )
                        expected_book_id = str(source_id) if source_label == "Book" else str(target_id)
                        if (
                            str(source_hash) != str(content_hash)
                            or str(source_hash) != source_content_hash
                            or str(target_hash) != target_content_hash
                            or str(projected_content_hash) != target_content_hash
                            or (str(projected_relationship), str(projected_source_label), str(projected_source_id), str(projected_target_label), str(projected_target_id))
                            != (str(relationship), str(source_label), str(source_id), str(target_label), str(target_id))
                            or str(projected_book_id) != expected_book_id
                        ):
                            book_edge_integrity = False
                            break
                        verified_edge_count += 1
                    book_edge_integrity = book_edge_integrity and verified_edge_count == expected_book_edges
        contributor_names = {"WRITTEN_BY", "AUTHORED", "EDITED", "ILLUSTRATED", "TRANSLATED", "NARRATED", "CONTRIBUTED"}
        for name, endpoint in sorted(source_schema_relationships.items()):
            info = source_relationships.get(name, {})
            expected_count = int(info.get("count", -1))
            expected_hash = str(info.get("sha256", ""))
            graph_edge_digest = _DigestAccumulator()
            with postgres_connection.cursor(name=f"audit_graph_edges_{uuid.uuid4().hex}") as graph_cursor:
                graph_cursor.execute(
                    "SELECT source_label, source_id, target_label, target_id, properties, content_sha256 "
                    "FROM app_graph_edges WHERE snapshot_run_id = %s AND relationship = %s ORDER BY edge_id",
                    (run_id, name),
                )
                for source_label, source_id, target_label, target_id, properties, stored_hash in graph_cursor:
                    actual_hash = _stored_edge_hash(
                        name, str(source_label), str(source_id), str(target_label), str(target_id), properties
                    )
                    graph_edge_digest.add(actual_hash if actual_hash == str(stored_hash) else "0" * 64)
            graph_verified = graph_edge_digest.count == expected_count and graph_edge_digest.hexdigest() == expected_hash
            if not graph_verified:
                preservation_blockers.append(f"archived PostgreSQL edges for {name} differ from the Kuzu snapshot")
            if tuple(endpoint) == ("User", "Book"):
                projection_table = "user_books"
                import_info = imports.get(projection_table)
                actual_count = int(import_info[1].get("source_edge_count", -1)) if import_info and import_info[0] == "verified" else -1
                group_expected = sum(
                    int(source_relationships.get(other, {}).get("count", 0))
                    for other, other_endpoint in source_schema_relationships.items()
                    if tuple(other_endpoint) == ("User", "Book")
                )
                # Empty Kuzu relationship tables are present in the source
                # schema and have a verified source count of zero, but they
                # do not get a key in the importer's positive-count map.
                # Treat an absent map entry as zero; any positive source
                # count still fails closed if it was not projected.
                book_edge_count = int(book_edge_counts.get(name, 0))
                okay = (
                    import_info is not None and import_info[0] == "verified"
                    and actual_count == group_expected and book_edge_count == expected_count
                    and book_edge_integrity and graph_verified
                )
                relationship_report[name] = {
                    "source_count": expected_count, "projection": projection_table,
                    "group_source_count": group_expected, "projected_edge_count": actual_count,
                    "book_edge_projection_count": book_edge_count,
                    "graph_verified": graph_verified,
                    "status": "verified" if okay else "not_verified_or_mismatched",
                }
            elif name in contributor_names:
                projection_table = "book_contributors"
                import_info = imports.get(projection_table)
                actual_count = int(import_info[1].get("count", -1)) if import_info and import_info[0] == "verified" else -1
                group_expected = sum(int(source_relationships.get(other, {}).get("count", 0)) for other in contributor_names)
                book_edge_count = int(book_edge_counts.get(name, 0))
                okay = (
                    import_info is not None and import_info[0] == "verified"
                    and actual_count == group_expected and book_edge_count == expected_count
                    and book_edge_integrity and graph_verified
                )
                relationship_report[name] = {
                    "source_count": expected_count, "projection": projection_table,
                    "group_source_count": group_expected, "projected_edge_count": actual_count,
                    "book_edge_projection_count": book_edge_count,
                    "graph_verified": graph_verified,
                    "status": "verified" if okay else "not_verified_or_mismatched",
                }
            elif name in {"LOGGED", "FOR_BOOK"}:
                projection_table = "reading_logs"
                import_info = imports.get(projection_table)
                actual_count = int(import_info[1].get("relationship_count", -1)) if import_info and import_info[0] == "verified" else -1
                group_expected = sum(int(source_relationships.get(other, {}).get("count", 0)) for other in ("LOGGED", "FOR_BOOK"))
                okay = import_info is not None and import_info[0] == "verified" and actual_count == group_expected and graph_verified
                relationship_report[name] = {
                    "source_count": expected_count, "projection": projection_table,
                    "group_source_count": group_expected, "projected_edge_count": actual_count,
                    "graph_verified": graph_verified,
                    "status": "verified" if okay else "not_verified_or_mismatched",
                }
                if name == "FOR_BOOK":
                    book_edge_count = int(book_edge_counts.get(name, 0))
                    okay = okay and book_edge_count == expected_count and book_edge_integrity
                    relationship_report[name]["book_edge_projection_count"] = book_edge_count
                    relationship_report[name]["status"] = "verified" if okay else "not_verified_or_mismatched"
            elif name == "HAS_CUSTOM_FIELD" and tuple(endpoint) == ("User", "CustomField"):
                projection_table = "custom_fields"
                import_info = imports.get(projection_table)
                actual_count = int(import_info[1].get("edge_count", -1)) if import_info and import_info[0] == "verified" else -1
                prefix = f"kuzu-edge:{run_id}:"
                cursor.execute("SELECT count(*) FROM custom_fields WHERE id LIKE %s", (prefix + "%",))
                target_count = int(cursor.fetchone()[0])
                cursor.execute(
                    "SELECT count(*) FROM bibliotheca_relational_imported_entities "
                    "WHERE run_id=%s AND entity_type='custom_fields'", (run_id,),
                )
                provenance_count = int(cursor.fetchone()[0])
                integrity = actual_count == target_count == provenance_count == expected_count
                verified_count = 0
                if integrity:
                    with postgres_connection.cursor(name=f"audit_custom_field_edges_{uuid.uuid4().hex}") as value_cursor:
                        value_cursor.execute(
                            "SELECT e.entity_id,e.source_sha256,e.target_sha256,g.content_sha256,g.relationship,"
                            "g.source_label,g.source_id,g.target_label,g.target_id,g.properties,d.properties,"
                            "f.id,f.user_id,f.definition_id,f.book_id,f.field_name,f.field_value,"
                            "f.is_required,f.is_active,f.created_at,f.updated_at,f.extra "
                            "FROM bibliotheca_relational_imported_entities e "
                            "JOIN app_graph_edges g ON g.snapshot_run_id=e.run_id AND g.edge_id=e.entity_id::bigint "
                            "JOIN app_graph_nodes d ON d.snapshot_run_id=e.run_id AND d.label='CustomField' AND d.node_id=g.target_id "
                            "JOIN custom_fields f ON f.id=%s || e.entity_id "
                            "WHERE e.run_id=%s AND e.entity_type='custom_fields' ORDER BY g.edge_id",
                            (prefix, run_id),
                        )
                        for row in value_cursor:
                            (entity_id, source_hash, target_hash, edge_hash, relationship,
                             source_label, source_id, target_label, target_id, edge_properties, definition_raw,
                             field_id, user_id, definition_id, book_id, field_name, field_value,
                             is_required, is_active, created_at, updated_at, extra) = row
                            properties = decode_migration_value(edge_properties)
                            definition_properties = decode_migration_value(definition_raw)
                            source_content_hash = _stored_edge_hash(
                                str(relationship), str(source_label), str(source_id),
                                str(target_label), str(target_id), edge_properties,
                            )
                            actual_target_hash = _content_hash(_postgres_value(dict(zip(_CUSTOM_FIELD_PROJECTION_COLUMNS, (
                                field_id, user_id, definition_id, book_id, field_name, field_value,
                                is_required, is_active, created_at, updated_at, extra,
                            )))))
                            edge_matches = (
                                isinstance(properties, dict) and isinstance(definition_properties, dict)
                                and str(entity_id) == str(int(entity_id))
                                and str(field_id) == prefix + str(entity_id)
                                and str(source_id) == str(user_id)
                                and str(target_id) == str(definition_id)
                                and (str(book_id) if book_id is not None else None)
                                    == (str(properties.get("book_id")) if properties.get("book_id") else None)
                                and field_name == (properties.get("field_name") or definition_properties.get("name"))
                                and field_value == properties.get("field_value")
                                and is_required == definition_properties.get("is_required")
                                and is_active == definition_properties.get("is_active")
                                and str(source_hash) == str(edge_hash) == source_content_hash
                                and str(target_hash) == actual_target_hash
                            )
                            if not edge_matches:
                                integrity = False
                                break
                            verified_count += 1
                okay = bool(import_info and import_info[0] == "verified" and integrity and verified_count == expected_count and graph_verified)
                relationship_report[name] = {
                    "source_count": expected_count, "projection": projection_table,
                    "projected_edge_count": actual_count, "projection_integrity": integrity,
                    "graph_verified": graph_verified,
                    "status": "verified" if okay else "not_verified_or_mismatched",
                }
            elif "Book" in tuple(endpoint):
                book_edge_count = int(book_edge_counts.get(name, 0))
                okay = (
                    book_edge_import is not None
                    and book_edge_import[0] == "verified"
                    and book_edge_count == expected_count
                    and book_edge_integrity
                    and graph_verified
                )
                relationship_report[name] = {
                    "source_count": expected_count,
                    "projection": "book_relationship_edges",
                    "projected_edge_count": book_edge_count,
                    "graph_verified": graph_verified,
                    "status": "verified" if okay else "not_verified_or_mismatched",
                }
            else:
                relationship_report[name] = {
                    "source_count": expected_count,
                    "projection": "app_graph_edges",
                    "graph_verified": graph_verified,
                    "archive_only": True,
                    "status": "verified" if graph_verified else "archive_mismatch",
                }
            if relationship_report[name]["status"] != "verified" and name not in blockers:
                if not any(f"relationship {name} " in item for item in blockers):
                    details = relationship_report[name]
                    edge_count = details.get("book_edge_projection_count", details.get("projected_edge_count", "unknown"))
                    blockers.append(
                        f"relationship {name} projection is missing or its edge count does not match "
                        f"(source_count={details.get('source_count', 'unknown')}, book_edge_count={edge_count})"
                    )

    return {
        "run_id": run_id,
        "archive_verified": True,
        "graph_snapshot_verified": True,
        "node_projections": node_report,
        "relationship_projections": relationship_report,
        "blockers": blockers,
        "preservation_blockers": preservation_blockers,
        "preservation_complete": not preservation_blockers,
        "projection_complete": not blockers,
        # Data equality alone does not establish a safe application cutover.
        # A cutover is allowed only when every discovered source node and edge
        # is preserved, all required typed projections pass, and no static
        # application compatibility blocker has been registered. Unserved
        # legacy relationships are explicitly identified as archive-only.
        "cutover_blockers": list(APPLICATION_CUTOVER_BLOCKERS),
        "cutover_allowed": not preservation_blockers and not blockers and not APPLICATION_CUTOVER_BLOCKERS,
    }


def migrate_and_project_kuzu(
    kuzu_connection: Any,
    postgres_connection: Any,
    *,
    quiescence_lease: _SourceQuiescenceLease,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Run or resume the verified archive and normalized projection pipeline.

    ``run_id`` makes a partially completed relational import resumable after
    restart. A new archive snapshot always requires an explicitly quiesced
    Kuzu source. The function deliberately never switches application traffic;
    it returns a completeness audit. The caller may enable PostgreSQL only
    when preservation, required typed-projection, and application compatibility
    checks all pass; this function never switches live application traffic.
    """
    source_manifest: dict[str, Any] | None = None
    if run_id is not None:
        with postgres_connection.cursor() as cursor:
            cursor.execute(
                "SELECT status, manifest FROM bibliotheca_migration_runs WHERE run_id = %s",
                (run_id,),
            )
            existing = cursor.fetchone()
        if existing:
            if existing[0] != "verified":
                raise MigrationError(f"Existing Kuzu migration run {run_id} is not verified")
            source_manifest = json.loads(existing[1]) if isinstance(existing[1], str) else existing[1]

    if source_manifest is None:
        source_manifest = migrate_kuzu_to_postgres(
            kuzu_connection,
            postgres_connection,
            quiescence_lease=quiescence_lease,
            run_id=run_id,
        )
        run_id = str(source_manifest["run_id"])

    run_id = str(run_id or source_manifest["run_id"])
    graph_manifest = import_verified_graph_snapshot(postgres_connection, run_id)
    projected: dict[str, Any] = {}
    projected["users"] = import_verified_users(postgres_connection, run_id)
    projected["books"] = import_verified_books(postgres_connection, run_id)
    projected["reference_entities"] = import_verified_reference_entities(postgres_connection, run_id)
    projected["custom_fields"] = import_verified_custom_field_edges(postgres_connection, run_id)
    projected["user_books"] = import_verified_user_books(postgres_connection, run_id)
    projected["reading_logs"] = import_verified_reading_logs(postgres_connection, run_id)
    projected["book_contributors"] = import_verified_book_contributors(postgres_connection, run_id)
    projected["book_relationship_edges"] = import_verified_book_relationship_edges(postgres_connection, run_id)
    audit = audit_relational_projection(postgres_connection, run_id)
    return {
        "run_id": run_id,
        "source_manifest": source_manifest,
        "graph_manifest": graph_manifest,
        "projections": projected,
        "audit": audit,
        "cutover_allowed": bool(audit.get("cutover_allowed")),
    }


def migrate_kuzu_manager_to_postgres(
    kuzu_manager: Any,
    postgres_store: Any,
    *,
    quiesce_timeout_seconds: float = 300.0,
    on_verified: Any | None = None,
    require_cutover_ready: bool = False,
) -> dict[str, Any]:
    """Take a fresh snapshot while the shared Kuzu manager blocks other work.

    This is the application entry point for a migration attempt. It acquires
    its own source connection, closes the race between snapshotting and live
    writers by pausing new Kuzu connections, waits for every other active
    operation to drain, and only then issues a short-lived quiescence lease to
    the lower-level importer. The source remains intact if any PostgreSQL
    step fails.

    A fresh run is always taken. Callers must not resume an old archive after
    the Kuzu application has been allowed to accept writes again, since that
    could omit writes made since the archived snapshot.
    """
    if require_cutover_ready and on_verified is None:
        raise ValueError("A production cutover requires an activation callback")
    postgres_store.initialize_schema()
    timeout = max(1.0, float(quiesce_timeout_seconds))
    with kuzu_manager.get_connection(operation="postgres_migration_snapshot") as kuzu_connection:
        with kuzu_manager.quiesce_for_backup(
            reason="postgres_migration_snapshot",
            allowed_active_connections=1,
            timeout_seconds=timeout,
        ):
            lease = _SourceQuiescenceLease(kuzu_manager, kuzu_connection)
            # The archive and every normalized projection commit together.
            # Nested transaction contexts in the import stages become
            # savepoints, so a crash or validation exception cannot leave a
            # half-projected Postgres target to be mistaken for a complete
            # migration on the next start.
            pending_written = False
            transaction_committed = False
            try:
                with postgres_store.transaction() as postgres_connection:
                    if require_cutover_ready:
                        # The relational projection uses global application
                        # tables. Lock and verify an empty target in this same
                        # transaction so it cannot overwrite a populated PG
                        # install or race an external writer after preflight.
                        postgres_store.assert_migration_target_empty(
                            postgres_connection, lock=True,
                        )
                    result = migrate_and_project_kuzu(
                        kuzu_connection,
                        postgres_connection,
                        quiescence_lease=lease,
                    )
                    if require_cutover_ready:
                        audit = result.get("audit") or {}
                        if (
                            not audit.get("preservation_complete")
                            or not audit.get("projection_complete")
                            or audit.get("cutover_blockers")
                            or not audit.get("cutover_allowed")
                        ):
                            raise MigrationNotReady(audit)
                        from .persistence_state import write_pending_cutover
                        write_pending_cutover(str(result["run_id"]))
                        pending_written = True
                transaction_committed = True
            except Exception:
                if pending_written and not transaction_committed:
                    from .persistence_state import clear_pending_cutover
                    clear_pending_cutover(str(result.get("run_id") if "result" in locals() else ""))
                raise
            finally:
                lease.close()
            if on_verified is not None:
                # If this callback fails after the verified PostgreSQL commit,
                # leave the durable pending marker in place and keep Kuzu
                # quiesced. Startup recovery finishes or refuses the switch.
                on_verified(result)
            if pending_written:
                from .persistence_state import clear_pending_cutover
                clear_pending_cutover(str(result["run_id"]))
            return result
