import uuid
import json
from contextlib import contextmanager
import threading
from datetime import date, datetime, timezone
from decimal import Decimal

import kuzu
import pytest

from app.infrastructure import postgres_migration
from app.infrastructure.postgres_migration import (
    MigrationError,
    _fetch_edges,
    _fetch_nodes,
    discover_kuzu_schema,
    migrate_kuzu_to_postgres,
    _node_properties,
    _canonical_json,
    decode_migration_value,
    _normalize_user_projection,
    _normalize_book_projection,
    _merge_user_book_edges,
)


def test_manager_migration_uses_real_quiesce_and_one_postgres_transaction(monkeypatch):
    events = []
    source_connection = object()
    target_connection = object()

    class Manager:
        def __init__(self):
            self._lock = threading.RLock()
            self._writes_quiesced = False
            self._pending_quiesce_reason = None

        @contextmanager
        def get_connection(self, *, operation):
            events.append(("connection", operation))
            yield source_connection

        @contextmanager
        def quiesce_for_backup(self, *, reason, allowed_active_connections, timeout_seconds):
            events.append(("quiesce_start", reason, allowed_active_connections, timeout_seconds))
            self._writes_quiesced = True
            self._pending_quiesce_reason = reason
            try:
                yield
            finally:
                self._writes_quiesced = False
                self._pending_quiesce_reason = None
                events.append(("quiesce_end", reason))

    class Store:
        def initialize_schema(self):
            events.append(("initialize_schema",))

        def assert_migration_target_empty(self, connection, *, lock):
            events.append(("assert_migration_target_empty", connection, lock))

        @contextmanager
        def transaction(self):
            events.append(("transaction_start",))
            yield target_connection
            events.append(("transaction_commit",))

    expected = {"run_id": "migration-1", "cutover_allowed": False}

    def run_migration(source, target, *, quiescence_lease):
        quiescence_lease.validate(source)
        events.append(("migration", source, target, True))
        return expected

    monkeypatch.setattr(postgres_migration, "migrate_and_project_kuzu", run_migration)
    result = postgres_migration.migrate_kuzu_manager_to_postgres(
        Manager(), Store(), quiesce_timeout_seconds=12,
    )

    assert result is expected
    assert events == [
        ("initialize_schema",),
        ("connection", "postgres_migration_snapshot"),
        ("quiesce_start", "postgres_migration_snapshot", 1, 12.0),
        ("transaction_start",),
        ("migration", source_connection, target_connection, True),
        ("transaction_commit",),
        ("quiesce_end", "postgres_migration_snapshot"),
    ]


def test_failed_projection_rolls_back_without_pending_cutover_or_backend_switch(monkeypatch, tmp_path):
    events = []
    state_path = tmp_path / "persistence_state.json"
    monkeypatch.setenv("MYBIBLIOTHECA_PERSISTENCE_STATE_PATH", str(state_path))

    class Manager:
        def __init__(self):
            self._lock = threading.RLock()
            self._writes_quiesced = False
            self._pending_quiesce_reason = None

        @contextmanager
        def get_connection(self, *, operation):
            events.append(("connection", operation))
            yield object()

        @contextmanager
        def quiesce_for_backup(self, *, reason, allowed_active_connections, timeout_seconds):
            self._writes_quiesced = True
            self._pending_quiesce_reason = reason
            events.append(("quiesce_start", reason))
            try:
                yield
            finally:
                self._writes_quiesced = False
                self._pending_quiesce_reason = None
                events.append(("quiesce_end", reason))

    class Store:
        def initialize_schema(self):
            events.append(("initialize_schema",))

        def assert_migration_target_empty(self, connection, *, lock):
            events.append(("assert_migration_target_empty", connection, lock))

        @contextmanager
        def transaction(self):
            events.append(("transaction_start",))
            try:
                yield object()
            except Exception:
                events.append(("transaction_rollback",))
                raise
            else:
                events.append(("transaction_commit",))

    def fail_projection(*_args, **_kwargs):
        raise MigrationError("fixture projection mismatch")

    monkeypatch.setattr(postgres_migration, "migrate_and_project_kuzu", fail_projection)
    manager = Manager()
    activation_calls = []

    with pytest.raises(MigrationError, match="fixture projection mismatch"):
        postgres_migration.migrate_kuzu_manager_to_postgres(
            manager,
            Store(),
            on_verified=lambda result: activation_calls.append(result),
            require_cutover_ready=True,
        )

    assert ("transaction_rollback",) in events
    assert ("transaction_commit",) not in events
    assert activation_calls == []
    assert not state_path.exists()
    assert not (tmp_path / "persistence_cutover_pending.json").exists()
    assert not manager._writes_quiesced
    assert manager._pending_quiesce_reason is None


def test_nonempty_postgres_target_aborts_before_projection_or_activation(monkeypatch, tmp_path):
    events = []
    monkeypatch.setenv("MYBIBLIOTHECA_PERSISTENCE_STATE_PATH", str(tmp_path / "state.json"))

    class Manager:
        def __init__(self):
            self._lock = threading.RLock()
            self._writes_quiesced = False
            self._pending_quiesce_reason = None

        @contextmanager
        def get_connection(self, *, operation):
            events.append(("source_connection", operation))
            yield object()

        @contextmanager
        def quiesce_for_backup(self, *, reason, allowed_active_connections, timeout_seconds):
            self._writes_quiesced = True
            self._pending_quiesce_reason = reason
            try:
                yield
            finally:
                self._writes_quiesced = False
                self._pending_quiesce_reason = None

    class Store:
        def initialize_schema(self):
            events.append(("initialize_schema",))

        @contextmanager
        def transaction(self):
            events.append(("transaction_start",))
            try:
                yield object()
            except Exception:
                events.append(("transaction_rollback",))
                raise

        def assert_migration_target_empty(self, _connection, *, lock):
            assert lock is True
            events.append(("target_not_empty",))
            raise RuntimeError("PostgreSQL migration target must be empty")

    monkeypatch.setattr(
        postgres_migration, "migrate_and_project_kuzu",
        lambda *_args, **_kwargs: pytest.fail("projection must not start for a populated target"),
    )
    with pytest.raises(RuntimeError, match="target must be empty"):
        postgres_migration.migrate_kuzu_manager_to_postgres(
            Manager(), Store(), on_verified=lambda _result: pytest.fail("must not activate"),
            require_cutover_ready=True,
        )

    assert ("target_not_empty",) in events
    assert ("transaction_rollback",) in events
    assert not (tmp_path / "state.json").exists()
    assert not (tmp_path / "persistence_cutover_pending.json").exists()


@pytest.fixture
def graph(tmp_path):
    database = kuzu.Database(str(tmp_path / "source.db"))
    connection = kuzu.Connection(database)
    connection.execute(
        "CREATE NODE TABLE User(id STRING, username STRING, extra STRING, PRIMARY KEY(id))"
    )
    connection.execute("CREATE NODE TABLE Book(id STRING, title STRING, PRIMARY KEY(id))")
    connection.execute("CREATE NODE TABLE GlobalMetadata(book_id STRING, created_at TIMESTAMP, PRIMARY KEY(book_id))")
    connection.execute("CREATE REL TABLE OWNS(FROM User TO Book, reading_status STRING)")
    connection.execute("CREATE REL TABLE HAS_GLOBAL_METADATA(FROM Book TO GlobalMetadata, global_custom_fields STRING)")
    connection.execute("CREATE (u:User {id: 'user-1', username: 'reader', extra: 'keep-me'})")
    connection.execute("CREATE (b:Book {id: 'book-1', title: 'A book'})")
    connection.execute("CREATE (gm:GlobalMetadata {book_id: 'book-1', created_at: TIMESTAMP('2026-10-19 12:00:00')})")
    connection.execute(
        "MATCH (u:User {id: 'user-1'}), (b:Book {id: 'book-1'}) "
        "CREATE (u)-[:OWNS {reading_status: 'read'}]->(b)"
    )
    connection.execute(
        "MATCH (b:Book {id: 'book-1'}), (gm:GlobalMetadata {book_id: 'book-1'}) "
        "CREATE (b)-[:HAS_GLOBAL_METADATA {global_custom_fields: '{\"edition\":\"signed\"}'}]->(gm)"
    )
    yield connection
    connection.close()
    del connection
    del database


def test_fetch_nodes_preserves_all_properties_and_strips_kuzu_internal_ids(graph):
    nodes = _fetch_nodes(graph, "User")

    assert len(nodes) == 1
    node_id, properties, _digest = nodes[0]
    assert node_id == "user-1"
    assert properties == {"id": "user-1", "username": "reader", "extra": "keep-me"}


def test_fetch_edges_preserves_endpoints_and_relationship_properties(graph):
    edges = _fetch_edges(graph, "OWNS", "User", "Book")

    assert len(edges) == 1
    source_id, target_id, properties, _digest = edges[0]
    assert (source_id, target_id) == ("user-1", "book-1")
    assert properties == {"reading_status": "read"}


def test_discovery_includes_every_node_and_relationship_table(graph):
    nodes, relationships = discover_kuzu_schema(graph)

    assert nodes == ("Book", "GlobalMetadata", "User")
    assert relationships == {
        "HAS_GLOBAL_METADATA": ("Book", "GlobalMetadata"),
        "OWNS": ("User", "Book"),
    }


def test_node_without_id_uses_its_declared_primary_key(graph):
    nodes = _fetch_nodes(graph, "GlobalMetadata")

    assert len(nodes) == 1
    assert nodes[0][0] == "book-1"
    assert nodes[0][1] == {"book_id": "book-1", "created_at": datetime(2026, 10, 19, 12, 0)}


def test_legacy_custom_field_nodes_project_as_definitions():
    spec = postgres_migration._REFERENCE_ENTITY_IMPORTS["CustomField"]

    assert spec["table"] == "custom_field_definitions"
    assert "is_global" in spec["columns"]
    assert "field_value" not in spec["columns"]


def test_global_metadata_nodes_have_a_lossless_relational_projection():
    spec = postgres_migration._REFERENCE_ENTITY_IMPORTS["GlobalMetadata"]

    assert spec["table"] == "global_metadata_records"
    assert spec["key"] == "book_id"
    assert spec["columns"] == ("book_id", "created_at", "updated_at")


def test_relationship_endpoints_use_their_declared_primary_keys(graph):
    edges = _fetch_edges(graph, "HAS_GLOBAL_METADATA", "Book", "GlobalMetadata")

    assert len(edges) == 1
    assert edges[0][0:2] == ("book-1", "book-1")
    assert edges[0][2] == {"global_custom_fields": '{"edition":"signed"}'}


def test_node_without_id_is_rejected():
    with pytest.raises(MigrationError, match="missing its id"):
        _node_properties({"_id": {"offset": 0}, "_label": "User", "username": "reader"}, "User")


def test_mismatched_label_is_rejected():
    with pytest.raises(MigrationError, match="Expected node label User"):
        _node_properties({"_label": "Book", "id": str(uuid.uuid4())}, "User")


def test_typed_archive_round_trip_preserves_binary_and_marker_shaped_maps():
    original = {
        "raw": bytes([0, 255]) + b"not-utf8",
        "date": date(2026, 10, 19),
        "timestamp": datetime(2026, 10, 19, 15, 22, tzinfo=timezone.utc),
        "decimal": Decimal("12.3400"),
        "uuid": uuid.UUID("19a342de-dade-4dd9-8b57-7f0548d1521b"),
        "user_map": {"$mybibliotheca_kuzu_value_v1": ["bytes", "not a tag"]},
    }

    encoded = json.loads(_canonical_json(original))

    assert decode_migration_value(encoded) == original


def test_user_projection_preserves_extra_fields_and_applies_nullable_defaults():
    values, extra = _normalize_user_projection(
        "u1",
        {
            "id": "u1", "username": "reader", "email": "reader@example.test",
            "password_hash": "hash", "is_active": None, "timezone": None,
            "future_preference": {"mode": "quiet"},
        },
    )

    assert values["is_active"] is True
    assert values["timezone"] == "UTC"
    assert extra == {"future_preference": {"mode": "quiet"}}


def test_user_projection_rejects_missing_credentials():
    with pytest.raises(MigrationError, match="missing required field email"):
        _normalize_user_projection("u1", {"id": "u1", "username": "reader"})


def test_book_projection_decodes_json_text_and_preserves_unknown_properties():
    values, extra = _normalize_book_projection(
        "b1",
        {
            "id": "b1", "title": "A Book", "custom_metadata": '{"edition": "first"}',
            "raw_categories": '["fiction", "history"]', "audio_narrators": '["A. Voice"]',
            "plugin_value": {"source": "legacy"},
        },
    )

    assert values["custom_metadata"] == {"edition": "first"}
    assert values["raw_categories"] == ["fiction", "history"]
    assert values["audio_narrators"] == ["A. Voice"]
    assert extra == {"plugin_value": {"source": "legacy"}}


def test_user_book_projection_merges_overlay_and_preserves_parallel_source_edges():
    values, pair_key, source_digest = _merge_user_book_edges(
        "u1",
        "b1",
        [
            (3, "HAS_PERSONAL_METADATA", {
                "personal_notes": "overlay note",
                "personal_custom_fields": '{"reading_status":"read","user_rating":5,"shelf_color":"blue"}',
            }, "c" * 64),
            (2, "OWNS", {"reading_status": "want_to_read", "user_rating": 3}, "b" * 64),
            (1, "OWNS", {
                "reading_status": "currently_reading", "personal_notes": "legacy note",
                "custom_metadata": '{"edition":"paperback"}',
            }, "a" * 64),
        ],
    )

    assert values["reading_status"] == "read"
    assert values["personal_notes"] == "overlay note"
    assert values["user_rating"] == 5
    assert values["custom_metadata"] == {"edition": "paperback", "shelf_color": "blue"}
    assert len(values["extra"]["_source_relationships"]) == 3
    assert pair_key and source_digest


def test_migration_requires_a_quiesced_source(graph):
    with pytest.raises(MigrationError, match="Pause application writes"):
        migrate_kuzu_to_postgres(graph, object())
