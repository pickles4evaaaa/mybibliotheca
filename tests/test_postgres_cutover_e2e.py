"""Opt-in full cutover integration test for an empty disposable PostgreSQL DB.

Set POSTGRES_TEST_DSN to a fresh, disposable database. The special value
"compose" uses the standard POSTGRES_* environment variables provided by the
local Compose service. This test commits a fixture migration and must not run
against a user's application database.
"""

from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from datetime import datetime, timezone

import kuzu
import pytest

from app.infrastructure import postgres_store
from app.infrastructure.postgres_store import PostgresMigrationTargetNotEmpty
from app.infrastructure.persistence_state import get_active_backend, set_active_backend
from app.infrastructure.postgres_migration import (
    _REFERENCE_ENTITY_IMPORTS,
    decode_migration_value,
    migrate_kuzu_manager_to_postgres,
)
from app.services.postgres_custom_field_service import PostgresCustomFieldService
import app.services.postgres_service_facade as postgres_facade_module
from app.services.postgres_service_facade import PostgresServiceFacade
from app.simplified_book_service import SimplifiedBookService, UserBookAnnotation


class _FixtureManager:
    def __init__(self, source):
        self.source = source
        self._lock = threading.RLock()
        self._writes_quiesced = False
        self._pending_quiesce_reason = None

    @contextmanager
    def get_connection(self, *, operation):
        yield self.source

    @contextmanager
    def quiesce_for_backup(self, *, reason, allowed_active_connections, timeout_seconds):
        self._writes_quiesced = True
        self._pending_quiesce_reason = reason
        try:
            yield
        finally:
            self._writes_quiesced = False
            self._pending_quiesce_reason = None

    def retain_quiesce_for_postgres_cutover(self):
        self._writes_quiesced = True
        self._pending_quiesce_reason = "postgres_cutover"


def test_full_kuzu_to_postgres_cutover_preserves_custom_field_values(monkeypatch, tmp_path):
    configured_dsn = os.getenv("POSTGRES_TEST_DSN")
    if not configured_dsn:
        pytest.skip("Set POSTGRES_TEST_DSN to an empty disposable PostgreSQL database")
    store = postgres_store.PostgresStore() if configured_dsn == "compose" else postgres_store.PostgresStore(configured_dsn)
    database = None
    source = None
    try:
        store.initialize_schema()
        with store.connection() as connection, connection.cursor() as cursor:
            target_tables = {
                "users", "books", "user_books", "reading_logs", "book_contributors",
                "book_relationship_edges", "custom_fields",
                *(spec["table"] for spec in _REFERENCE_ENTITY_IMPORTS.values()),
            }
            for table in sorted(target_tables):
                cursor.execute(f"SELECT count(*) FROM {table}")
                assert cursor.fetchone()[0] == 0, f"POSTGRES_TEST_DSN must be empty; {table} is not empty"

        database = kuzu.Database(str(tmp_path / "source.db"))
        source = kuzu.Connection(database)
        source.execute(
            "CREATE NODE TABLE User(id STRING, username STRING, email STRING, password_hash STRING, "
            "is_active BOOLEAN, timezone STRING, future_setting STRING, PRIMARY KEY(id))"
        )
        source.execute("CREATE NODE TABLE Book(id STRING, title STRING, PRIMARY KEY(id))")
        source.execute(
            "CREATE NODE TABLE CustomField(id STRING, name STRING, field_type STRING, "
            "is_required BOOLEAN, is_active BOOLEAN, PRIMARY KEY(id))"
        )
        source.execute(
            "CREATE NODE TABLE UserCustomField(id STRING, field_name STRING, field_type STRING, "
            "is_required BOOLEAN, default_value STRING, future_rule STRING, PRIMARY KEY(id))"
        )
        source.execute("CREATE NODE TABLE Library(id STRING, name STRING, description STRING, PRIMARY KEY(id))")
        source.execute("CREATE REL TABLE OWNS(FROM User TO Book, reading_status STRING, personal_notes STRING)")
        source.execute("CREATE REL TABLE USER_OWNS_LIBRARY(FROM User TO Library, access_level STRING)")
        source.execute(
            "CREATE REL TABLE HAS_CUSTOM_FIELD(FROM User TO CustomField, book_id STRING, field_name STRING, "
            "field_value STRING, created_at TIMESTAMP, source_hint STRING)"
        )
        source.execute(
            "CREATE (u:User {id:'user-1',username:'reader',email:'reader@example.test',password_hash:'hash', "
            "is_active:true,timezone:'UTC',future_setting:'preserved'})"
        )
        source.execute("CREATE (b:Book {id:'book-1',title:'A preserved book'})")
        source.execute(
            "CREATE (f:CustomField {id:'field-1',name:'edition',field_type:'text',is_required:false,is_active:true})"
        )
        source.execute(
            "CREATE (f:UserCustomField {id:'legacy-user-field-1',field_name:'shelf_note',field_type:'text', "
            "is_required:false,default_value:'',future_rule:'preserve-me'})"
        )
        source.execute("CREATE (l:Library {id:'library-1',name:'Home library',description:'legacy library'})")
        source.execute(
            "MATCH (u:User {id:'user-1'}),(b:Book {id:'book-1'}) "
            "CREATE (u)-[:OWNS {reading_status:'currently_reading',personal_notes:'keep this note'}]->(b)"
        )
        source.execute(
            "MATCH (u:User {id:'user-1'}),(f:CustomField {id:'field-1'}) "
            "CREATE (u)-[:HAS_CUSTOM_FIELD {book_id:'book-1',field_name:'edition',field_value:'signed', "
            "created_at:TIMESTAMP('2026-10-19 12:00:00'),source_hint:'legacy-edge'}]->(f)"
        )
        source.execute(
            "MATCH (u:User {id:'user-1'}),(l:Library {id:'library-1'}) "
            "CREATE (u)-[:USER_OWNS_LIBRARY {access_level:'owner'}]->(l)"
        )

        monkeypatch.setenv("MYBIBLIOTHECA_PERSISTENCE_STATE_PATH", str(tmp_path / "persistence_state.json"))
        monkeypatch.setattr(postgres_store, "get_postgres_store", lambda: store)
        monkeypatch.setattr(postgres_facade_module, "get_postgres_store", lambda: store)
        manager = _FixtureManager(source)
        result = migrate_kuzu_manager_to_postgres(
            manager,
            store,
            quiesce_timeout_seconds=10,
            on_verified=lambda migration: (
                manager.retain_quiesce_for_postgres_cutover(),
                set_active_backend("postgres", migration_run_id=migration["run_id"]),
            ),
            require_cutover_ready=True,
        )

        assert result["audit"]["preservation_complete"]
        assert result["audit"]["projection_complete"]
        assert result["audit"]["cutover_allowed"]
        with pytest.raises(PostgresMigrationTargetNotEmpty, match=r"users \(1\)"):
            store.assert_migration_target_empty()
        assert result["audit"]["relationship_projections"]["HAS_CUSTOM_FIELD"]["status"] == "verified"
        legacy_relation = result["audit"]["relationship_projections"]["USER_OWNS_LIBRARY"]
        assert legacy_relation["status"] == "verified"
        assert legacy_relation["archive_only"] is True
        assert get_active_backend() == "postgres"
        facade = PostgresServiceFacade(store)
        migrated_user = facade.user_service.get_user_by_id_sync("user-1")
        assert migrated_user is not None
        assert migrated_user.username == "reader"
        migrated_book = facade.book_service.get_book_by_id_sync("book-1")
        assert migrated_book is not None
        assert migrated_book.title == "A preserved book"
        with store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("INSERT INTO categories (id,name) VALUES ('genre-1','Fiction')")
            cursor.execute("INSERT INTO books (id,title) VALUES ('unowned-book','Not in this library')")
            cursor.execute(
                "INSERT INTO book_categories (book_id,category_id) VALUES "
                "('book-1','genre-1'),('unowned-book','genre-1')"
            )
        category_books = facade.get_books_by_category_sync("genre-1", "user-1")
        assert [book["id"] for book in category_books] == ["book-1"]
        assert category_books[0]["uid"] == "book-1"
        assert [book["id"] for book in facade.get_books_by_category_sync("genre-1")] == [
            "book-1", "unowned-book",
        ]
        # Production restarts the worker after cutover. Drop any Kuzu service
        # instances created while the migration UI was running so this check
        # follows the same backend selection path as the restarted worker.
        from app.services import reset_all_services
        reset_all_services()
        migrated_overlay = facade.library_service.get_user_book_sync("user-1", "book-1")
        assert migrated_overlay is not None
        assert migrated_overlay["reading_status"] == "currently_reading"
        assert migrated_overlay["personal_notes"] == "keep this note"
        assert SimplifiedBookService().create_user_annotation(UserBookAnnotation(
            user_id="user-1", book_id="book-1", reading_status="read",
            user_rating=4.5, personal_notes="annotation update",
            custom_metadata={"annotation_tone": "lyrical"},
        ))
        annotated_overlay = facade.library_service.get_user_book_sync("user-1", "book-1")
        assert annotated_overlay["reading_status"] == "read"
        assert annotated_overlay["user_rating"] == 4.5
        assert annotated_overlay["personal_notes"] == "annotation update"
        assert annotated_overlay["custom_metadata"]["annotation_tone"] == "lyrical"
        with store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT extra->>'future_setting' FROM users WHERE id='user-1'")
            assert cursor.fetchone()[0] == "preserved"
            cursor.execute(
                "SELECT field_name,field_type,is_required,default_value,extra->>'future_rule' "
                "FROM user_custom_fields WHERE id='legacy-user-field-1'"
            )
            assert cursor.fetchone() == ("shelf_note", "text", False, "", "preserve-me")
            cursor.execute(
                "SELECT user_id,definition_id,book_id,field_name,field_value,is_required,is_active,created_at,extra "
                "FROM custom_fields WHERE id LIKE %s",
                (f"kuzu-edge:{result['run_id']}:%",),
            )
            assert cursor.fetchone() == (
                "user-1", "field-1", "book-1", "edition", "signed", False, True,
                datetime(2026, 10, 19, 12, 0, tzinfo=timezone.utc), {"source_hint": "legacy-edge"},
            )
            cursor.execute(
                "SELECT relationship,source_label,source_id,target_label,target_id,properties "
                "FROM app_graph_edges WHERE snapshot_run_id=%s AND relationship='USER_OWNS_LIBRARY'",
                (result["run_id"],),
            )
            legacy_edge = cursor.fetchone()
            assert legacy_edge[:5] == (
                "USER_OWNS_LIBRARY", "User", "user-1", "Library", "library-1",
            )
            assert decode_migration_value(legacy_edge[5]) == {"access_level": "owner"}
        assert PostgresCustomFieldService(store)._read_custom_metadata("book-1", "user-1") == {
            "edition": "signed", "annotation_tone": "lyrical",
        }
        assert source.execute("MATCH (u:User {id:'user-1'}) RETURN u.future_setting").get_next()[0] == "preserved"
    finally:
        if source is not None:
            source.close()
        if database is not None:
            del source
            del database
        store.close()
