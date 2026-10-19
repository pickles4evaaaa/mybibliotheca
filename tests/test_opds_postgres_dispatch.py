from __future__ import annotations

import importlib
from types import SimpleNamespace

import pytest

from app.services.opds_sync_service import OPDSSyncService


def test_opds_preview_uses_postgres_lookup_without_opening_kuzu(monkeypatch):
    from app.infrastructure import persistence_state
    opds_sync_service = importlib.import_module("app.services.opds_sync_service")

    monkeypatch.setattr(persistence_state, "get_active_backend", lambda: "postgres")

    class Books:
        def get_book_by_opds_source_id_sync(self, source_id):
            return SimpleNamespace(id="existing-book") if source_id == "already-imported" else None

    service = object.__new__(OPDSSyncService)
    service._postgres_facade = lambda: SimpleNamespace(book_service=Books())

    def forbidden_kuzu_connection(**_kwargs):
        pytest.fail("PostgreSQL OPDS preview must not request a Kuzu connection")

    monkeypatch.setattr(opds_sync_service, "safe_get_connection", forbidden_kuzu_connection)
    result = service._simulate_entries([
        {"opds_source_id": "already-imported", "title": "Existing"},
        {"opds_source_id": "new-source", "title": "New"},
        {"title": "No source id"},
    ])

    assert result["would_update"] == 1
    assert result["would_create"] == 1
    assert result["skipped"] == 1
    assert [row["action"] for row in result["preview"]] == ["update", "create", "skip"]


def test_opds_preview_marks_lookup_failures_skipped_instead_of_new(monkeypatch):
    from app.infrastructure import persistence_state

    monkeypatch.setattr(persistence_state, "get_active_backend", lambda: "postgres")
    service = object.__new__(OPDSSyncService)

    def unavailable_facade():
        raise RuntimeError("PostgreSQL unavailable")

    service._postgres_facade = unavailable_facade
    result = service._simulate_entries([{"opds_source_id": "unknown", "title": "Unknown"}])

    assert result["would_update"] == 0
    assert result["would_create"] == 0
    assert result["skipped"] == 1
    assert result["preview"][0]["action"] == "skip"
    assert result["preview"][0]["reason"] == "lookup_failed"
