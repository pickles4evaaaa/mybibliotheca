"""Opt-in PostgreSQL backup/restore integration test.

Set POSTGRES_TEST_DSN to a fresh disposable database. The special value
"compose" uses the standard POSTGRES_* variables provided by the local
Compose service. Restore replaces database objects and bundled app files, so
this test must never target an application database.
"""

from __future__ import annotations

import os
import zipfile
from datetime import datetime, timezone

import pytest

from app.infrastructure.postgres_store import PostgresStore
from app.services import postgres_backup_service
from app.services.postgres_backup_service import PostgresBackupService, PostgresBackupError


def test_postgres_backup_restore_round_trip_and_rejects_corrupt_archive(monkeypatch, tmp_path):
    configured_dsn = os.getenv("POSTGRES_TEST_DSN")
    if not configured_dsn:
        pytest.skip("Set POSTGRES_TEST_DSN to a fresh disposable PostgreSQL database")
    store = PostgresStore() if configured_dsn == "compose" else PostgresStore(configured_dsn)
    backup_dir = tmp_path / "backups"
    covers_dir = tmp_path / "covers"
    uploads_dir = tmp_path / "uploads"
    covers_dir.mkdir()
    uploads_dir.mkdir()
    (covers_dir / "cover.txt").write_text("original cover", encoding="utf-8")
    (uploads_dir / "import.txt").write_text("original upload", encoding="utf-8")
    (tmp_path / "ai_config.json").write_text('{"provider":"original"}', encoding="utf-8")
    (tmp_path / "backup_settings.json").write_text('{"enabled":true}', encoding="utf-8")
    monkeypatch.setattr(postgres_backup_service, "get_postgres_store", lambda: store)
    service = PostgresBackupService(backup_dir, covers_dir, uploads_dir)

    try:
        store.initialize_schema()
        with store.connection() as connection, connection.cursor() as cursor:
            for table in ("users", "books", "user_books", "reading_logs", "book_contributors",
                          "book_relationship_edges", "custom_fields"):
                cursor.execute(f"SELECT count(*) FROM {table}")
                assert cursor.fetchone()[0] == 0, f"POSTGRES_TEST_DSN must be fresh; {table} is not empty"
            cursor.execute("CREATE TABLE backup_restore_probe (value text NOT NULL)")
            cursor.execute("INSERT INTO backup_restore_probe VALUES ('before')")
        backup_path, manifest = service.create_backup(
            name="round-trip", description="integration test", backup_id="backup-test-1",
            timestamp=datetime.now(timezone.utc), reason="test",
        )

        with store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("UPDATE backup_restore_probe SET value='after'")
        (covers_dir / "cover.txt").write_text("modified cover", encoding="utf-8")
        (covers_dir / "extra.txt").write_text("remove this on restore", encoding="utf-8")
        (uploads_dir / "import.txt").write_text("modified upload", encoding="utf-8")
        (tmp_path / "ai_config.json").write_text('{"provider":"modified"}', encoding="utf-8")

        result = service.restore_backup(backup_path, backup_id="backup-test-1")
        assert result["database"] == manifest["database"]
        with store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT value FROM backup_restore_probe")
            assert cursor.fetchone()[0] == "before"
        assert (covers_dir / "cover.txt").read_text(encoding="utf-8") == "original cover"
        assert not (covers_dir / "extra.txt").exists()
        assert (uploads_dir / "import.txt").read_text(encoding="utf-8") == "original upload"
        assert (tmp_path / "ai_config.json").read_text(encoding="utf-8") == '{"provider":"original"}'
        assert (tmp_path / "backup_settings.json").read_text(encoding="utf-8") == '{"enabled":true}'

        corrupt_backup = backup_dir / "corrupt.zip"
        with zipfile.ZipFile(corrupt_backup, "w") as archive:
            archive.writestr("backup_metadata.json", "{}")
        with pytest.raises(PostgresBackupError):
            service.restore_backup(corrupt_backup, backup_id="corrupt")
        with store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT value FROM backup_restore_probe")
            assert cursor.fetchone()[0] == "before"
        assert (covers_dir / "cover.txt").read_text(encoding="utf-8") == "original cover"
    finally:
        store.close()
