import json

import pytest

from app.infrastructure.persistence_state import (
    PersistenceStateError,
    get_active_backend,
    get_persistence_state,
    set_active_backend,
)


def test_missing_state_defaults_to_kuzu_without_creating_state_file(tmp_path, monkeypatch):
    path = tmp_path / "persistent" / "state.json"
    monkeypatch.setenv("MYBIBLIOTHECA_PERSISTENCE_STATE_PATH", str(path))
    monkeypatch.setenv("KUZU_DB_PATH", str(tmp_path / "empty-kuzu"))
    for name in ("POSTGRES_DSN", "DATABASE_URL", "POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER"):
        monkeypatch.delenv(name, raising=False)

    assert get_persistence_state() == {"backend": "kuzu", "migration_run_id": None}
    assert not path.exists()


def test_existing_kuzu_database_stays_authoritative_when_postgres_is_configured(tmp_path, monkeypatch):
    state_path = tmp_path / "persistent" / "state.json"
    kuzu_directory = tmp_path / "existing-kuzu"
    kuzu_directory.mkdir()
    (kuzu_directory / "bibliotheca.db").write_bytes(b"legacy database marker")
    monkeypatch.setenv("MYBIBLIOTHECA_PERSISTENCE_STATE_PATH", str(state_path))
    monkeypatch.setenv("KUZU_DB_PATH", str(kuzu_directory))
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://bibliotheca:test@localhost/bibliotheca")

    assert get_persistence_state() == {"backend": "kuzu", "migration_run_id": None}
    assert not state_path.exists()


def test_fresh_install_bootstraps_postgres_and_persists_backend_choice(tmp_path, monkeypatch):
    state_path = tmp_path / "persistent" / "state.json"
    monkeypatch.setenv("MYBIBLIOTHECA_PERSISTENCE_STATE_PATH", str(state_path))
    monkeypatch.setenv("KUZU_DB_PATH", str(tmp_path / "empty-kuzu"))
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://bibliotheca:test@localhost/bibliotheca")

    class Store:
        initialized = False

        def initialize_schema(self):
            self.initialized = True

    store = Store()
    monkeypatch.setattr("app.infrastructure.postgres_store.get_postgres_store", lambda: store)

    state = get_persistence_state()

    assert state == {"backend": "postgres", "migration_run_id": "native-postgres-install"}
    assert store.initialized
    assert json.loads(state_path.read_text()) == state
    assert state_path.stat().st_mode & 0o777 == 0o600


def test_postgres_state_requires_migration_run_and_is_atomic(tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    monkeypatch.setenv("MYBIBLIOTHECA_PERSISTENCE_STATE_PATH", str(path))

    with pytest.raises(ValueError, match="migration run id"):
        set_active_backend("postgres")

    monkeypatch.setattr("app.infrastructure.persistence_state._validate_postgres_cutover", lambda _run_id: None)
    set_active_backend("postgres", migration_run_id="verified-run-17")
    assert get_persistence_state() == {"backend": "postgres", "migration_run_id": "verified-run-17"}
    assert get_active_backend() == "postgres"
    assert json.loads(path.read_text()) == {
        "backend": "postgres", "migration_run_id": "verified-run-17",
    }
    assert not list(tmp_path.glob(".persistence-state-*"))

    with pytest.raises(PersistenceStateError, match="Direct rollback"):
        set_active_backend("kuzu")


def test_corrupt_state_fails_closed(tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    path.write_text('{"backend":"postgres"}')
    monkeypatch.setenv("MYBIBLIOTHECA_PERSISTENCE_STATE_PATH", str(path))

    with pytest.raises(PersistenceStateError, match="run id"):
        get_active_backend()
