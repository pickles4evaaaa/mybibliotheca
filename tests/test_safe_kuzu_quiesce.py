import pytest

from app.utils.safe_kuzu_manager import SafeKuzuManager
from app.infrastructure import persistence_state


def test_quiesce_allows_only_the_reserved_migration_connection(tmp_path):
    manager = SafeKuzuManager(str(tmp_path / "kuzu.db"))
    manager._connection_count = 1

    with manager.quiesce_for_backup(
        reason="postgres_migration",
        allowed_active_connections=1,
        timeout_seconds=0.01,
    ):
        assert manager._writes_quiesced is True
        assert manager._connection_count == 1

    assert manager._writes_quiesced is False
    assert manager._pending_quiesce_reason is None


def test_quiesce_timeout_fails_closed_and_releases_waiters(tmp_path):
    manager = SafeKuzuManager(str(tmp_path / "kuzu.db"))
    manager._connection_count = 1

    with pytest.raises(TimeoutError, match="did not drain"):
        with manager.quiesce_for_backup(reason="postgres_migration", timeout_seconds=0.01):
            pytest.fail("quiesce should not proceed while an unowned connection is active")

    assert manager._writes_quiesced is False
    assert manager._pending_quiesce_reason is None


def test_nested_quiesce_is_rejected_without_releasing_outer_gate(tmp_path):
    manager = SafeKuzuManager(str(tmp_path / "kuzu.db"))

    with manager.quiesce_for_backup(reason="outer", timeout_seconds=0.01):
        with pytest.raises(RuntimeError, match="already quiesced"):
            with manager.quiesce_for_backup(reason="inner", timeout_seconds=0.01):
                pass
        assert manager._writes_quiesced is True

    assert manager._writes_quiesced is False


def test_postgres_cutover_retains_quiesce_until_restart_or_explicit_rollback(tmp_path):
    manager = SafeKuzuManager(str(tmp_path / "kuzu.db"))

    with manager.quiesce_for_backup(reason="postgres_migration_snapshot", timeout_seconds=0.01):
        manager.retain_quiesce_for_postgres_cutover()

    assert manager._writes_quiesced is True
    assert manager._permanently_quiesced is True
    assert manager._pending_quiesce_reason == "postgres_cutover_restart_required"

    manager.release_postgres_cutover_hold()
    assert manager._writes_quiesced is False
    assert manager._permanently_quiesced is False


def test_active_postgres_backend_rejects_any_kuzu_connection(tmp_path, monkeypatch):
    monkeypatch.setattr(persistence_state, "get_active_backend", lambda: "postgres")
    manager = SafeKuzuManager(str(tmp_path / "kuzu.db"))

    with pytest.raises(RuntimeError, match="PostgreSQL is the active persistence backend"):
        with manager.get_connection(operation="legacy_route"):
            pytest.fail("Kuzu should not be opened after PostgreSQL cutover")


def test_legacy_graph_connection_rejects_kuzu_after_postgres_cutover(tmp_path, monkeypatch):
    from app.infrastructure.kuzu_graph import KuzuGraphDB

    monkeypatch.setattr(persistence_state, "get_active_backend", lambda: "postgres")
    database_path = tmp_path / "must-not-open.db"
    graph = KuzuGraphDB(str(database_path))

    with pytest.raises(RuntimeError, match="Direct Kuzu access is disabled"):
        graph.connect()
    assert not database_path.exists()


def test_legacy_graph_storage_rechecks_backend_before_using_held_connection(monkeypatch):
    from app.infrastructure.kuzu_graph import KuzuGraphDB, KuzuGraphStorage

    monkeypatch.setattr(persistence_state, "get_active_backend", lambda: "postgres")

    class RawConnection:
        def execute(self, *_args, **_kwargs):
            pytest.fail("held raw Kuzu connection must not execute after cutover")

    graph = KuzuGraphDB.__new__(KuzuGraphDB)
    storage = KuzuGraphStorage.__new__(KuzuGraphStorage)
    storage.connection = graph
    storage.kuzu_conn = RawConnection()

    with pytest.raises(RuntimeError, match="Direct Kuzu access is disabled"):
        storage._execute_query("RETURN 1")
