"""Durable application persistence selection for the Kuzu-to-PostgreSQL cutover."""

from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any


class PersistenceStateError(RuntimeError):
    """Raised when the active persistence state cannot be trusted."""


_lock = threading.RLock()
_NATIVE_POSTGRES_RUN_ID = "native-postgres-install"


def _state_path() -> Path:
    configured = os.getenv("MYBIBLIOTHECA_PERSISTENCE_STATE_PATH")
    if configured:
        return Path(configured).expanduser().resolve()
    data_dir = os.getenv("APP_DATA_DIR")
    if data_dir:
        root = Path(data_dir)
    else:
        kuzu_path = Path(os.getenv("KUZU_DB_PATH", "data/kuzu"))
        root = kuzu_path.parent
    if not root.is_absolute():
        root = Path.cwd() / root
    return root.resolve() / "persistence_state.json"


def _pending_cutover_path() -> Path:
    return _state_path().with_name("persistence_cutover_pending.json")


def _has_legacy_kuzu_database() -> bool:
    """Return whether the configured Kuzu path contains the actual database."""
    configured = Path(os.getenv("KUZU_DB_PATH", "data/kuzu")).expanduser()
    if not configured.is_absolute():
        configured = Path.cwd() / configured
    return (configured / "bibliotheca.db").exists()


def _postgres_is_configured() -> bool:
    """Check configuration only; connectivity is verified during schema setup."""
    if (os.getenv("POSTGRES_DSN") or "").strip():
        return True
    database_url = (os.getenv("DATABASE_URL") or "").strip().lower()
    if database_url.startswith(("postgres://", "postgresql://", "postgresql+psycopg://")):
        return True
    return all((os.getenv(name) or "").strip() for name in ("POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER"))


def _atomic_write_state(payload: dict[str, Any]) -> None:
    """Durably replace the active backend marker with an atomic filesystem write."""
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=".persistence-state-", dir=str(path.parent))
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            os.fchmod(stream.fileno(), 0o600)
            json.dump(payload, stream, ensure_ascii=False, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        try:
            os.unlink(temporary_name)
        except OSError:
            pass
        raise


def _bootstrap_native_postgres_install(path: Path) -> dict[str, Any]:
    """Initialize PostgreSQL for a fresh install that has no legacy Kuzu DB."""
    from .postgres_store import get_postgres_store

    get_postgres_store().initialize_schema()
    payload = {"backend": "postgres", "migration_run_id": _NATIVE_POSTGRES_RUN_ID}
    with _lock:
        # A competing worker may have completed bootstrap while schema setup
        # ran. Never overwrite an already selected backend.
        if path.exists():
            try:
                current = json.loads(path.read_text(encoding="utf-8"))
            except Exception as exc:
                raise PersistenceStateError(f"Cannot read persistence state at {path}") from exc
            backend = current.get("backend") if isinstance(current, dict) else None
            if backend not in {"kuzu", "postgres"}:
                raise PersistenceStateError(f"Invalid persistence backend in {path}")
            if backend == "postgres" and not current.get("migration_run_id"):
                raise PersistenceStateError("PostgreSQL backend state has no verified migration run id")
            return {"backend": backend, "migration_run_id": current.get("migration_run_id")}
        _atomic_write_state(payload)
    return payload


def write_pending_cutover(migration_run_id: str) -> None:
    """Record the commit/activation boundary so a process crash is recoverable."""
    run_id = str(migration_run_id or "").strip()
    if not run_id:
        raise ValueError("A pending PostgreSQL cutover requires a migration run id")
    path = _pending_cutover_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with _lock:
        if path.exists():
            existing = json.loads(path.read_text(encoding="utf-8"))
            if existing.get("migration_run_id") != run_id:
                raise PersistenceStateError("Another PostgreSQL cutover is already pending recovery")
            return
        descriptor, temporary_name = tempfile.mkstemp(prefix=".persistence-pending-", dir=str(path.parent))
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                os.fchmod(stream.fileno(), 0o600)
                json.dump({"migration_run_id": run_id}, stream, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_name, path)
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except Exception:
            try:
                os.unlink(temporary_name)
            except OSError:
                pass
            raise


def clear_pending_cutover(migration_run_id: str | None = None) -> None:
    """Remove a matching pending marker after activation or transaction rollback."""
    path = _pending_cutover_path()
    with _lock:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return
        except Exception as exc:
            raise PersistenceStateError(f"Cannot read pending cutover state at {path}") from exc
        if migration_run_id is not None and str(payload.get("migration_run_id")) != str(migration_run_id):
            return
        path.unlink(missing_ok=True)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)


def recover_pending_cutover() -> str | None:
    """Resolve a crash between the verified PostgreSQL commit and state switch.

    If the run did not commit, Kuzu remains authoritative and the stale marker
    is discarded. If it did commit, startup validates the full cutover audit
    and activates PostgreSQL before Kuzu services or requests can start.
    """
    path = _pending_cutover_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception as exc:
        raise PersistenceStateError(f"Cannot read pending cutover state at {path}") from exc
    run_id = str(payload.get("migration_run_id") or "").strip()
    if not run_id:
        raise PersistenceStateError("Pending PostgreSQL cutover has no migration run id")
    current = get_persistence_state()
    if current["backend"] == "postgres":
        if current.get("migration_run_id") != run_id:
            raise PersistenceStateError("Active PostgreSQL run does not match pending cutover recovery")
        clear_pending_cutover(run_id)
        return "postgres"

    from .postgres_store import get_postgres_store
    store = get_postgres_store()
    store.initialize_schema()
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT status FROM bibliotheca_migration_runs WHERE run_id=%s", (run_id,))
        row = cursor.fetchone()
    if not row:
        # The PostgreSQL transaction did not commit. Kuzu remains the source.
        clear_pending_cutover(run_id)
        return "kuzu"
    if row[0] != "verified":
        raise PersistenceStateError("Pending PostgreSQL migration run is not verified")
    set_active_backend("postgres", migration_run_id=run_id)
    clear_pending_cutover(run_id)
    return "postgres"


def get_persistence_state() -> dict[str, Any]:
    """Read the selected backend or safely distinguish upgrade from fresh install.

    An existing Kuzu database remains authoritative until its owner runs the
    verified migration wizard. A new install with PostgreSQL configured starts
    directly on PostgreSQL and records that choice durably.
    """
    path = _state_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        if _has_legacy_kuzu_database() or not _postgres_is_configured():
            return {"backend": "kuzu", "migration_run_id": None}
        return _bootstrap_native_postgres_install(path)
    except Exception as exc:
        raise PersistenceStateError(f"Cannot read persistence state at {path}") from exc
    backend = payload.get("backend") if isinstance(payload, dict) else None
    if backend not in {"kuzu", "postgres"}:
        raise PersistenceStateError(f"Invalid persistence backend in {path}")
    if backend == "postgres" and not payload.get("migration_run_id"):
        raise PersistenceStateError("PostgreSQL backend state has no verified migration run id")
    return {"backend": backend, "migration_run_id": payload.get("migration_run_id")}


def get_active_backend() -> str:
    return str(get_persistence_state()["backend"])


def _validate_postgres_cutover(migration_run_id: str) -> None:
    """Refuse activation unless the stored migration run passes the live audit."""
    if str(migration_run_id) == _NATIVE_POSTGRES_RUN_ID:
        if _has_legacy_kuzu_database():
            raise PersistenceStateError(
                "Native PostgreSQL state is invalid because a legacy Kuzu database is present"
            )
        from .postgres_store import get_postgres_store
        get_postgres_store().initialize_schema()
        return

    from .postgres_migration import audit_relational_projection
    from .postgres_store import get_postgres_store

    store = get_postgres_store()
    store.initialize_schema()
    with store.connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT status FROM bibliotheca_migration_runs WHERE run_id=%s",
            (str(migration_run_id),),
        )
        row = cursor.fetchone()
        if not row or row[0] != "verified":
            raise PersistenceStateError("The selected PostgreSQL migration run is not verified")
        audit = audit_relational_projection(connection, str(migration_run_id))
    if not audit.get("preservation_complete") or not audit.get("projection_complete"):
        raise PersistenceStateError("PostgreSQL cutover is blocked by the data preservation/projection audit")
    if audit.get("cutover_blockers"):
        raise PersistenceStateError(
            "PostgreSQL cutover is blocked: " + "; ".join(str(item) for item in audit["cutover_blockers"])
        )
    if not audit.get("cutover_allowed"):
        raise PersistenceStateError("Application PostgreSQL cutover readiness has not been approved")


def set_active_backend(backend: str, *, migration_run_id: str | None = None) -> None:
    """Atomically persist the selected backend after the migration workflow validates it."""
    if backend not in {"kuzu", "postgres"}:
        raise ValueError("backend must be 'kuzu' or 'postgres'")
    current = get_persistence_state()
    if current["backend"] == "postgres" and backend == "kuzu":
        raise PersistenceStateError(
            "Direct rollback to Kuzu is disabled after PostgreSQL activation because PostgreSQL may contain new writes"
        )
    if (
        current["backend"] == "postgres"
        and backend == "postgres"
        and current.get("migration_run_id") != str(migration_run_id or "")
    ):
        raise PersistenceStateError("The active PostgreSQL migration run cannot be replaced in place")
    if backend == "postgres" and not str(migration_run_id or "").strip():
        raise ValueError("PostgreSQL activation requires a verified migration run id")
    if backend == "postgres":
        _validate_postgres_cutover(str(migration_run_id))
    path = _state_path()
    payload = {
        "backend": backend,
        "migration_run_id": str(migration_run_id) if backend == "postgres" else None,
    }
    with _lock:
        _atomic_write_state(payload)
