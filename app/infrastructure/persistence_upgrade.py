"""Durable first-upgrade state for the Kuzu-to-PostgreSQL wizard."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any


def _data_root() -> Path:
    configured = os.getenv("APP_DATA_DIR")
    if configured:
        return Path(configured).expanduser().resolve()
    kuzu_path = Path(os.getenv("KUZU_DB_PATH", "data/kuzu")).expanduser()
    if not kuzu_path.is_absolute():
        kuzu_path = Path.cwd() / kuzu_path
    return kuzu_path.resolve().parent


def _upgrade_state_path() -> Path:
    return _data_root() / "persistence_upgrade.json"


def has_legacy_kuzu_database() -> bool:
    kuzu_path = Path(os.getenv("KUZU_DB_PATH", "data/kuzu")).expanduser()
    if not kuzu_path.is_absolute():
        kuzu_path = Path.cwd() / kuzu_path
    return (kuzu_path / "bibliotheca.db").exists()


def _backup_directory() -> Path:
    return _data_root() / "backups"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def record_verified_kuzu_backup(backup_id: str, backup_path: str | Path) -> dict[str, Any]:
    """Persist identity and integrity details for a completed Kuzu archive."""
    path = Path(backup_path).expanduser().resolve()
    backup_root = _backup_directory().resolve()
    if not path.is_relative_to(backup_root):
        raise ValueError("The migration backup must be stored in the application backup directory")
    if not path.is_file() or path.stat().st_size <= 0:
        raise ValueError("The Kuzu migration backup is missing or empty")
    record = {
        "status": "verified",
        "backup_id": str(backup_id),
        "path": str(path),
        "size_bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }
    state_path = _upgrade_state_path()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=".persistence-upgrade-", dir=str(state_path.parent))
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            os.fchmod(stream.fileno(), 0o600)
            json.dump(record, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, state_path)
        directory_fd = os.open(state_path.parent, os.O_RDONLY)
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
    return record


def get_verified_kuzu_backup(*, verify_hash: bool = False) -> dict[str, Any] | None:
    """Return a valid backup marker, optionally rechecking its full digest."""
    try:
        record = json.loads(_upgrade_state_path().read_text(encoding="utf-8"))
        if not isinstance(record, dict) or record.get("status") != "verified":
            return None
        path = Path(str(record.get("path", ""))).resolve()
        if not path.is_relative_to(_backup_directory().resolve()) or not path.is_file():
            return None
        if path.stat().st_size != int(record.get("size_bytes", -1)):
            return None
        if verify_hash and _sha256(path) != record.get("sha256"):
            return None
        return record
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None
