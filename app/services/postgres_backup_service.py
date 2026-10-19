"""Verified PostgreSQL backup archives for the application backup manager.

Database data is captured with PostgreSQL's custom archive format. The archive
is validated with ``pg_restore --list`` before it is published to the backup
index. User supplied files are added beneath fixed archive roots and never
extracted using untrusted archive paths during restore.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import zipfile
import uuid
from datetime import datetime
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any

from psycopg.conninfo import conninfo_to_dict

from app.infrastructure.postgres_store import get_postgres_store


class PostgresBackupError(RuntimeError):
    """Raised when a PostgreSQL backup cannot be safely created/restored."""


_PG_ENV_KEYS = {
    "host": "PGHOST",
    "port": "PGPORT",
    "user": "PGUSER",
    "password": "PGPASSWORD",
    "sslmode": "PGSSLMODE",
    "sslrootcert": "PGSSLROOTCERT",
    "sslcert": "PGSSLCERT",
    "sslkey": "PGSSLKEY",
    "connect_timeout": "PGCONNECT_TIMEOUT",
    "gssencmode": "PGGSSENCMODE",
}


class PostgresBackupService:
    def __init__(self, backup_dir: Path, covers_dir: Path, uploads_dir: Path):
        self.backup_dir = Path(backup_dir)
        self.covers_dir = Path(covers_dir)
        self.uploads_dir = Path(uploads_dir)

    @staticmethod
    def _client(name: str) -> str:
        executable = os.getenv(name.upper() + "_BIN", name)
        resolved = shutil.which(executable)
        if not resolved:
            raise PostgresBackupError(f"{name} is required for PostgreSQL backup and restore")
        return resolved

    @staticmethod
    def _pg_environment() -> tuple[dict[str, str], str]:
        store = get_postgres_store()
        values = conninfo_to_dict(store.dsn)
        database = values.pop("dbname", "")
        if not database:
            raise PostgresBackupError("PostgreSQL connection has no database name")
        environment = os.environ.copy()
        for key, value in values.items():
            target = _PG_ENV_KEYS.get(key)
            if target and value:
                environment[target] = str(value)
        return environment, str(database)

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @staticmethod
    def _copy_tree_to_zip(archive: zipfile.ZipFile, path: Path, arc_root: str) -> int:
        count = 0
        if not path.exists():
            return count
        for child in path.rglob("*"):
            if child.is_file() and not child.is_symlink():
                relative = child.relative_to(path).as_posix()
                archive.write(child, f"{arc_root}/{relative}")
                count += 1
        return count

    def create_backup(self, *, name: str, description: str, backup_id: str, timestamp: datetime, reason: str) -> tuple[Path, dict[str, Any]]:
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(self.backup_dir, 0o700)
        pg_dump = self._client("pg_dump")
        pg_restore = self._client("pg_restore")
        environment, database = self._pg_environment()
        target = self.backup_dir / f"{name}_{backup_id[:8]}.zip"
        with tempfile.TemporaryDirectory(prefix="postgres-backup-", dir=self.backup_dir) as temp_dir:
            temp = Path(temp_dir)
            dump_file = temp / "database.dump"
            completed = subprocess.run(
                [pg_dump, "--format=custom", "--no-owner", "--no-acl", "--dbname", database, "--file", str(dump_file)],
                env=environment, capture_output=True, text=True, timeout=int(os.getenv("POSTGRES_BACKUP_TIMEOUT_SECONDS", "7200")),
            )
            if completed.returncode != 0 or not dump_file.is_file() or dump_file.stat().st_size == 0:
                detail = (completed.stderr or completed.stdout or "pg_dump failed").strip()[-2000:]
                raise PostgresBackupError(f"PostgreSQL backup failed: {detail}")
            with dump_file.open("rb") as dump_stream:
                archive_magic = dump_stream.read(5)
            if archive_magic != b"PGDMP":
                raise PostgresBackupError("pg_dump did not produce a valid custom-format archive")
            verified = subprocess.run(
                [pg_restore, "--list", str(dump_file)], env=environment, capture_output=True, text=True,
                timeout=int(os.getenv("POSTGRES_BACKUP_VERIFY_TIMEOUT_SECONDS", "300")),
            )
            if verified.returncode != 0 or not any(
                line.strip() and not line.lstrip().startswith(";")
                for line in (verified.stdout or "").splitlines()
            ):
                detail = (verified.stderr or "archive listing did not validate").strip()[-2000:]
                raise PostgresBackupError(f"PostgreSQL backup archive validation failed: {detail}")

            manifest: dict[str, Any] = {
                "backup_id": backup_id,
                "created_at": timestamp.isoformat(),
                "backend": "postgres",
                "backup_type": "postgres_full_system_backup",
                "database": database,
                "database_dump": "postgres/database.dump",
                "database_dump_sha256": self._sha256(dump_file),
                "database_dump_size": dump_file.stat().st_size,
                "original_size": dump_file.stat().st_size,
                "covers_files": 0,
                "uploads_files": 0,
                "environment_secrets_included": False,
                "reason": reason,
                "description": description,
                "restore_mode": "single-transaction with pg_restore --clean",
            }
            pending = target.with_suffix(target.suffix + ".partial")
            try:
                with zipfile.ZipFile(pending, "w", zipfile.ZIP_DEFLATED, compresslevel=3) as archive:
                    archive.write(dump_file, "postgres/database.dump")
                    manifest["covers_files"] = self._copy_tree_to_zip(archive, self.covers_dir, "data/covers")
                    manifest["uploads_files"] = self._copy_tree_to_zip(archive, self.uploads_dir, "data/uploads")
                    for config_path, member in (
                        (self.backup_dir.parent / "backup_settings.json", "config/backup_settings.json"),
                        (self.backup_dir.parent / "ai_config.json", "config/ai_config.json"),
                    ):
                        if config_path.is_file() and not config_path.is_symlink():
                            archive.write(config_path, member)
                    archive.writestr("backup_metadata.json", json.dumps(manifest, sort_keys=True, indent=2))
                os.chmod(pending, 0o600)
                with zipfile.ZipFile(pending, "r") as check:
                    bad_member = check.testzip()
                    if bad_member:
                        raise PostgresBackupError(f"Backup archive failed CRC validation at {bad_member}")
                    info = check.getinfo("postgres/database.dump")
                    if info.file_size != manifest["database_dump_size"]:
                        raise PostgresBackupError("Backup archive database size does not match its manifest")
                os.replace(pending, target)
                directory_fd = os.open(self.backup_dir, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            finally:
                pending.unlink(missing_ok=True)
        return target, manifest

    def restore_backup(self, backup_path: Path, *, backup_id: str) -> dict[str, Any]:
        """Restore a validated archive atomically at the PostgreSQL transaction level."""
        backup_path = Path(backup_path).resolve()
        pg_restore = self._client("pg_restore")
        environment, database = self._pg_environment()
        with zipfile.ZipFile(backup_path, "r") as archive:
            try:
                manifest = json.loads(archive.read("backup_metadata.json"))
                dump_info = archive.getinfo("postgres/database.dump")
            except (KeyError, ValueError, zipfile.BadZipFile) as exc:
                raise PostgresBackupError("The selected backup is not a PostgreSQL backup archive") from exc
            if manifest.get("backend") != "postgres" or manifest.get("backup_type") != "postgres_full_system_backup":
                raise PostgresBackupError("The selected archive does not contain a PostgreSQL database backup")
            if dump_info.file_size != int(manifest.get("database_dump_size", -1)):
                raise PostgresBackupError("Backup database size does not match its manifest")
            with tempfile.TemporaryDirectory(prefix="postgres-restore-", dir=self.backup_dir) as temp_dir:
                staging = Path(temp_dir)
                dump_file = staging / "database.dump"
                covers_stage = staging / "covers"
                uploads_stage = staging / "uploads"
                covers_stage.mkdir()
                uploads_stage.mkdir()
                allowed_exact = {
                    "backup_metadata.json", "postgres/database.dump",
                    "config/backup_settings.json", "config/ai_config.json",
                }
                seen_names: set[str] = set()
                total_uncompressed = 0
                max_uncompressed = int(os.getenv("POSTGRES_RESTORE_MAX_ARCHIVE_BYTES", str(100 * 1024**3)))
                for member in archive.infolist():
                    member_path = PurePosixPath(member.filename)
                    if member_path.is_absolute() or ".." in member_path.parts or "\\" in member.filename:
                        raise PostgresBackupError("Backup contains an unsafe archive path")
                    if member.filename in seen_names:
                        raise PostgresBackupError(f"Backup contains a duplicate archive member: {member.filename}")
                    seen_names.add(member.filename)
                    if member.is_dir():
                        continue
                    total_uncompressed += member.file_size
                    if total_uncompressed > max_uncompressed:
                        raise PostgresBackupError("Backup archive exceeds the configured restore size limit")
                    if member.filename in allowed_exact:
                        continue
                    if not (member.filename.startswith("data/covers/") or member.filename.startswith("data/uploads/")):
                        raise PostgresBackupError(f"Backup contains an unexpected file: {member.filename}")
                corrupt_member = archive.testzip()
                if corrupt_member:
                    raise PostgresBackupError(f"Backup archive CRC validation failed at {corrupt_member}")

                for member in archive.infolist():
                    if member.is_dir() or member.filename in allowed_exact:
                        continue
                    if member.filename.startswith("data/covers/"):
                        target = covers_stage / PurePosixPath(member.filename).relative_to("data/covers")
                    elif member.filename.startswith("data/uploads/"):
                        target = uploads_stage / PurePosixPath(member.filename).relative_to("data/uploads")
                    else:
                        continue
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with archive.open(member) as source, target.open("wb") as destination:
                        shutil.copyfileobj(source, destination, length=1024 * 1024)

                digest = hashlib.sha256()
                with archive.open("postgres/database.dump") as source, dump_file.open("wb") as destination:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        digest.update(chunk)
                        destination.write(chunk)
                    destination.flush()
                    os.fsync(destination.fileno())
                if digest.hexdigest() != manifest.get("database_dump_sha256"):
                    raise PostgresBackupError("Backup database checksum verification failed")
                checked = subprocess.run(
                    [pg_restore, "--list", str(dump_file)], env=environment, capture_output=True, text=True, timeout=300,
                )
                if checked.returncode != 0:
                    raise PostgresBackupError("PostgreSQL rejected the database archive before restore")

                # Stage and swap user files before the database transaction.
                # If either the swap or restore fails, put the old paths back.
                replacements = [
                    (covers_stage, self.covers_dir),
                    (uploads_stage, self.uploads_dir),
                ]
                for source, target in (
                    ("config/ai_config.json", self.backup_dir.parent / "ai_config.json"),
                    ("config/backup_settings.json", self.backup_dir.parent / "backup_settings.json"),
                ):
                    if any(item.filename == source for item in archive.infolist()):
                        file_stage = staging / (Path(source).name + ".stage")
                        with archive.open(source) as archive_file, file_stage.open("wb") as staged_file:
                            shutil.copyfileobj(archive_file, staged_file)
                        replacements.append((file_stage, target))

                moved_old: list[tuple[Path, Path | None, Path]] = []
                try:
                    for source, target in replacements:
                        target.parent.mkdir(parents=True, exist_ok=True)
                        old_path = target.with_name(f".{target.name}.pre-restore-{uuid.uuid4().hex}") if target.exists() else None
                        if old_path:
                            os.replace(target, old_path)
                        moved_old.append((target, old_path, source))
                        os.replace(source, target)
                    restored = subprocess.run(
                        [pg_restore, "--clean", "--if-exists", "--single-transaction", "--no-owner", "--no-acl", "--dbname", database, str(dump_file)],
                        env=environment, capture_output=True, text=True,
                        timeout=int(os.getenv("POSTGRES_RESTORE_TIMEOUT_SECONDS", "7200")),
                    )
                    if restored.returncode != 0:
                        detail = (restored.stderr or restored.stdout or "pg_restore failed").strip()[-3000:]
                        raise PostgresBackupError(f"PostgreSQL restore transaction failed: {detail}")
                except Exception:
                    for target, old_path, _source in reversed(moved_old):
                        if target.exists():
                            if target.is_dir():
                                shutil.rmtree(target)
                            else:
                                target.unlink()
                        if old_path and old_path.exists():
                            os.replace(old_path, target)
                    raise
                for _target, old_path, _source in moved_old:
                    if old_path and old_path.exists():
                        if old_path.is_dir():
                            shutil.rmtree(old_path)
                        else:
                            old_path.unlink()
        return {"database": database, "backup_id": backup_id, "restored_at": datetime.now().isoformat()}
