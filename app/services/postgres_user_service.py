"""PostgreSQL implementation of the account service contract.

This service is kept separate from the Kuzu adapter while the rest of the
application is being ported. It intentionally does not select itself based on
an environment flag: changing only account storage would create a split
database and is not a complete application cutover.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone as dt_timezone
from typing import Any

from ..domain.models import Location, User
from ..infrastructure.postgres_store import PostgresStore, get_postgres_store


_USER_COLUMNS = (
    "id", "username", "email", "password_hash", "share_current_reading",
    "share_reading_activity", "share_library", "is_admin", "is_active",
    "password_must_change", "failed_login_attempts", "locked_until",
    "last_login", "password_changed_at", "reading_streak_offset", "timezone",
    "display_name", "bio", "location", "website", "created_at", "updated_at",
)


def _user_from_row(row: Any) -> User:
    values = dict(zip(_USER_COLUMNS, row))
    values["created_at"] = values["created_at"] or datetime.now(dt_timezone.utc)
    values["updated_at"] = values["updated_at"] or values["created_at"]
    return User(**values)


class PostgresUserService:
    """Synchronous account persistence with async-compatible wrappers."""

    def __init__(self, store: PostgresStore | None = None):
        self.store = store or get_postgres_store()

    def _get_user(self, predicate: str, value: str) -> User | None:
        if predicate not in ("id", "username", "email"):
            raise ValueError("Unsupported user lookup predicate")
        where = f"lower({predicate}) = lower(%s)" if predicate != "id" else "id = %s"
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(f"SELECT {', '.join(_USER_COLUMNS)} FROM users WHERE {where} LIMIT 1", (value,))
            row = cursor.fetchone()
        return _user_from_row(row) if row else None

    def get_user_by_id_sync(self, user_id: str) -> User | None:
        return self._get_user("id", user_id)

    async def get_user_by_id(self, user_id: str) -> User | None:
        return await asyncio.to_thread(self.get_user_by_id_sync, user_id)

    def get_user_by_username_sync(self, username: str) -> User | None:
        return self._get_user("username", username)

    async def get_user_by_username(self, username: str) -> User | None:
        return await asyncio.to_thread(self.get_user_by_username_sync, username)

    def get_user_by_email_sync(self, email: str) -> User | None:
        return self._get_user("email", email)

    async def get_user_by_email(self, email: str) -> User | None:
        return await asyncio.to_thread(self.get_user_by_email_sync, email)

    def get_user_by_username_or_email_sync(self, username_or_email: str) -> User | None:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {', '.join(_USER_COLUMNS)} FROM users "
                "WHERE lower(username) = lower(%s) OR lower(email) = lower(%s) LIMIT 1",
                (username_or_email, username_or_email),
            )
            row = cursor.fetchone()
        return _user_from_row(row) if row else None

    async def get_user_by_username_or_email(self, username_or_email: str) -> User | None:
        return await asyncio.to_thread(self.get_user_by_username_or_email_sync, username_or_email)

    def create_user_sync(
        self,
        username: str,
        email: str,
        password_hash: str,
        display_name: str | None = None,
        is_admin: bool = False,
        is_active: bool = True,
        password_must_change: bool = False,
        timezone: str = "UTC",
        location: str = "",
    ) -> User | None:
        user = User(
            id=str(uuid.uuid4()), username=username, email=email, password_hash=password_hash,
            display_name=display_name, is_admin=is_admin, is_active=is_active,
            password_must_change=password_must_change, timezone=timezone,
            location=location or None,
        )
        now = datetime.now(dt_timezone.utc)
        try:
            with self.store.transaction() as connection, connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO users (id, username, email, password_hash, display_name, is_admin, "
                    "is_active, password_must_change, timezone, location, created_at, updated_at) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (user.id, username, email, password_hash, display_name, is_admin, is_active,
                     password_must_change, timezone, user.location, now, now),
                )
        except Exception as exc:
            from psycopg import errors

            if isinstance(exc, errors.UniqueViolation):
                return None
            raise
        user.created_at = now
        user.updated_at = now
        return user

    async def create_user(self, username: str, email: str, password_hash: str,
                          display_name: str | None = None, is_admin: bool = False) -> User | None:
        return await asyncio.to_thread(
            self.create_user_sync, username, email, password_hash, display_name, is_admin,
        )

    def get_user_count_sync(self) -> int:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM users")
            return int(cursor.fetchone()[0])

    async def get_user_count(self) -> int:
        return await asyncio.to_thread(self.get_user_count_sync)

    def get_active_user_count_sync(self) -> int:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM users WHERE is_active")
            return int(cursor.fetchone()[0])

    def get_admin_count_sync(self) -> int:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM users WHERE is_admin")
            return int(cursor.fetchone()[0])

    async def get_admin_count(self) -> int:
        return await asyncio.to_thread(self.get_admin_count_sync)

    def get_all_users_sync(self, limit: int = 1000) -> list[User]:
        limit = max(0, min(int(limit), 10000))
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {', '.join(_USER_COLUMNS)} FROM users ORDER BY created_at, id LIMIT %s",
                (limit,),
            )
            return [_user_from_row(row) for row in cursor.fetchall()]

    async def get_all_users(self, limit: int = 1000) -> list[User]:
        return await asyncio.to_thread(self.get_all_users_sync, limit)

    def update_user_sync(self, user: User) -> User | None:
        if not user.id:
            return None
        fields = [column for column in _USER_COLUMNS if column not in ("id", "created_at")]
        values = [getattr(user, column) for column in fields]
        values[fields.index("updated_at")] = datetime.now(dt_timezone.utc)
        assignments = ", ".join(f"{field} = %s" for field in fields)
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"UPDATE users SET {assignments} WHERE id = %s RETURNING {', '.join(_USER_COLUMNS)}",
                (*values, user.id),
            )
            row = cursor.fetchone()
        return _user_from_row(row) if row else None

    async def update_user(self, user: User) -> User | None:
        return await asyncio.to_thread(self.update_user_sync, user)

    def delete_user_sync(self, user_id: str) -> bool:
        # Keep account and reading history intact. Admin deletion in the
        # PostgreSQL backend deactivates access; hard deletion needs an
        # explicit product policy for all dependent records.
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute("UPDATE users SET is_active = FALSE, updated_at = now() WHERE id = %s", (user_id,))
            return cursor.rowcount > 0

    async def delete_user(self, user_id: str) -> bool:
        return await asyncio.to_thread(self.delete_user_sync, user_id)

    async def create_user_location(self, user_id: str, name: str, description: str | None = None,
                                   location_type: str = "home", is_default: bool = False) -> Location | None:
        location_id = str(uuid.uuid4())
        now = datetime.now(dt_timezone.utc)
        with self.store.transaction() as connection, connection.cursor() as cursor:
            if is_default:
                cursor.execute(
                    "UPDATE locations SET is_default = FALSE, updated_at = %s WHERE owner_user_id = %s",
                    (now, user_id),
                )
            cursor.execute(
                "INSERT INTO locations (id, owner_user_id, name, description, location_type, "
                "is_default, is_active, created_at, updated_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s, %s)",
                (location_id, user_id, name, description or "Default location set during onboarding",
                 location_type, is_default, now, now),
            )
        return Location(
            id=location_id, user_id=user_id, name=name,
            description=description or "Default location set during onboarding",
            location_type=location_type, is_default=is_default, is_active=True,
            created_at=now, updated_at=now,
        )

    async def get_user_locations(self, user_id: str) -> list[Location]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT id, owner_user_id, name, description, location_type, address, "
                "is_default, is_active, created_at, updated_at "
                "FROM locations WHERE owner_user_id = %s ORDER BY is_default DESC, name, id",
                (user_id,),
            )
            rows = cursor.fetchall()
        return [
            Location(
                id=row[0], user_id=row[1], name=row[2], description=row[3],
                location_type=row[4] or "home", address=row[5], is_default=bool(row[6]),
                is_active=True if row[7] is None else bool(row[7]),
                created_at=row[8] or datetime.now(dt_timezone.utc),
                updated_at=row[9] or row[8] or datetime.now(dt_timezone.utc),
            )
            for row in rows
        ]
