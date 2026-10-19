"""PostgreSQL persistence for personal and shared reading history."""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

from ..domain.models import ReadingLog
from ..infrastructure import redis_cache
from ..infrastructure.postgres_store import PostgresStore, get_postgres_store

logger = logging.getLogger(__name__)

_LOG_COLUMNS = ("id", "user_id", "book_id", "date", "pages_read", "minutes_read", "notes", "created_at", "updated_at")


def _dict(columns: tuple[str, ...], row: Any) -> dict[str, Any]:
    return dict(zip(columns, row))


def _iso(value: Any) -> Any:
    return value.isoformat() if isinstance(value, (date, datetime)) else value


class PostgresReadingLogService:
    """Reading-log API with user scoping and transaction-safe daily merges."""

    def __init__(self, store: PostgresStore | None = None):
        self.store = store or get_postgres_store()

    def get_total_reading_log_count_sync(self) -> int:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM reading_logs")
            return int(cursor.fetchone()[0])

    def get_user_logs_count_sync(self, user_id: str) -> int:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM reading_logs WHERE user_id=%s", (str(user_id),))
            return int(cursor.fetchone()[0])

    def _cached_query(self, key: str, tags: tuple[str, ...], callback):
        snapshot = redis_cache.snapshot_tag_versions(tags)
        hit, value = redis_cache.get_json(key, tags=tags, snapshot=snapshot)
        if hit:
            return value
        value = callback()
        redis_cache.set_json(key, value, ttl_seconds=30, tags=tags, snapshot=snapshot)
        return value

    @staticmethod
    def _user_tag(user_id: str) -> str:
        return f"postgres:reading_logs:{user_id}"

    def _row(self, cursor: Any, log_id: str, user_id: str) -> dict[str, Any] | None:
        cursor.execute(
            f"SELECT {', '.join(_LOG_COLUMNS)} FROM reading_logs WHERE id=%s AND user_id=%s",
            (log_id, user_id),
        )
        row = cursor.fetchone()
        return _dict(_LOG_COLUMNS, row) if row else None

    def _with_book(self, log: dict[str, Any], book: dict[str, Any] | None) -> dict[str, Any]:
        log["book"] = book
        return log

    def get_recent_shared_logs_sync(self, days_back: int = 7, limit: int = 50) -> list[dict[str, Any]]:
        days_back = max(0, int(days_back))
        limit = max(1, min(int(limit), 1000))
        cache_key = f"postgres:reading_logs:shared:{days_back}:{limit}:{date.today().isoformat()}"
        tags = ("postgres:reading_logs", "postgres:users", "postgres:books", "postgres:people")
        snapshot = redis_cache.snapshot_tag_versions(tags)
        hit, cached = redis_cache.get_json(cache_key, tags=tags, snapshot=snapshot)
        if hit:
            return cached
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT rl.id,rl.user_id,rl.book_id,rl.date,rl.pages_read,rl.minutes_read,rl.notes,rl.created_at,rl.updated_at, "
                "u.id,u.username,u.display_name,u.share_reading_activity, to_jsonb(b), "
                "COALESCE(string_agg(DISTINCT p.name, ', ' ORDER BY p.name), '') AS author "
                "FROM reading_logs rl JOIN users u ON u.id=rl.user_id "
                "JOIN books b ON b.id=rl.book_id "
                "LEFT JOIN book_contributors bc ON bc.book_id=b.id "
                "LEFT JOIN people p ON p.id=bc.person_id "
                "WHERE u.share_reading_activity AND rl.date >= %s "
                "GROUP BY rl.id,u.id,b.id ORDER BY rl.date DESC NULLS LAST,rl.created_at DESC NULLS LAST,rl.id LIMIT %s",
                (date.today() - timedelta(days=days_back), limit),
            )
            rows = cursor.fetchall()
        results = []
        for row in rows:
            log = _dict(_LOG_COLUMNS, row[:9])
            log["user"] = {"id": row[9], "username": row[10], "display_name": row[11], "share_reading_activity": row[12]}
            book = dict(row[13] or {})
            book["author"] = row[14] or ""
            log["book"] = book
            results.append(log)
        redis_cache.set_json(cache_key, results, ttl_seconds=30, tags=tags, snapshot=snapshot)
        return results

    def get_user_reading_logs_sync(self, user_id: str, days_back: int = 30, limit: int | None = 100) -> list[dict[str, Any]]:
        days_back = max(0, int(days_back))
        cache_key = f"postgres:reading_logs:user:{user_id}:{days_back}:{limit}:{date.today().isoformat()}"
        tags = (self._user_tag(user_id), "postgres:books")
        snapshot = redis_cache.snapshot_tag_versions(tags)
        hit, cached = redis_cache.get_json(cache_key, tags=tags, snapshot=snapshot)
        if hit:
            return cached
        params: list[Any] = [user_id, date.today() - timedelta(days=max(0, int(days_back)))]
        limit_sql = ""
        if limit is not None and int(limit) > 0:
            limit_sql = " LIMIT %s"
            params.append(min(int(limit), 10000))
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT rl.id,rl.user_id,rl.book_id,rl.date,rl.pages_read,rl.minutes_read,rl.notes,rl.created_at,rl.updated_at, "
                "to_jsonb(b) "
                "FROM reading_logs rl LEFT JOIN books b ON b.id=rl.book_id "
                "WHERE rl.user_id=%s AND rl.date >= %s ORDER BY rl.date DESC NULLS LAST,rl.created_at DESC NULLS LAST,rl.id" + limit_sql,
                tuple(params),
            )
            rows = cursor.fetchall()
        results = [self._with_book(_dict(_LOG_COLUMNS, row[:9]), dict(row[9]) if row[9] else None) for row in rows]
        redis_cache.set_json(cache_key, results, ttl_seconds=30, tags=tags, snapshot=snapshot)
        return results

    def get_book_reading_logs_sync(self, user_id: str, book_id: str, limit: int = 10) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 1000))
        cache_key = f"postgres:reading_logs:book:{user_id}:{book_id}:{limit}"
        tags = (self._user_tag(user_id), "postgres:books")
        snapshot = redis_cache.snapshot_tag_versions(tags)
        hit, cached = redis_cache.get_json(cache_key, tags=tags, snapshot=snapshot)
        if hit:
            return cached
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {', '.join(_LOG_COLUMNS)} FROM reading_logs WHERE user_id=%s AND book_id=%s "
                "ORDER BY date DESC NULLS LAST,created_at DESC NULLS LAST,id LIMIT %s", (user_id, book_id, limit),
            )
            results = [_dict(_LOG_COLUMNS, row) for row in cursor.fetchall()]
        redis_cache.set_json(cache_key, results, ttl_seconds=30, tags=tags, snapshot=snapshot)
        return results

    def get_user_reading_dates_sync(self, user_id: str, days_back: int | None = None) -> list[date]:
        cache_key = f"postgres:reading_logs:dates:{user_id}:{days_back}"
        tags = (self._user_tag(user_id),)
        snapshot = redis_cache.snapshot_tag_versions(tags)
        hit, cached = redis_cache.get_json(cache_key, tags=tags, snapshot=snapshot)
        if hit:
            return cached
        query = "SELECT DISTINCT date FROM reading_logs WHERE user_id=%s AND date IS NOT NULL"
        params: list[Any] = [user_id]
        if days_back is not None:
            query += " AND date >= %s"
            params.append(date.today() - timedelta(days=max(0, int(days_back))))
        query += " ORDER BY date DESC"
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(query, tuple(params))
            results = [row[0] for row in cursor.fetchall() if row[0] is not None]
        redis_cache.set_json(cache_key, results, ttl_seconds=30, tags=tags, snapshot=snapshot)
        return results

    @staticmethod
    def _log_payload(log: ReadingLog) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        return {
            "id": str(log.id or uuid.uuid4()), "user_id": str(log.user_id),
            "book_id": str(log.book_id) if log.book_id else None,
            "date": log.date, "pages_read": int(log.pages_read or 0),
            "minutes_read": int(log.minutes_read or 0), "notes": log.notes,
            "created_at": log.created_at or now, "updated_at": log.updated_at or now,
        }

    def create_reading_log_sync(self, reading_log: ReadingLog) -> dict[str, Any] | None:
        payload = self._log_payload(reading_log)
        committed = False
        try:
            with self.store.transaction() as connection, connection.cursor() as cursor:
                # Serialize same-user/day writes, including bookless logs, so concurrent imports
                # cannot create duplicate daily rows when merging existing entries.
                lock_key = f"reading-log:{payload['user_id']}:{payload['book_id'] or 'none'}:{payload['date']}"
                cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (lock_key,))
                cursor.execute(
                    "SELECT id,pages_read,minutes_read,notes FROM reading_logs WHERE user_id=%s AND book_id IS NOT DISTINCT FROM %s AND date IS NOT DISTINCT FROM %s "
                    "ORDER BY created_at NULLS FIRST,id LIMIT 1 FOR UPDATE",
                    (payload["user_id"], payload["book_id"], payload["date"]),
                )
                existing = cursor.fetchone()
                if existing:
                    old_notes, new_notes = existing[3] or "", payload["notes"] or ""
                    combined_notes = f"{old_notes}\n\n{new_notes}" if old_notes and new_notes else (new_notes or old_notes or None)
                    log_id = str(existing[0])
                    # A caller-supplied stable ID makes a retried create idempotent.
                    if log_id != payload["id"]:
                        cursor.execute(
                            "UPDATE reading_logs SET pages_read=%s,minutes_read=%s,notes=%s,updated_at=%s WHERE id=%s",
                            (int(existing[1] or 0) + payload["pages_read"], int(existing[2] or 0) + payload["minutes_read"],
                             combined_notes, payload["updated_at"], existing[0]),
                        )
                else:
                    log_id = payload["id"]
                    cursor.execute(
                        "INSERT INTO reading_logs(id,user_id,book_id,date,pages_read,minutes_read,notes,created_at,updated_at) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (log_id, payload["user_id"], payload["book_id"], payload["date"], payload["pages_read"],
                         payload["minutes_read"], payload["notes"], payload["created_at"], payload["updated_at"]),
                    )
                result = self._row(cursor, log_id, payload["user_id"])
                if payload["book_id"] and payload["date"]:
                    start_date = datetime(payload["date"].year, payload["date"].month, payload["date"].day, tzinfo=timezone.utc)
                    cursor.execute(
                        "INSERT INTO user_books(user_id,book_id,reading_status,ownership_status,start_date,date_added,created_at,updated_at) "
                        "VALUES (%s,%s,'','owned',%s,now(),now(),now()) "
                        "ON CONFLICT(user_id,book_id) DO UPDATE SET "
                        "start_date=COALESCE(user_books.start_date,EXCLUDED.start_date),updated_at=now()",
                        (payload["user_id"], payload["book_id"], start_date),
                    )
            committed = True
            if result:
                redis_cache.invalidate_tags(self._user_tag(payload["user_id"]), f"postgres:user_books:{payload['user_id']}", "postgres:reading_logs")
                return {**result, "date": _iso(result["date"]), "created_at": _iso(result["created_at"]), "updated_at": _iso(result["updated_at"])}
            return None
        except Exception:
            if committed:
                raise
            logger.exception("Could not create or merge reading log for user %s", payload["user_id"])
            return None

    def delete_reading_log_sync(self, log_id: str, user_id: str) -> bool:
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute("DELETE FROM reading_logs WHERE id=%s AND user_id=%s", (log_id, user_id))
            deleted = cursor.rowcount == 1
        if deleted:
            redis_cache.invalidate_tags(self._user_tag(user_id), "postgres:reading_logs")
        return deleted

    def get_user_reading_stats_sync(self, user_id: str, days_back: int = 30) -> dict[str, Any]:
        days_back = max(0, int(days_back))
        cache_key = f"postgres:reading_logs:stats:{user_id}:{days_back}:{date.today().isoformat()}"
        tags = (self._user_tag(user_id),)
        snapshot = redis_cache.snapshot_tag_versions(tags)
        hit, cached = redis_cache.get_json(cache_key, tags=tags, snapshot=snapshot)
        if hit:
            return cached
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*),coalesce(sum(pages_read),0),coalesce(sum(minutes_read),0),count(DISTINCT date) "
                "FROM reading_logs WHERE user_id=%s AND date >= %s",
                (user_id, date.today() - timedelta(days=max(0, int(days_back)))),
            )
            sessions, pages, minutes, days = cursor.fetchone()
        days = int(days or 0)
        result = {"total_sessions": int(sessions or 0), "total_pages": int(pages or 0),
                "total_minutes": int(minutes or 0), "days_read": days,
                "avg_pages_per_day": int(pages or 0) / days if days else 0,
                "avg_minutes_per_day": int(minutes or 0) / days if days else 0, "days_back": int(days_back)}
        redis_cache.set_json(cache_key, result, ttl_seconds=30, tags=tags, snapshot=snapshot)
        return result

    def get_existing_log_sync(self, book_id: str, user_id: str, log_date: date) -> dict[str, Any] | None:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(f"SELECT {', '.join(_LOG_COLUMNS)} FROM reading_logs WHERE book_id=%s AND user_id=%s AND date=%s ORDER BY created_at,id LIMIT 1",
                           (book_id, user_id, log_date))
            row = cursor.fetchone()
        return _dict(_LOG_COLUMNS, row) if row else None

    def get_recently_read_books_sync(self, user_id: str, limit: int = 5) -> list[dict[str, Any]]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT to_jsonb(b),max(rl.date) "
                "FROM reading_logs rl JOIN books b ON b.id=rl.book_id WHERE rl.user_id=%s "
                "GROUP BY b.id ORDER BY max(rl.date) DESC NULLS LAST,b.id LIMIT %s",
                (user_id, max(0, min(int(limit), 1000))),
            )
            rows = cursor.fetchall()
        return [{**dict(row[0]), "latest_log_date": row[1], "authors": []} for row in rows]

    def get_user_all_time_reading_stats_sync(self, user_id: str) -> dict[str, Any]:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*),coalesce(sum(pages_read),0),coalesce(sum(minutes_read),0),count(DISTINCT book_id) FILTER (WHERE book_id IS NOT NULL),count(DISTINCT date) FILTER (WHERE date IS NOT NULL) "
                "FROM reading_logs WHERE user_id=%s", (user_id,),
            )
            entries, pages, minutes, books, days = cursor.fetchone()
        minutes = int(minutes or 0)
        return {"total_log_entries": int(entries or 0), "total_pages": int(pages or 0), "total_minutes": minutes,
                "distinct_books": int(books or 0), "distinct_days": int(days or 0),
                "total_time_formatted": f"{minutes // 1440}d {(minutes % 1440) // 60}h {minutes % 60}m" if minutes > 0 else "0m"}

    def get_user_reading_logs_paginated_sync(self, user_id: str, page: int = 1, per_page: int = 25,
                                              book_id: str | None = None) -> dict[str, Any]:
        page, per_page = max(1, int(page)), max(1, min(int(per_page), 500))
        cache_key = f"postgres:reading_logs:page:{user_id}:{page}:{per_page}:{book_id or ''}"
        tags = (self._user_tag(user_id), "postgres:books")
        snapshot = redis_cache.snapshot_tag_versions(tags)
        hit, cached = redis_cache.get_json(cache_key, tags=tags, snapshot=snapshot)
        if hit:
            return cached
        where = "user_id=%s" + (" AND book_id=%s" if book_id else "")
        params: list[Any] = [user_id] + ([book_id] if book_id else [])
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM reading_logs WHERE " + where, tuple(params))
            total = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT rl.id,rl.user_id,rl.book_id,rl.date,rl.pages_read,rl.minutes_read,rl.notes,rl.created_at,rl.updated_at, "
                "to_jsonb(b) FROM reading_logs rl "
                "LEFT JOIN books b ON b.id=rl.book_id WHERE " + where +
                " ORDER BY rl.date DESC NULLS LAST,rl.created_at DESC NULLS LAST,rl.id OFFSET %s LIMIT %s",
                tuple(params + [(page - 1) * per_page, per_page]),
            )
            rows = cursor.fetchall()
        logs = [self._with_book(_dict(_LOG_COLUMNS, row[:9]), dict(row[9]) if row[9] else None) for row in rows]
        pages = (total + per_page - 1) // per_page if total else 1
        result = {"logs": logs, "pagination": {"page": page, "per_page": per_page, "total_count": total,
                "total_pages": pages, "has_prev": page > 1, "has_next": page < pages}}
        redis_cache.set_json(cache_key, result, ttl_seconds=30, tags=tags, snapshot=snapshot)
        return result

    def update_reading_log_sync(self, log_id: str, reading_log: ReadingLog) -> dict[str, Any] | None:
        now = reading_log.updated_at or datetime.now(timezone.utc)
        with self.store.transaction() as connection, connection.cursor() as cursor:
            cursor.execute(
                "UPDATE reading_logs SET book_id=%s,date=%s,pages_read=%s,minutes_read=%s,notes=%s,updated_at=%s "
                "WHERE id=%s AND user_id=%s",
                (reading_log.book_id, reading_log.date, int(reading_log.pages_read or 0), int(reading_log.minutes_read or 0),
                 reading_log.notes, now, log_id, reading_log.user_id),
            )
            if cursor.rowcount != 1:
                return None
            row = self._row(cursor, log_id, reading_log.user_id)
        if row:
            redis_cache.invalidate_tags(self._user_tag(reading_log.user_id), "postgres:reading_logs")
            return {**row, "date": _iso(row["date"]), "created_at": _iso(row["created_at"]), "updated_at": _iso(row["updated_at"])}
        return None

    def get_reading_log_by_id_sync(self, log_id: str, user_id: str) -> dict[str, Any] | None:
        with self.store.connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                f"SELECT {', '.join('rl.' + column for column in _LOG_COLUMNS)},to_jsonb(b) "
                "FROM reading_logs rl LEFT JOIN books b ON b.id=rl.book_id WHERE rl.id=%s AND rl.user_id=%s",
                (log_id, user_id),
            )
            row = cursor.fetchone()
        if not row:
            return None
        result = _dict(_LOG_COLUMNS, row[:9])
        result["book"] = dict(row[9]) if row[9] else None
        return result
