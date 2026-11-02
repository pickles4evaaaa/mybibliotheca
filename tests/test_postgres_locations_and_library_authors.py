"""Regression tests for issues #255 (locations) and #256 (library author)."""

from contextlib import contextmanager
from datetime import datetime, timezone

from app.services.postgres_library_service import PostgresLibraryService
from app.services.postgres_location_service import PostgresLocationService

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class FakeCursor:
    def __init__(self, script):
        self.script = script
        self.queries = []
        self._result = []
        self.rowcount = 1

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params=None):
        self.queries.append((query, params))
        self._result = self.script(query, params)

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self):
        return list(self._result)


class FakeStore:
    def __init__(self, script):
        self.cursor_obj = FakeCursor(script)

    @contextmanager
    def connection(self):
        yield self

    transaction = connection

    def cursor(self):
        return self.cursor_obj


LOCATION_ROW = ("loc-1", "user-1", "Shelf", None, "home", None, False, True, NOW, NOW)


def test_get_location_returns_location_without_cache(monkeypatch):
    # Regression #255: the raw DB row has owner_user_id, which Location() rejects.
    service = PostgresLocationService(FakeStore(lambda q, p: [LOCATION_ROW]))
    location = service.get_location("loc-1")
    assert location.id == "loc-1" and location.name == "Shelf"
    assert location.user_id == "user-1"


def test_edit_and_delete_location_paths_work():
    def script(query, params):
        if "FROM locations WHERE id" in query and "FOR UPDATE" not in query:
            return [LOCATION_ROW]
        if "SELECT is_default" in query:
            return [(False,)]
        if "SELECT 1 FROM locations" in query:
            return [(1,)]
        return []

    service = PostgresLocationService(FakeStore(script))
    updated = service.update_location("loc-1", name="Renamed")
    assert updated is not None and updated.id == "loc-1"
    assert service.delete_location("loc-1") is True


def test_get_location_from_cache_roundtrip(monkeypatch):
    from app.infrastructure import redis_cache
    cache = {}
    monkeypatch.setattr(redis_cache, "snapshot_tag_versions", lambda tags: ())
    monkeypatch.setattr(redis_cache, "get_json", lambda key, **kw: (key in cache, cache.get(key)))
    monkeypatch.setattr(redis_cache, "set_json", lambda key, value, **kw: cache.__setitem__(key, value) or True)
    service = PostgresLocationService(FakeStore(lambda q, p: [LOCATION_ROW]))
    first = service.get_location("loc-1")
    second = service.get_location("loc-1")  # served from cache
    assert first == second


def test_set_book_location_syncs_user_book_primary_location():
    seen = []

    def script(query, params):
        seen.append(query)
        return [(1,)]

    service = PostgresLocationService(FakeStore(script))
    assert service.set_book_location("book-1", "loc-1", "user-1") is True
    assert any(q.startswith("UPDATE user_books SET location_id") for q in seen)


def test_library_rows_include_author():
    # Regression #256: library rows had no author/contributors.
    cols = len(__import__("app.services.postgres_library_service", fromlist=["_LIBRARY_KEYS"])._LIBRARY_KEYS)

    def script(query, params):
        if "FROM user_books ub" in query:
            row = [None] * cols
            row[1] = "book-1"
            row[31] = "A Title"
            return [tuple(row)]
        if "FROM book_contributors" in query:
            return [("book-1", "p1", "Ann Author", "authored", 0),
                    ("book-1", "p2", "Ed Editor", "edited", 1),
                    ("book-1", "p3", "Bo Co", "co_authored", 2)]
        return []

    service = PostgresLibraryService(FakeStore(script))
    rows = service.get_books_with_user_overlay_paginated_sync("user-1", 10, 0)
    assert rows[0]["author"] == "Ann Author, Bo Co"
    assert [a["name"] for a in rows[0]["authors"]] == ["Ann Author", "Bo Co"]
    assert rows[0]["contributors"][0]["person"]["name"] == "Ann Author"
