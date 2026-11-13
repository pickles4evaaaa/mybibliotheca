"""Regression tests for issues #261 (status tiles), #262 (aliases), #263 (reading log page)."""

from contextlib import contextmanager
from pathlib import Path

import jinja2

from app.domain.models import Category, normalize_aliases
from app.services.postgres_library_service import PostgresLibraryService
from app.services.postgres_reading_log_service import PostgresReadingLogService

ROOT = Path(__file__).resolve().parents[1]


class FakeCursor:
    def __init__(self, script):
        self.script = script
        self.queries = []
        self._result = []

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


# --- #261 -------------------------------------------------------------------

def test_status_counts_use_the_keys_the_library_tiles_read():
    rows = [("reading", "owned", 3), ("read", "owned", 16), ("plan_to_read", "owned", 14),
            ("plan_to_read", "wishlist", 2), ("library_only", "owned", 0), ("", "", 1),
            ("on_hold", "owned", 1), ("currently_reading", "owned", 1)]
    counts = PostgresLibraryService(FakeStore(lambda q, p: rows)).get_library_status_counts_sync("user-1")
    assert counts == {"read": 16, "currently_reading": 4, "on_hold": 1, "plan_to_read": 16, "wishlist": 2}


# --- #262 -------------------------------------------------------------------

def test_normalize_aliases_unwraps_repr_artifacts():
    assert normalize_aliases(["[]"]) == []
    assert normalize_aliases(["['[]']"]) == []
    assert normalize_aliases("[]") == []
    assert normalize_aliases(None) == []
    assert normalize_aliases("['Sci-Fi', 'SF']") == ["Sci-Fi", "SF"]
    assert normalize_aliases("Sci-Fi\n SF \n\nSci-Fi") == ["Sci-Fi", "SF"]
    assert normalize_aliases(["Sci-Fi", "SF"]) == ["Sci-Fi", "SF"]
    # Bracketed text that is not a list-of-strings repr is a real alias.
    assert normalize_aliases(["[sic]", "[1984]"]) == ["[sic]", "[1984]"]


def test_category_cleans_aliases_from_any_backend_shape():
    assert Category(name="parenting", aliases=["[]"]).aliases == []
    assert Category(name="sf", aliases="Sci-Fi\nSF").aliases == ["Sci-Fi", "SF"]  # type: ignore[arg-type]


def test_edit_category_can_clear_aliases_and_prefills_one_per_line():
    source = (ROOT / "app/routes/genre_routes.py").read_text()
    assert "else getattr(category, 'aliases', [])" not in source
    assert "form.aliases.data = '\\n'.join(normalize_aliases(" in source


# --- #263 -------------------------------------------------------------------

def test_reading_stats_are_scoped_to_the_book_when_filtered():
    store = FakeStore(lambda q, p: [(7, 528, 0, 1, 7)])
    stats = PostgresReadingLogService(store).get_user_all_time_reading_stats_sync("user-1", book_id="book-1")
    query, params = store.cursor_obj.queries[0]
    assert "book_id=%s" in query and params == ("user-1", "book-1")
    assert stats["total_log_entries"] == 7 and stats["total_pages"] == 528 and stats["distinct_days"] == 7

    store = FakeStore(lambda q, p: [(21, 839, 1620, 5, 20)])
    PostgresReadingLogService(store).get_user_all_time_reading_stats_sync("user-1")
    query, params = store.cursor_obj.queries[0]
    assert "AND book_id" not in query and params == ("user-1",)


def test_reading_log_route_serves_a_full_page_unless_partial_requested():
    source = (ROOT / "app/routes/stats_routes.py").read_text()
    assert "reading_logs/user_logs_page.html" in source
    page = (ROOT / "app/templates/reading_logs/user_logs_page.html").read_text()
    assert '{% extends "base.html" %}' in page
    assert '{% include "reading_logs/user_logs.html" %}' in page
    # The stats dashboard embeds the table, so it must ask for the bare partial.
    assert "fetch('/stats/reading-logs?partial=1')" in (ROOT / "app/templates/stats.html").read_text()


def _render_logs_partial(**context):
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(str(ROOT / "app/templates")), autoescape=True)

    def url_for(endpoint, **values):
        query = "&".join(f"{k}={v}" for k, v in values.items() if v is not None)
        return f"/{endpoint}" + (f"?{query}" if query else "")

    env.globals.update(url_for=url_for, csrf_token=lambda: "token")
    env.filters.setdefault("tojson", lambda value: value)
    base = {
        "logs": [],
        "pagination": {"page": 2, "per_page": 10, "total_count": 35, "total_pages": 4,
                       "has_prev": True, "has_next": True},
        "all_time_stats": {"total_log_entries": 7, "total_pages": 528, "total_minutes": 0,
                           "distinct_books": 1, "distinct_days": 6, "total_time_formatted": "0m"},
        "valid_per_page": [10, 25, 50, 100],
        "show_all": False,
        "book_id": None,
    }
    base.update(context)
    return env.get_template("reading_logs/user_logs.html").render(**base)


def test_reading_log_partial_keeps_book_filter_in_navigation():
    html = _render_logs_partial(book_id="book-1")
    assert "Days Read" in html and "Distinct Books" not in html
    assert "per_page=25&amp;book_id=book-1" in html
    assert "loadPage" not in html and 'data-page="' not in html

    html = _render_logs_partial()
    assert "Distinct Books" in html and "book_id=" not in html


def test_reading_log_route_scopes_totals_and_picks_template(monkeypatch):
    from flask import Flask
    from flask_login import LoginManager

    import app.routes.stats_routes as stats_routes

    class _User:
        id = "user-1"
        is_active = True
        is_authenticated = True
        is_anonymous = False

        def get_id(self):
            return self.id

    calls = {}

    class _Service:
        def get_user_reading_logs_paginated_sync(self, user_id, page=1, per_page=25, book_id=None):
            calls["page"] = (user_id, page, per_page, book_id)
            return {"logs": [], "pagination": {}}

        def get_user_all_time_reading_stats_sync(self, user_id, book_id=None):
            calls["stats"] = (user_id, book_id)
            return {}

    rendered = {}

    def _render(template, **context):
        rendered.update(template=template, **context)
        return "OK"

    monkeypatch.setattr(stats_routes, "reading_log_service", _Service())
    monkeypatch.setattr(stats_routes, "render_template", _render)

    flask_app = Flask(__name__)
    flask_app.secret_key = "test-only"
    LoginManager(flask_app).user_loader(lambda _user_id: _User())
    flask_app.add_url_rule("/stats/reading-logs", view_func=stats_routes.reading_logs)
    client = flask_app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = _User.id
        session["_fresh"] = True

    assert client.get("/stats/reading-logs?book_id=book-1&per_page=all").status_code == 200
    assert rendered["template"] == "reading_logs/user_logs_page.html"
    assert calls["stats"] == ("user-1", "book-1")
    assert calls["page"] == ("user-1", 1, 500, "book-1")
    assert rendered["show_all"] is True and rendered["book_id"] == "book-1"

    assert client.get("/stats/reading-logs?partial=1&per_page=7&page=x").status_code == 200
    assert rendered["template"] == "reading_logs/user_logs.html"
    assert calls["stats"] == ("user-1", None)
    assert calls["page"] == ("user-1", 1, 25, None)
