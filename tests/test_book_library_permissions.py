"""Library isolation checks using a disposable POSTGRES_TEST_DSN."""
import os
from uuid import uuid4

import pytest

from app.domain.models import Book
from app.infrastructure.postgres_store import PostgresStore
from app.services.postgres_service_facade import PostgresServiceFacade


@pytest.fixture
def facade():
    dsn = os.getenv("POSTGRES_TEST_DSN")
    if not dsn:
        pytest.skip("requires a disposable PostgreSQL database")
    store = PostgresStore(None if dsn == "compose" else dsn)
    try:
        store.initialize_schema()
        with store.shared_transaction() as connection, connection.transaction(force_rollback=True):
            yield PostgresServiceFacade(store)
    finally:
        store.close()


def test_membership_controls_book_changes_and_suggestions(facade):
    store = facade.store
    owner, reader = (str(uuid4()) for _ in range(2))
    with store.transaction() as connection, connection.cursor() as cursor:
        for uid in (owner, reader):
            cursor.execute(
                "INSERT INTO users (id, username, email, password_hash) VALUES (%s, %s, %s, %s)",
                (uid, uid, f"{uid}@example.test", "unused"),
            )
    book = facade.create_book_sync(Book(title="Original"), owner)
    assert facade.update_book_sync(book.id, reader, title="Unauthorized") is None
    assert not facade.delete_book_sync(book.id, reader)
    assert facade.get_recently_added_books_sync(reader) == []
    assert facade.get_book_by_uid_sync(book.id, owner).title == "Original"
    assert facade.update_book_sync(book.id, owner, title="Revised").title == "Revised"
    assert [item.id for item in facade.get_recently_added_books_sync(owner)] == [book.id]

    facade.add_book_to_user_library_sync(reader, book.id)
    assert facade.update_book_sync(book.id, reader, title="Shared edit", personal_notes="mixed") is None
    assert not facade.get_book_by_uid_sync(book.id, reader).personal_notes
    assert facade.update_book_sync(book.id, reader, personal_notes="Private").personal_notes == "Private"
    assert not facade.get_book_by_uid_sync(book.id, owner).personal_notes
    assert facade.delete_book_sync(book.id, reader)
    assert not facade.delete_book_sync(book.id, reader)
    assert facade.get_book_by_uid_sync(book.id, reader) is None
    assert facade.get_book_by_uid_sync(book.id, owner).title == "Revised"


def test_book_api_and_reading_picker_use_the_signed_in_library(facade, monkeypatch):
    from flask import Flask
    from flask_login import LoginManager, UserMixin
    from app.api import books
    from app.routes import reading_log_routes

    app = Flask(__name__)
    app.secret_key = "test-session"
    login = LoginManager(app)
    user = UserMixin()
    user.id = str(uuid4())
    login.user_loader(lambda uid: user if uid == user.id else None)
    with facade.store.connection() as connection:
        connection.execute(
            "INSERT INTO users (id, username, email, password_hash) VALUES (%s, %s, %s, %s)",
            (user.id, user.id, f"{user.id}@example.test", "unused"),
        )
    monkeypatch.setattr(books, "book_service", facade)
    monkeypatch.setattr(reading_log_routes, "book_service", facade)
    monkeypatch.setattr(reading_log_routes, "reading_log_service", facade.reading_log_service)
    app.register_blueprint(books.books_api)
    app.register_blueprint(reading_log_routes.reading_logs)
    client = app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = user.id
    supplied_id = str(uuid4())
    response = client.post("/api/v1/books", json={"id": supplied_id, "title": "My book"}, follow_redirects=True)
    assert response.status_code == 201
    book_id = response.json["data"]["id"]
    assert book_id and book_id != supplied_id
    picker = client.get("/reading-logs/api/user/books")
    assert picker.status_code == 200
    assert [book["id"] for book in picker.json["books"]] == [book_id]
    assert client.delete(f"/api/v1/books/{book_id}").status_code == 200
    assert client.get("/reading-logs/api/user/books").json["books"] == []


def test_library_sharing_and_reading_activity_privacy(facade, monkeypatch):
    from datetime import date
    from flask import Flask
    from flask_login import LoginManager
    from app import services
    from app.api import users
    from app.domain.models import ReadingLog

    owner, reader = (str(uuid4()) for _ in range(2))
    with facade.store.connection() as connection:
        for uid in (owner, reader):
            connection.execute(
                "INSERT INTO users (id, username, email, password_hash) VALUES (%s,%s,%s,%s)",
                (uid, uid, f"{uid}@example.test", "unused"),
            )
    book = facade.create_book_sync(Book(title="Public title"), owner)
    facade.update_book_sync(book.id, owner, personal_notes="Owner notes")
    logs = facade.reading_log_service
    entry = ReadingLog(user_id=owner, book_id=book.id, date=date.today(), pages_read=10)
    saved = logs.create_reading_log_sync(entry)
    assert saved
    entry.user_id = reader
    assert logs.create_reading_log_sync(entry) is None
    assert facade.get_book_by_uid_sync(book.id, reader) is None
    general = ReadingLog(user_id=reader, date=date.today(), minutes_read=5)
    general_id = logs.create_reading_log_sync(general)["id"]
    assert logs.update_reading_log_sync(general_id, entry) is None
    assert logs.get_reading_log_by_id_sync(general_id, reader)["book_id"] is None

    app = Flask(__name__)
    app.secret_key = "test-session"
    LoginManager(app).user_loader(facade.user_service.get_user_by_id_sync)
    monkeypatch.setattr(users, "user_service", facade.user_service)
    monkeypatch.setattr(services, "book_service", facade)
    app.register_blueprint(users.users_api)
    client = app.test_client()
    library = f"/api/v1/users/{owner}/library"
    with client.session_transaction() as session:
        session["_user_id"] = owner
    assert client.get(library).json["data"][0]["personal_notes"] == "Owner notes"
    assert client.put("/api/v1/users/me", json={"share_library": "false"}).status_code == 400
    assert facade.user_service.get_user_by_id_sync(owner).share_library is False
    assert any(row["id"] == saved["id"] for row in logs.get_recent_shared_logs_sync())
    assert client.put("/api/v1/users/me", json={"share_library": True, "share_reading_activity": False}).status_code == 200
    assert all(row["id"] != saved["id"] for row in logs.get_recent_shared_logs_sync())
    with client.session_transaction() as session:
        session["_user_id"] = reader
    public_book = client.get(library).json["data"][0]
    assert public_book["title"] == book.title
    assert {"personal_notes", "reading_status", "custom_metadata", "location_id"}.isdisjoint(public_book)
    facade.user_service.update_privacy_settings_sync(owner, {"share_library": False})
    assert client.get(library).status_code == 403
