from contextlib import contextmanager

from app.services.postgres_service_facade import PostgresServiceFacade
from app.services.postgres_catalog_service import PostgresCatalogService


def test_legacy_bulk_import_book_lookup_alias_uses_postgres_book_service():
    expected = object()

    class BookService:
        def get_book_by_id_sync(self, book_id):
            assert book_id == "book-1"
            return expected

    facade = PostgresServiceFacade.__new__(PostgresServiceFacade)
    facade.book_service = BookService()

    assert facade.get_book_sync("book-1") is expected


def test_person_book_records_include_uid_alias_in_both_scopes():
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, _query, _params):
            pass

        def fetchall(self):
            return [("book-1", "A Book", None, None, None, None, "authored", "authored")]

    class Connection:
        def cursor(self):
            return Cursor()

    class Store:
        @contextmanager
        def connection(self):
            yield Connection()

    service = PostgresCatalogService(Store())

    all_books = service.get_books_by_person_sync("person-1")
    user_books = service.get_books_by_person_sync("person-1", "user-1")

    assert all_books[0]["id"] == all_books[0]["uid"] == "book-1"
    assert user_books["authored"][0]["id"] == user_books["authored"][0]["uid"] == "book-1"


def test_get_books_for_user_passes_pagination_as_keywords():
    class LibraryService:
        def get_books_with_user_overlay_paginated_sync(self, user_id, limit, offset):
            assert (user_id, limit, offset) == ("user-1", 17, 34)
            return [{"id": "book-1"}]

    facade = PostgresServiceFacade.__new__(PostgresServiceFacade)
    facade.library_service = LibraryService()
    facade._overlay_to_book = lambda row: row

    assert facade.get_books_for_user("user-1", 17, 34) == [{"id": "book-1"}]


def test_category_book_results_are_user_scoped_and_have_uid_alias():
    class Cursor:
        query = ""
        params = ()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, query, params):
            self.query = query
            self.params = params

        def fetchall(self):
            return [("book-1", "A Book", None, None, None, None, "reading", "owned", None, None)]

    cursor = Cursor()

    class Connection:
        def cursor(self):
            return cursor

    class Store:
        @contextmanager
        def connection(self):
            yield Connection()

    books = PostgresCatalogService(Store()).get_books_by_category_sync("category-1", "user-1")

    assert "JOIN user_books ub ON ub.book_id = b.id AND ub.user_id = %s" in cursor.query
    assert "LEFT JOIN user_books" not in cursor.query
    assert cursor.params == ("user-1", "category-1")
    assert books[0]["id"] == books[0]["uid"] == "book-1"
