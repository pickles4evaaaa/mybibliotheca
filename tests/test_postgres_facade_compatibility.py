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
