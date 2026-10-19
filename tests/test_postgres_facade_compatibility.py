from app.services.postgres_service_facade import PostgresServiceFacade


def test_legacy_bulk_import_book_lookup_alias_uses_postgres_book_service():
    expected = object()

    class BookService:
        def get_book_by_id_sync(self, book_id):
            assert book_id == "book-1"
            return expected

    facade = PostgresServiceFacade.__new__(PostgresServiceFacade)
    facade.book_service = BookService()

    assert facade.get_book_sync("book-1") is expected
