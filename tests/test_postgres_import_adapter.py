import asyncio

from app.domain.models import ContributionType
from app.simplified_book_service import SimplifiedBook, SimplifiedBookService


def test_simplified_book_conversion_preserves_import_relationship_fields():
    service = SimplifiedBookService.__new__(SimplifiedBookService)
    imported = SimplifiedBook(
        title="A Test Book", author="First Author", additional_authors="Second Author",
        narrator="Narrator", publisher="Example Press", series="Test Series",
        categories=["Fiction", "Mystery"], published_date="2024",
        global_custom_metadata={"edition": "signed"}, media_type="ebook", quantity=2,
    )

    book = service._to_domain_book(imported)

    assert book.title == "A Test Book"
    assert book.published_date.isoformat() == "2024-01-01"
    assert [item.person.name for item in book.contributors] == ["First Author", "Second Author", "Narrator"]
    assert [item.contribution_type for item in book.contributors] == [
        ContributionType.AUTHORED, ContributionType.CO_AUTHORED, ContributionType.NARRATED,
    ]
    assert book.publisher.name == "Example Press"
    assert book.series.name == "Test Series"
    assert [item.name for item in book.categories] == ["Fiction", "Mystery"]
    assert book.custom_metadata == {"edition": "signed"}
    assert (book.media_type, book.quantity) == ("ebook", 2)


def test_postgres_import_creates_user_overlay_without_kuzu(monkeypatch):
    events = []

    class Facade:
        def add_book_to_user_library_sync(self, user_id, book_id, reading_status, ownership_status, locations, custom_metadata):
            events.append(("add", user_id, book_id, reading_status, ownership_status, locations, custom_metadata))
            return True

        def update_book_sync(self, book_id, user_id, **updates):
            events.append(("update", book_id, user_id, updates))

    import app.services as services

    monkeypatch.setattr(SimplifiedBookService, "_is_postgres_backend", staticmethod(lambda: True))
    monkeypatch.setattr(services, "_get_book_service", lambda: Facade())
    service = SimplifiedBookService.__new__(SimplifiedBookService)
    async def find_or_create(book_data):
        return "book-1"
    monkeypatch.setattr(service, "find_or_create_book", find_or_create)

    result = asyncio.run(service.add_book_to_user_library(
        SimplifiedBook(title="A Test Book", author="An Author", reading_status="read", date_read="2024-05-01"),
        "user-1", media_type="ebook", custom_metadata={"shelf": "blue"},
    ))
    assert result

    assert events[0] == ("add", "user-1", "book-1", "read", "owned", None, {"shelf": "blue"})
    assert events[1][0:3] == ("update", "book-1", "user-1")
    assert events[1][3]["finish_date"].isoformat() == "2024-05-01T00:00:00+00:00"
    assert events[1][3]["custom_metadata"] == {"shelf": "blue"}
