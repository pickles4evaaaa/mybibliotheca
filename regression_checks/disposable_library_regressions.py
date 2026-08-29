"""CRUD smoke test that is safe to run only against disposable Kuzu data.

The database path and project data volume are supplied by the Docker test
runner.  This script intentionally exercises the normal facade/service path
without ever opening the repository's real ``data/`` directory.
"""

from __future__ import annotations

import json

from regression_checks.refactor_safety import check_disposable_database_boundary


def run_regression_suite() -> dict[str, object]:
    boundary = check_disposable_database_boundary()

    from app import create_app
    from app.domain.models import Book
    from app.services import user_service
    from app.services.kuzu_service_facade import KuzuServiceFacade

    app = create_app()
    with app.app_context():
        user = user_service.create_user_sync(
            "refactor-user",
            "refactor@example.test",
            "disposable-password-hash",
            is_admin=False,
        )
        if not user or not user.id:
            raise AssertionError("could not create disposable test user")

        facade = KuzuServiceFacade()
        created = facade.create_book_sync(
            Book(
                title="Disposable Refactor Book",
                isbn13="9781234567890",
                cover_url="https://example.test/cover.jpg",
            ),
            user.id,
        )
        if not created or not created.id:
            raise AssertionError("could not create disposable test book")

        library = facade.get_all_books_with_user_overlay_sync(user.id)
        if not any(book.get("title") == "Disposable Refactor Book" for book in library):
            raise AssertionError("created book was not returned by library retrieval")

        updated = facade.update_book_sync(
            created.id,
            user.id,
            personal_notes="refactor smoke test",
            reading_status="reading",
        )
        if not updated:
            raise AssertionError("could not update disposable reading metadata")
        if not facade.get_book_by_uid_sync(created.id, user.id):
            raise AssertionError("updated book could not be read back")

        if not facade.delete_book_sync(created.id, user.id):
            raise AssertionError("could not delete disposable test book")
        if facade.get_book_by_uid_sync(created.id, user.id) is not None:
            raise AssertionError("deleted disposable book was still returned")

    return {"boundary": boundary, "success": True}


if __name__ == "__main__":
    print(json.dumps(run_regression_suite(), indent=2, sort_keys=True))
