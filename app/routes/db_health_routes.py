"""Lightweight, backend-neutral database integrity endpoint."""

from flask import Blueprint, jsonify

from app.services import book_service, user_service

# Lightweight health/introspection blueprint to validate the configured store.
db_health = Blueprint("db_health", __name__, url_prefix="/api/db")


@db_health.get("/integrity")
def db_integrity():
    """Return basic integrity info: user/book counts and backend readiness."""
    info = {"initialized": False, "backend": "unknown"}
    try:
        facade = getattr(book_service, "_service", None)
        if facade is None:
            # Resolve the lazy service and keep the health check consistent
            # with normal application traffic.
            from app.services import _get_book_service
            facade = _get_book_service()
        info["backend"] = "postgres" if hasattr(facade, "store") else "kuzu"
        user_count = int(user_service.get_user_count_sync())
        book_count = int(book_service.get_total_book_count_sync())
        info.update(
            initialized=True,
            user_count=user_count,
            book_count=book_count,
            empty_database=(user_count == 0 and book_count == 0),
        )
    except Exception as exc:
        info.update(query_error=str(exc), user_count=0, book_count=0, empty_database=True)
    return jsonify(info)
