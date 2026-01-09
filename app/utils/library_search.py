from __future__ import annotations

from typing import Any, Iterable


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple, set)):
        return " ".join(_as_text(v) for v in value)
    return str(value)


def library_book_matches_query(book: dict[str, Any], query: str) -> bool:
    """Return True if `query` matches title/author/series/category/description.

    This helper is intentionally Flask- and DB-free so it can be unit-tested in
    isolation and reused by routes/services.
    """

    if not query:
        return True

    tokens = [t.strip().lower() for t in query.split() if t.strip()]
    if not tokens:
        return True

    haystack_parts: Iterable[Any] = (
        book.get("title"),
        book.get("normalized_title"),
        book.get("subtitle"),
        book.get("author"),
        book.get("authors_text"),
        book.get("authors"),
        book.get("series"),
        book.get("series_name"),
        book.get("categories"),
        book.get("category"),
        book.get("description"),
    )

    haystack = " ".join(_as_text(p) for p in haystack_parts).lower()
    return all(token in haystack for token in tokens)
