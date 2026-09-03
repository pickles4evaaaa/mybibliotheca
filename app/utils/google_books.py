"""Google Books API URL helper."""

from __future__ import annotations

from urllib.parse import urlencode

from app.utils.google_books_settings import get_google_api_key

__all__ = ['google_books_url', 'BASE_URL']

BASE_URL = 'https://www.googleapis.com/books/v1'


def google_books_url(path: str = 'volumes', **params) -> str:
    """``{BASE_URL}/{path}?{params}``, plus ``&key=...`` when a key is configured.

    ``params`` use Google's own names (``q``, ``maxResults``, ``projection``);
    ``None`` values are dropped.
    """
    query = {k: v for k, v in params.items() if v is not None}
    key = get_google_api_key()
    if key:
        query['key'] = key
    url = f"{BASE_URL}/{path.strip('/')}"
    return f"{url}?{urlencode(query, safe=':')}" if query else url
