import importlib.util
import sys
import types
from pathlib import Path
import requests


class DummyResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}")


def load_unified_metadata_module():
    module_name = "app.utils.unified_metadata"
    module_path = Path(__file__).resolve().parent.parent / "app" / "utils" / "unified_metadata.py"

    # Stub required app modules to avoid importing Flask-heavy app package
    app_mod = types.ModuleType("app")
    utils_mod = types.ModuleType("app.utils")

    metadata_settings = types.ModuleType("app.utils.metadata_settings")
    metadata_settings.apply_field_policy = lambda entity, field, g, o, m: m

    book_utils = types.ModuleType("app.utils.book_utils")
    book_utils.select_highest_google_image = lambda *_args, **_kwargs: None
    book_utils.upgrade_google_cover_url = lambda url: url

    sys.modules["app"] = app_mod
    sys.modules["app.utils"] = utils_mod
    sys.modules["app.utils.metadata_settings"] = metadata_settings
    sys.modules["app.utils.book_utils"] = book_utils

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _google_payload(isbn_value: str):
    return {
        "items": [
            {
                "id": "vol1",
                "volumeInfo": {
                    "title": "Wrong Volume",
                    "industryIdentifiers": [
                        {"type": "ISBN_13", "identifier": isbn_value},
                    ],
                },
            }
        ]
    }


def test_fetch_google_by_isbn_drops_mismatched_isbn(monkeypatch):
    """Ensure Google results with a different ISBN are ignored."""
    unified_metadata = load_unified_metadata_module()

    def fake_get(url, timeout=None, headers=None):
        if "googleapis.com/books/v1/volumes?q=isbn:" in url:
            return DummyResponse(_google_payload("9781591826071"))
        if "googleapis.com/books/v1/volumes/vol1" in url:
            return DummyResponse({})
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(requests, "get", fake_get)

    result = unified_metadata._fetch_google_by_isbn("9781591826040")
    assert result == {}


def test_unified_fetch_prefers_matching_metadata(monkeypatch):
    """When Google mismatches, fallback data should keep the requested ISBN."""
    unified_metadata = load_unified_metadata_module()

    requested_isbn = "9781591826040"

    def fake_get(url, timeout=None, headers=None):
        if "googleapis.com/books/v1/volumes?q=isbn:" in url:
            return DummyResponse(_google_payload("9781591826071"))
        if "googleapis.com/books/v1/volumes/vol1" in url:
            return DummyResponse({})
        if "openlibrary.org/api/books" in url:
            return DummyResponse(
                {
                    f"ISBN:{requested_isbn}": {
                        "title": "Fruits Basket, Vol. 2",
                        "identifiers": {"isbn_13": [requested_isbn]},
                        "publish_date": "2004",
                        "subjects": [{"name": "Comics"}],
                    }
                }
            )
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(requests, "get", fake_get)

    merged, errors = unified_metadata.fetch_unified_by_isbn_detailed(requested_isbn)

    assert merged["isbn13"] == requested_isbn
    assert merged["title"] == "Fruits Basket, Vol. 2"
    assert merged.get("_isbn_mismatch") is False
    assert errors.get("google") == "empty"
