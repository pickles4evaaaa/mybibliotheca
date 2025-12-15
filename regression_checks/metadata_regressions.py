"""Regression checks for unified metadata fetching logic."""

from __future__ import annotations

from pathlib import Path
import json
import sys
import types
from typing import Any, Dict, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if 'app' not in sys.modules:
    app_pkg = types.ModuleType('app')
    app_pkg.__path__ = [str(PROJECT_ROOT / 'app')]
    sys.modules['app'] = app_pkg
else:
    app_pkg = sys.modules['app']

if 'app.utils' not in sys.modules:
    utils_pkg = types.ModuleType('app.utils')
    utils_pkg.__path__ = [str(PROJECT_ROOT / 'app' / 'utils')]
    sys.modules['app.utils'] = utils_pkg
    setattr(app_pkg, 'utils', utils_pkg)
else:
    utils_pkg = sys.modules['app.utils']

if 'app.utils.book_utils' not in sys.modules:
    book_utils_stub = types.ModuleType('app.utils.book_utils')

    def _select_highest_google_image(image_links):
        return None if not image_links else next(iter(image_links.values()), None)

    def _upgrade_google_cover_url(url):
        return url

    book_utils_stub.select_highest_google_image = _select_highest_google_image
    book_utils_stub.upgrade_google_cover_url = _upgrade_google_cover_url
    sys.modules['app.utils.book_utils'] = book_utils_stub
    setattr(utils_pkg, 'book_utils', book_utils_stub)

from app.utils import unified_metadata


def _openlibrary_payload(
    isbn13: str,
    title: str = 'Fruits Basket, Vol. 2',
    isbn10: str = '1591826045',
) -> Dict[str, Any]:
    return {
        'title': title,
        'subtitle': None,
        'publisher': 'Tokyopop',
        'authors': ['Natsuki Takaya'],
        'published_date': '2004-02-10',
        'published_date_specificity': 3,
        'page_count': 192,
        'language': 'en',
        'description': 'Second volume metadata',
        'categories': ['Comics & Graphic Novels / Manga'],
        'raw_category_paths': ['Comics & Graphic Novels / Manga'],
        'average_rating': None,
        'rating_count': None,
        'cover_url': None,
        'isbn10': isbn10,
        'isbn13': isbn13,
        'openlibrary_id': 'OL12345W',
        'series': 'Fruits Basket',
    }


def _google_payload(isbn13: str, title: str) -> Dict[str, Any]:
    return {
        'title': title,
        'subtitle': None,
        'publisher': 'Tokyopop',
        'authors': ['Natsuki Takaya'],
        'published_date': '2003-01-01',
        'published_date_specificity': 3,
        'page_count': 208,
        'language': 'en',
        'description': 'Another volume',
        'categories': ['Series: Fruits Basket'],
        'raw_category_paths': ['Series: Fruits Basket'],
        'average_rating': 4.2,
        'rating_count': 100,
        'cover_url': 'https://example.test/cover.jpg',
        'isbn10': '1591826037',
        'isbn13': isbn13,
        'google_books_id': 'abc123',
        'series': 'Fruits Basket',
    }


def _run_case(case_name: str, google_payload: Dict[str, Any], openlib_payload: Dict[str, Any], expect_empty: bool) -> Tuple[str, bool, str]:
    """Execute a regression case by patching the fetch pair helper."""
    original_fetch = unified_metadata._unified_fetch_pair  # type: ignore[attr-defined]

    def _fake_pair(isbn: str):
        assert isbn == '9781591826040'
        return google_payload, openlib_payload, {}

    try:
        unified_metadata._unified_fetch_pair = _fake_pair  # type: ignore[attr-defined]
        merged, errors = unified_metadata.fetch_unified_by_isbn_detailed('9781591826040')
    finally:
        unified_metadata._unified_fetch_pair = original_fetch  # type: ignore[attr-defined]

    if openlib_payload:
        expected_title = openlib_payload['title']
    else:
        expected_title = None

    if expect_empty:
        passed = (
            merged == {}
            and errors.get('google') == 'isbn_mismatch'
            and errors.get('openlib') == 'isbn_mismatch'
        )
    else:
        passed = (
            merged.get('title') == expected_title
            and errors.get('google') == 'isbn_mismatch'
            and errors.get('openlib') in (None, 'empty')
        )

    detail = json.dumps({'merged_title': merged.get('title'), 'errors': errors})
    return case_name, passed, detail


def run_regression_suite():
    results = []

    # Case 1: Google returns Volume 1, OpenLibrary returns Volume 2. We expect OpenLibrary to win.
    case = _run_case(
        'google_mismatch_dropped',
        _google_payload('9781591826033', 'Fruits Basket, Vol. 1'),
        _openlibrary_payload('9781591826040', 'Fruits Basket, Vol. 2'),
        expect_empty=False,
    )
    results.append(case)

    # Case 2: Both providers mismatch requested ISBN; merged result should be empty.
    case = _run_case(
        'all_providers_mismatch',
        _google_payload('9781591826033', 'Fruits Basket, Vol. 1'),
        _openlibrary_payload('9781591826071', 'Fruits Basket, Vol. 5', isbn10='1591826078'),
        expect_empty=True,
    )
    results.append(case)

    success = all(passed for _, passed, _ in results)
    return success, results


def main():
    success, results = run_regression_suite()
    print(json.dumps({'success': success, 'results': results}, indent=2))
    return 0 if success else 1


if __name__ == '__main__':
    raise SystemExit(main())
