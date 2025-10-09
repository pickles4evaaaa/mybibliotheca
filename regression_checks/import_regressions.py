import csv
import json
import sys
import types
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if 'kuzu' not in sys.modules:
    kuzu_stub = types.ModuleType('kuzu')

    class _DummyConnection:  # pragma: no cover - simple stub for imports
        pass

    class _DummyDatabase:  # pragma: no cover
        def __init__(self, *_args, **_kwargs):
            pass

    setattr(kuzu_stub, 'Connection', _DummyConnection)
    setattr(kuzu_stub, 'Database', _DummyDatabase)
    sys.modules['kuzu'] = kuzu_stub

from app.routes.import_routes import (
    detect_csv_format,
    auto_detect_fields,
    _auto_detect_reading_history_fields,
)

TEST_FILES_DIR = Path(__file__).resolve().parents[1] / 'test_files'


def _read_headers(filename: str):
    with (TEST_FILES_DIR / filename).open('r', encoding='utf-8') as handle:
        reader = csv.reader(handle)
        return next(reader)


def run_regression_suite():
    results = []

    csv_path = TEST_FILES_DIR / 'goodreads-mini.csv'
    fmt, confidence = detect_csv_format(csv_path)
    results.append(('detect_goodreads', fmt == 'goodreads' and confidence >= 0.3))

    csv_path = TEST_FILES_DIR / 'storygraph-mini.csv'
    fmt, confidence = detect_csv_format(csv_path)
    results.append(('detect_storygraph', fmt == 'storygraph' and confidence >= 0.3))

    headers = _read_headers('goodreads-mini.csv')
    mappings = auto_detect_fields(headers, user_id='user-123')
    results.append(('map_goodreads_title', mappings.get('Title') == 'title'))
    results.append(('map_goodreads_author', mappings.get('Author') == 'author'))
    results.append(('map_goodreads_isbn', mappings.get('ISBN13') == 'isbn'))
    results.append(('map_goodreads_shelves', mappings.get('Bookshelves') == 'custom_personal_goodreads_shelves'))

    headers = _read_headers('storygraph-mini.csv')
    mappings = auto_detect_fields(headers, user_id='user-123')
    results.append(('map_storygraph_title', mappings.get('Title') == 'title'))
    results.append(('map_storygraph_authors', mappings.get('Authors') == 'author'))
    results.append(('map_storygraph_status', mappings.get('Read Status') == 'reading_status'))

    headers = _read_headers('reading_history_template.csv')
    mappings = _auto_detect_reading_history_fields(headers)
    results.append(('map_reading_history_date', mappings.get('Date') == 'Date'))
    results.append(('map_reading_history_book', mappings.get('Book Name') == 'Book Name'))
    results.append(('map_reading_history_pages', mappings.get('Pages Read') == 'Pages Read'))
    results.append(('map_reading_history_minutes', mappings.get('Minutes Read') == 'Minutes Read'))

    success = all(status for _, status in results)
    return success, results


def main():
    success, results = run_regression_suite()
    print(json.dumps({'success': success, 'results': results}, indent=2))
    return 0 if success else 1


if __name__ == '__main__':
    raise SystemExit(main())
