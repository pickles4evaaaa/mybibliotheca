"""Regression tests for issue #208: Library Journey timeline bucketed books
without the selected date into bogus years via date fallbacks."""
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

from app.routes import stats_routes
from app.routes.stats_helpers import _get_book_date


def _book(uid, status='read', finish=None, added='2024-05-01', start=None, pub=None):
    return {
        'uid': uid, 'title': f'Book {uid}', 'reading_status': status,
        'finish_date': finish, 'date_added': added, 'start_date': start,
        'publication_date': pub, 'authors': [], 'categories': [],
    }


BOOKS = [
    _book('a', finish='2019-03-10'),
    _book('b', finish='2020-07-01'),
    _book('c', finish='2020-12-25'),
    _book('d', finish=None, added='2004-01-01'),   # imported, never finished
    _book('e', status='library_only', finish=None, added='2004-06-01'),
]


def _run(query):
    app = Flask(__name__)
    app.secret_key = 'x'
    captured = {}

    def fake_render(template, **ctx):
        captured.update(ctx)
        return 'ok'

    user = SimpleNamespace(id='u1', is_authenticated=True)
    with patch.object(stats_routes, 'render_template', fake_render), \
         patch.object(stats_routes, 'current_user', user), \
         patch.object(stats_routes.book_service, 'get_all_books_with_user_overlay_sync',
                      return_value=list(BOOKS)):
        with app.test_request_context('/library-journey?' + query):
            stats_routes.library_journey.__wrapped__()
    return captured


def _uids(ctx):
    out = []
    for item in ctx['books']:
        out.extend(b['uid'] for b in (item['books'] if item['type'] == 'cluster' else [item['book']]))
    return sorted(out)


def test_finish_date_excludes_books_without_finish_date():
    ctx = _run('date_type=finish_date')
    assert _uids(ctx) == ['a', 'b', 'c']
    assert ctx['stats']['date_range'] == '2019 - 2020'


def test_finish_date_with_read_status():
    ctx = _run('date_type=finish_date&status=read')
    assert _uids(ctx) == ['a', 'b', 'c']
    assert '2004' not in ctx['stats']['date_range']


def test_year_range_filter_uses_finish_date_only():
    ctx = _run('date_type=finish_date&year_from=2020&year_to=2020')
    assert _uids(ctx) == ['b', 'c']


def test_no_matching_dates_yields_empty_timeline():
    ctx = _run('date_type=start_date')
    assert ctx['books'] == []


def test_get_book_date_does_not_fall_back():
    assert _get_book_date({'date_added': '2004-01-01'}, 'finish_date') is None
