from datetime import datetime
from types import SimpleNamespace
from pathlib import Path
import types
import json
import sys

from flask import Flask


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def create_app_with_stubs():
    if 'app.services' not in sys.modules:
        services_stub = types.ModuleType('app.services')
        setattr(services_stub, 'book_service', SimpleNamespace())
        setattr(services_stub, 'user_service', SimpleNamespace())
        setattr(services_stub, 'reading_log_service', SimpleNamespace())
        setattr(services_stub, 'run_async', lambda *args, **kwargs: None)
        setattr(services_stub, 'reset_all_services', lambda: None)
        sys.modules['app.services'] = services_stub

    if 'app.services.kuzu_service_facade' not in sys.modules:
        facade_module = types.ModuleType('app.services.kuzu_service_facade')

        class _StubFacade:
            def __init__(self, *args, **kwargs):
                pass

            def __getattr__(self, _name):
                def _noop(*_args, **_kwargs):
                    return None

                return _noop

        setattr(facade_module, 'KuzuServiceFacade', _StubFacade)
        sys.modules['app.services.kuzu_service_facade'] = facade_module

    if 'kuzu' not in sys.modules:
        kuzu_stub = types.ModuleType('kuzu')

        class _DummyConnection:
            pass

        class _DummyDatabase:
            def __init__(self, *_args, **_kwargs):
                pass

        setattr(kuzu_stub, 'Connection', _DummyConnection)
        setattr(kuzu_stub, 'Database', _DummyDatabase)
        sys.modules['kuzu'] = kuzu_stub

    """Create a Flask app registering API blueprints with stubbed services."""
    from app.api import books as books_module
    from app.api import reading_logs as logs_module
    from app.api import users as users_module
    import app.services as services_pkg

    app = Flask(__name__)
    app.config.update(TESTING=True, API_TEST_TOKEN='test-token')

    test_user = SimpleNamespace(
        id='user-123',
        username='tester',
        email='tester@example.com',
        is_authenticated=True,
        share_library=True,
        share_current_reading=True,
        share_reading_activity=True,
        is_admin=False,
        created_at=datetime(2024, 1, 1, 12, 0, 0),
        last_login=datetime(2024, 1, 2, 12, 0, 0),
    )

    # Replace current_user proxies
    books_module.current_user = test_user
    logs_module.current_user = test_user
    users_module.current_user = test_user

    class StubBookServices:
        def __init__(self):
            self.created = []
            self.updated = []
            self.deleted = []

        def get_all_books_with_user_overlay_sync(self, user_id):
            return [
                {
                    'id': 'book-1',
                    'title': 'Existing Title',
                    'subtitle': 'Existing Subtitle',
                    'isbn13': '9781234567890',
                    'authors': ['Author Example'],
                    'publisher': {'name': 'Example Publisher'},
                    'categories': ['Fiction'],
                    'cover_url': 'http://example.com/cover.jpg',
                    'created_at': '2024-01-01T00:00:00Z',
                    'updated_at': '2024-01-02T00:00:00Z',
                }
            ]

        def get_book_by_uid_sync(self, book_id, user_id):
            return {
                'id': book_id,
                'title': 'Existing Title',
                'isbn13': '9781234567890',
                'authors': ['Author Example'],
            }

        def create_book_sync(self, domain_book, user_id):
            self.created.append(domain_book)
            if isinstance(domain_book, dict):
                title = domain_book.get('title', 'Untitled')
                isbn13 = domain_book.get('isbn13')
            else:
                title = getattr(domain_book, 'title', 'Untitled')
                isbn13 = getattr(domain_book, 'isbn13', None)
            return {
                'id': 'book-created',
                'title': title,
                'isbn13': isbn13,
                'authors': ['Author Example'],
            }

        def update_book_sync(self, book_id, user_id, **updates):
            self.updated.append((book_id, updates))
            payload = {'id': book_id}
            payload.update(updates)
            return payload

        def delete_book_sync(self, book_id, user_id):
            self.deleted.append(book_id)
            return True

        def get_user_books_sync(self, user_id):
            return [
                SimpleNamespace(
                    title='Existing Title',
                    description='Some description',
                    contributors=[],
                    categories=[],
                    publisher=None,
                    authors=[],
                    reading_status=None,
                    series=None,
                    subtitle='Existing Subtitle',
                )
            ]

    class StubReadingLogService:
        def __init__(self):
            self.created = []
            self.checked = []

        def create_reading_log_sync(self, log_payload):
            self.created.append(log_payload)
            if isinstance(log_payload, dict):
                return {'id': 'log-created', **log_payload}
            return {'id': 'log-created'}

        def get_existing_log_sync(self, book_id, user_id, log_date):
            self.checked.append((book_id, user_id, log_date))
            if book_id == 'book-existing':
                return {
                    'id': 'log-existing',
                    'book_id': book_id,
                    'user_id': user_id,
                    'date': log_date,
                    'pages_read': 5,
                    'minutes_read': 0,
                }
            return None

    class StubUserService:
        def __init__(self, active_user):
            self.active_user = active_user
            self.updated = False

        def get_user_by_id_sync(self, user_id):
            if user_id == self.active_user.id:
                return self.active_user
            return None

        def update_user_sync(self, user):
            self.updated = True
            return user

        def get_all_users_sync(self):
            return [
                SimpleNamespace(
                    id='user-123',
                    username='tester',
                    created_at=datetime(2024, 1, 1, 12, 0, 0),
                    last_login=datetime(2024, 1, 2, 12, 0, 0),
                )
            ]

    book_stub = StubBookServices()
    reading_stub = StubReadingLogService()
    user_stub = StubUserService(test_user)

    books_module.kuzu_book_service = book_stub
    books_module.book_service = book_stub
    books_module.parse_book_data = lambda data: data

    logs_module.reading_log_service = reading_stub
    logs_module.DomainReadingLog = lambda **kwargs: kwargs
    logs_module.get_effective_reading_defaults = lambda _user_id: (5, 15)

    users_module.user_service = user_stub

    # Update lazy services module-wide to keep compatibility if routes reach for them
    services_pkg.book_service = book_stub
    services_pkg.reading_log_service = reading_stub
    services_pkg.user_service = user_stub

    app.register_blueprint(books_module.books_api)
    app.register_blueprint(logs_module.reading_logs_api)
    app.register_blueprint(users_module.users_api)

    return app, {'book_stub': book_stub, 'reading_stub': reading_stub, 'user_stub': user_stub, 'user': test_user}


def run_regression_suite():
    app, stubs = create_app_with_stubs()
    client = app.test_client()

    def _auth_headers():
        return {'Authorization': 'Bearer test-token'}

    results = []

    resp = client.get('/api/v1/books', headers=_auth_headers())
    results.append(('books_list', resp.status_code == 200))

    create_payload = {'title': 'New Title', 'isbn13': '9781111111111'}
    resp = client.post('/api/v1/books', json=create_payload, headers=_auth_headers())
    results.append(('books_create', resp.status_code == 201))

    update_payload = {'title': 'Updated Title'}
    resp = client.put('/api/v1/books/book-1', json=update_payload, headers=_auth_headers())
    results.append(('books_update', resp.status_code == 200))

    resp = client.delete('/api/v1/books/book-1', headers=_auth_headers())
    results.append(('books_delete', resp.status_code == 200))

    resp = client.post('/api/v1/reading-logs', json={'book_id': 'book-1', 'date': '2024-02-02'}, headers=_auth_headers())
    results.append(('reading_log_create', resp.status_code == 201))

    resp = client.post('/api/v1/reading-logs/check', json={'book_id': 'book-existing', 'date': '2024-02-02'}, headers=_auth_headers())
    payload = resp.get_json() if resp.is_json else {}
    results.append(('reading_log_check', resp.status_code == 200 and payload.get('exists') is True))

    resp = client.get('/api/v1/users/me', headers=_auth_headers(), follow_redirects=True)
    results.append(('users_me', resp.status_code == 200))

    resp = client.put('/api/v1/users/me', json={'share_library': False}, headers=_auth_headers(), follow_redirects=True)
    results.append(('users_update', resp.status_code == 200 and stubs['user_stub'].updated is True))

    resp = client.get('/api/v1/users/user-123', headers=_auth_headers(), follow_redirects=True)
    results.append(('users_profile', resp.status_code == 200))

    resp = client.get('/api/v1/users/', headers=_auth_headers(), follow_redirects=True)
    results.append(('users_list', resp.status_code == 200))

    all_passed = all(status for _, status in results)
    return all_passed, results


def main():
    success, results = run_regression_suite()
    print(json.dumps({'success': success, 'results': results}, indent=2))
    return 0 if success else 1


if __name__ == '__main__':
    raise SystemExit(main())
