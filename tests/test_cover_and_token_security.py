import logging

from flask import Flask
from flask_login import LoginManager

from app.api_auth import api_token_required
from app.routes.cover_routes import cover_bp
from app.services import cover_service as covers


def test_anonymous_cover_requests_are_denied():
    app = Flask(__name__)
    app.secret_key = 'test'
    login = LoginManager(app)
    login.user_loader(lambda _: None)
    app.register_blueprint(cover_bp)
    client = app.test_client()
    assert client.post('/api/cover/schedule', json={}).status_code == 401
    assert client.get('/api/cover/status/example').status_code == 401


def test_cover_capacity_and_expiry(monkeypatch):
    service = covers.CoverService()
    monkeypatch.setattr(covers, '_COVER_JOBS', {})
    monkeypatch.setattr(covers, '_MAX_PENDING_JOBS', 1)
    monkeypatch.setattr(service, 'select_candidate', lambda **kw: {'url': 'https://example.org/cover'})
    tasks = []
    monkeypatch.setattr(covers._EXECUTOR, 'submit', tasks.append)
    monkeypatch.setattr(service, 'fetch_and_cache', lambda **kw: None)
    first = service.schedule_async_processing()
    assert service.schedule_async_processing() is None
    tasks.pop()()
    assert service.get_job(first['id'])['status'] == 'failed'
    monkeypatch.setattr(covers, '_JOB_TTL_SECONDS', 0)
    assert service.get_job(first['id']) is None
    assert service.schedule_async_processing()


def test_bearer_credentials_stay_out_of_logs(caplog):
    app = Flask(__name__)
    app.config['API_TEST_TOKEN'] = 'configured-secret'

    @app.get('/api/check')
    @api_token_required
    def check():
        return 'ok'

    with caplog.at_level(logging.INFO):
        for token, status in [('configured-secret', 200), ('rejected-secret', 401)]:
            response = app.test_client().get('/api/check', headers={'Authorization': f'Bearer {token}'})
            assert response.status_code == status
    assert 'configured' not in caplog.text
    assert 'rejected-se' not in caplog.text


def test_completed_job_exposes_cached_cover(monkeypatch):
    from types import SimpleNamespace
    from flask import current_app

    app = Flask(__name__)
    service = covers.CoverService()
    monkeypatch.setattr(covers, '_COVER_JOBS', {})
    monkeypatch.setattr(service, 'select_candidate', lambda **kw: {'url': 'https://example.org/cover'})
    tasks = []
    monkeypatch.setattr(covers._EXECUTOR, 'submit', tasks.append)

    def fetch(**kwargs):
        assert current_app._get_current_object() is app
        return SimpleNamespace(cached_url='/covers/processed.jpg')

    monkeypatch.setattr(service, 'fetch_and_cache', fetch)
    with app.app_context():
        job = service.schedule_async_processing()
    tasks.pop()()
    completed = service.get_job(job['id'])
    assert completed['status'] == 'done'
    assert completed['processed_url'] == '/covers/processed.jpg'
