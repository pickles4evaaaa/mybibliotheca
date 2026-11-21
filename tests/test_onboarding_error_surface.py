import sys
from unittest.mock import patch
from pathlib import Path

from flask import Flask, session

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.infrastructure import persistence_state

# Importing the onboarding blueprint pulls in services that probe the active
# persistence backend at import time; avoid needing a live PostgreSQL server.
with patch.object(persistence_state, 'get_persistence_state', return_value={'backend': 'kuzu', 'migration_run_id': None}):
    import app.onboarding_system as ob  # noqa: E402
    import app.auth_settings as auth_settings  # noqa: E402


def _data():
    return {
        "admin": {"username": "admin", "email": "a@example.com", "password": "pw-12345678"},
        "site_config": {"timezone": "UTC"},
        "data_options": {"option": "fresh"},
    }


def _app():
    app = Flask(__name__)
    app.secret_key = "t"
    return app


def test_setup_failure_exception_records_real_reason(monkeypatch):
    def boom(**_kw):
        raise RuntimeError("Buffer manager exception: Mmap failed")

    monkeypatch.setattr(ob.user_service, "create_user_sync", boom)
    with _app().test_request_context():
        assert ob.execute_onboarding_setup_only(_data()) is False
        msg = ob._pop_setup_error("Setup failed.")
        assert "Mmap failed" in msg
        assert session.get(ob._SETUP_ERROR_KEY) is None  # consumed


def test_setup_failure_none_user_has_explanation(monkeypatch):
    monkeypatch.setattr(ob.user_service, "create_user_sync", lambda **_kw: None)
    with _app().test_request_context():
        assert ob.execute_onboarding_setup_only(_data()) is False
        assert "admin user" in ob._pop_setup_error("Setup failed.")


def test_pop_setup_error_default_when_none():
    with _app().test_request_context():
        assert ob._pop_setup_error("Setup failed.") == "Setup failed."


def test_issue_251_load_ai_config_imported():
    """Regression for #251: AI panel referenced load_ai_config without importing it."""
    assert callable(auth_settings.load_ai_config)


def test_weak_password_does_not_advance_admin_setup(monkeypatch):
    app = _app()
    app.config['WTF_CSRF_ENABLED'] = False
    monkeypatch.setattr(ob, 'render_template', lambda name, **ctx: ctx['form'].errors)
    monkeypatch.setattr(ob.user_service, 'get_user_by_username_sync', lambda _: None)
    monkeypatch.setattr(ob.user_service, 'get_user_by_email_sync', lambda _: None)
    with app.test_request_context(method='POST', data={
        'username': 'reader', 'email': 'reader@example.com', 'password': 'a', 'password2': 'a',
    }):
        assert 'password' in ob.admin_setup_step()
        assert 'admin' not in session.get('onboarding_data', {})
        assert session.get('onboarding_step', 1) == 1


def test_admin_setup_checks_duplicate_then_accepts_valid_credentials(monkeypatch):
    app = _app()
    app.config['WTF_CSRF_ENABLED'] = False
    app.register_blueprint(ob.onboarding_bp)
    monkeypatch.setattr(ob, 'render_template', lambda name, **ctx: ctx['form'].errors)
    data = dict(username='reader', email='reader@example.com',
                password='Strong-Password-987!', password2='Strong-Password-987!')
    for existing in (True, False):
        user = object() if existing else None
        monkeypatch.setattr(ob.user_service, 'get_user_by_username_sync', lambda _: user)
        monkeypatch.setattr(ob.user_service, 'get_user_by_email_sync', lambda _: user)
        with app.test_request_context(method='POST', data=data):
            response = ob.admin_setup_step()
            if existing:
                assert set(response) == {'username', 'email'}
                assert 'onboarding_data' not in session
            else:
                assert response.status_code == 302
                assert session['onboarding_step'] == 2
