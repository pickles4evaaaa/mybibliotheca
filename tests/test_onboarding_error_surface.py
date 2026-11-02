import sys
from pathlib import Path

from flask import Flask, session

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.infrastructure import persistence_state

# Importing the onboarding blueprint pulls in services that probe the active
# persistence backend at import time; avoid needing a live PostgreSQL server.
persistence_state.get_persistence_state = lambda: {"backend": "kuzu", "migration_run_id": None}

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
