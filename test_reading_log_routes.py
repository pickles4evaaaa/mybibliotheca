import importlib.util
import sys
import types
from pathlib import Path

from flask import Flask, jsonify


class ReadingLogServiceStub:
    def __init__(self, logs):
        self.logs = logs
        self.calls = []

    def get_user_reading_logs_sync(self, user_id, days_back=30):
        self.calls.append((user_id, days_back))
        return list(self.logs)


class BookServiceStub:
    def __init__(self):
        self.calls = []

    def get_book_by_id_for_user_sync(self, book_id, user_id):
        self.calls.append((book_id, user_id))
        return {"id": book_id, "title": "Book One"}


def load_reading_log_routes_module(logs):
    module_name = "app.routes.reading_log_routes"
    here = Path(__file__).resolve().parent
    repo_root = here if (here / "app").exists() else here.parent
    module_path = repo_root / "app" / "routes" / "reading_log_routes.py"

    app_mod = types.ModuleType("app")
    routes_mod = types.ModuleType("app.routes")
    forms_mod = types.ModuleType("app.forms")
    services_mod = types.ModuleType("app.services")
    personal_meta_mod = types.ModuleType("app.services.personal_metadata_service")
    domain_mod = types.ModuleType("app.domain")
    domain_models_mod = types.ModuleType("app.domain.models")
    utils_mod = types.ModuleType("app.utils")
    user_settings_mod = types.ModuleType("app.utils.user_settings")
    flask_login_mod = types.ModuleType("flask_login")

    reading_log_service = ReadingLogServiceStub(logs)
    book_service = BookServiceStub()

    forms_mod.ReadingLogEntryForm = object
    services_mod.reading_log_service = reading_log_service
    services_mod.book_service = book_service
    personal_meta_mod.personal_metadata_service = types.SimpleNamespace(
        ensure_start_date=lambda *args, **kwargs: None,
        update_personal_metadata=lambda *args, **kwargs: None,
    )
    domain_models_mod.ReadingLog = object
    domain_models_mod.ReadingStatus = types.SimpleNamespace(READING=types.SimpleNamespace(value="reading"))
    user_settings_mod.get_effective_reading_defaults = lambda _user_id: (0, 0)
    flask_login_mod.login_required = lambda f: f
    flask_login_mod.current_user = types.SimpleNamespace(id="user-1")

    sys.modules["app"] = app_mod
    sys.modules["app.routes"] = routes_mod
    sys.modules["app.forms"] = forms_mod
    sys.modules["app.services"] = services_mod
    sys.modules["app.services.personal_metadata_service"] = personal_meta_mod
    sys.modules["app.domain"] = domain_mod
    sys.modules["app.domain.models"] = domain_models_mod
    sys.modules["app.utils"] = utils_mod
    sys.modules["app.utils.user_settings"] = user_settings_mod
    sys.modules["flask_login"] = flask_login_mod

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module, reading_log_service, book_service


def test_my_reading_logs_filters_by_book_id():
    logs = [
        {"id": "log-1", "book": {"id": "book-1", "title": "Book One"}, "pages_read": 10, "minutes_read": 20},
        {"id": "log-2", "book": {"id": "book-2", "title": "Book Two"}, "pages_read": 15, "minutes_read": 30},
        {"id": "log-3", "book": None, "pages_read": 5, "minutes_read": 10},
    ]
    reading_log_routes, reading_log_service, book_service = load_reading_log_routes_module(logs)

    app = Flask(__name__)
    app.config["TESTING"] = True

    captured = {}

    def fake_render_template(_template, **context):
        captured.update(context)
        ids = []
        for log in context.get("logs", []):
            book = log.get("book") if isinstance(log, dict) else None
            ids.append((book or {}).get("id"))
        return jsonify({"log_ids": ids, "count": len(ids), "filtered_book_id": context.get("filtered_book_id")})

    reading_log_routes.render_template = fake_render_template
    app.register_blueprint(reading_log_routes.reading_logs)

    with app.test_client() as client:
        response = client.get("/reading-logs/my-logs?book_id=book-1")

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["count"] == 1
    assert payload["log_ids"] == ["book-1"]
    assert payload["filtered_book_id"] == "book-1"
    assert reading_log_service.calls == [("user-1", 30)]
    assert book_service.calls == [("book-1", "user-1")]
    assert captured["filtered_book_id"] == "book-1"
    assert len(captured["logs"]) == 1
