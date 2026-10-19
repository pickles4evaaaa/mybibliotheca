from io import BytesIO
import tempfile
from pathlib import Path

from flask import Flask
from flask_login import LoginManager

import app.routes.import_routes as import_routes


class _TestUser:
    id = "alice"
    is_authenticated = True
    is_active = True
    is_anonymous = False

    def get_id(self):
        return self.id


def test_import_file_path_accepts_only_current_users_temp_upload(tmp_path):
    with tempfile.NamedTemporaryFile(prefix="import_alice_", suffix=".csv", delete=False) as upload:
        upload.write(b"Title,Author\nBook,Writer\n")
        upload_path = Path(upload.name)

    try:
        assert import_routes._owned_import_file_path(str(upload_path), "alice") == str(upload_path.resolve())
        assert import_routes._owned_import_file_path(str(upload_path), "bob") is None

        outside_path = tmp_path / "private.txt"
        outside_path.write_text("private", encoding="utf-8")
        assert import_routes._owned_import_file_path(str(outside_path), "alice") is None
    finally:
        upload_path.unlink(missing_ok=True)


def test_import_file_path_rejects_symlink_to_file_outside_temp_dir(tmp_path):
    outside_path = tmp_path / "private.txt"
    outside_path.write_text("private", encoding="utf-8")
    link_path = Path(tempfile.gettempdir()) / "import_alice_link.csv"
    try:
        link_path.symlink_to(outside_path)
        assert import_routes._owned_import_file_path(str(link_path), "alice") is None
    finally:
        link_path.unlink(missing_ok=True)


def test_import_execute_rejects_client_supplied_server_path(monkeypatch):
    app = Flask(__name__)
    app.secret_key = "test-only"
    login_manager = LoginManager(app)
    login_manager.user_loader(lambda _user_id: _TestUser())
    app.register_blueprint(import_routes.import_bp)

    job_creation_calls = []
    monkeypatch.setattr(import_routes, "safe_create_import_job", lambda *args, **kwargs: job_creation_calls.append(args))

    client = app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = _TestUser.id
        session["_fresh"] = True

    response = client.post("/import-books/execute", data={"csv_file_path": "/etc/passwd"})

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/import-books")
    assert job_creation_calls == []


def test_legacy_upload_runs_the_import_processor(monkeypatch):
    app = Flask(__name__)
    app.secret_key = "test-only"
    login_manager = LoginManager(app)
    login_manager.user_loader(lambda _user_id: _TestUser())
    app.register_blueprint(import_routes.import_bp)

    job_creation_calls = []
    processor_calls = []

    async def _process_import(config):
        processor_calls.append(config)

    class _ImmediateThread:
        def __init__(self, target):
            self.target = target
            self.daemon = False

        def start(self):
            self.target()

    monkeypatch.setattr(import_routes, "safe_create_import_job", lambda *args, **kwargs: job_creation_calls.append(args))
    monkeypatch.setattr(import_routes, "process_simple_import", _process_import)
    monkeypatch.setattr(import_routes.threading, "Thread", _ImmediateThread)

    client = app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = _TestUser.id
        session["_fresh"] = True

    response = client.post(
        "/upload",
        data={
            "csv_file": (BytesIO(b"Title,Author\nA Book,An Author\n"), "books.CSV"),
            "field_Title": "title",
            "field_Author": "author",
        },
        content_type="multipart/form-data",
    )

    assert response.status_code == 302
    assert len(job_creation_calls) == 1
    assert len(processor_calls) == 1
    assert processor_calls[0]["user_id"] == _TestUser.id
