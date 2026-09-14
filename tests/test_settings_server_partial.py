import json
import sys
from pathlib import Path

from flask import Flask
from flask_login import LoginManager

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import app.auth_settings as auth_settings


class _TestAdmin:
    id = "settings-test-admin"
    is_admin = True
    is_active = True
    is_authenticated = True
    is_anonymous = False

    def get_id(self):
        return self.id


def test_server_config_partial_saves_default_rows_per_page(tmp_path, monkeypatch):
    """Saving a valid admin default row count must return the partial successfully."""
    app = Flask(__name__)
    app.secret_key = "test-only"
    app.config["DATA_DIR"] = str(tmp_path)
    login_manager = LoginManager(app)
    login_manager.user_loader(lambda _user_id: _TestAdmin())
    monkeypatch.setattr(auth_settings, "get_admin_settings_context", lambda: {})
    monkeypatch.setattr(
        auth_settings,
        "render_template",
        lambda *_args, **_kwargs: "CONFIG PARTIAL OK",
    )
    app.add_url_rule(
        "/settings/partial/<panel>",
        view_func=auth_settings.settings_server_partial,
        methods=["GET", "POST"],
    )

    client = app.test_client()
    with client.session_transaction() as session:
        session["_user_id"] = _TestAdmin.id
        session["_fresh"] = True

    response = client.post("/settings/partial/config", data={
        "inline": "1",
        "site_name": "Test",
        "server_timezone": "UTC",
        "terminology_preference": "genre",
        "default_rows_per_page": "6",
        "default_book_format": "physical",
        "min_password_length": "12",
    })

    assert response.status_code == 200
    assert response.get_data(as_text=True) == "CONFIG PARTIAL OK"
    saved_config = json.loads((tmp_path / "system_config.json").read_text(encoding="utf-8"))
    assert saved_config["library_defaults"]["default_rows_per_page"] == 6
